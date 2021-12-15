package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/swithek/sessionup"
)

var (
	// ErrInvalidTable is returned when invalid table name is provided.
	ErrInvalidTable = errors.New("invalid table name")

	// ErrInvalidInterval is returned when invalid cleanup interval is provided.
	ErrInvalidInterval = errors.New("invalid cleanup interval")
)

// Option is used to set custom options for SQLiteStore implementation.
type Option func(*SQLiteStore)

// ErrorChannelBuffer is used to set buffer size for errors channel.
// The default value is 0.
func ErrorChannelBuffer(size int) func(*SQLiteStore) {
	return func(st *SQLiteStore) {
		st.cleanup.errCh = make(chan error, size)
	}
}

// OnDeletion is used to set a callback function that will be called whenever
// session is deleted.
// If deletion is automatic, service context will be used that is closed only
// after calling Close(). Otherwise if deletion occured via DeleteByID or
// DeleteByUserKey methods, method context will be passed through.
func OnDeletion(fn func(context.Context, sessionup.Session)) func(*SQLiteStore) {
	return func(st *SQLiteStore) {
		st.deletionFn = fn
	}
}

// SQLiteStore is a SQLite implementation of sessionup.Store.
type SQLiteStore struct {
	db         *sql.DB
	table      string
	deletionFn func(context.Context, sessionup.Session)

	cleanup struct {
		sync.RWMutex
		errCh chan error
	}

	lifetime struct {
		wg     sync.WaitGroup
		cancel context.CancelFunc
	}
}

// New creates and returns a fresh intance of SQLiteStore.
// Table parameter determines table name which is used to store and manage
// sessions, it cannot be an empty string.
// Cleanup interval parameter is an interval time between each clean up. If
// this interval is equal to zero, cleanup won't be executed. Cannot be less than
// zero.
func New(
	db *sql.DB,
	table string,
	cleanupInterval time.Duration,
	opts ...Option,
) (*SQLiteStore, error) {

	if table == "" {
		return nil, ErrInvalidTable
	}

	if cleanupInterval < 0 {
		return nil, ErrInvalidInterval
	}

	if _, err := db.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(255) PRIMARY KEY NOT NULL,
		user_key VARCHAR(255) NOT NULL,
		expires_at DATETIME NOT NULL,
		data BLOB NOT NULL
	)`, table)); err != nil {
		return nil, err
	}

	st := &SQLiteStore{
		db:    db,
		table: table,
	}

	for _, opt := range opts {
		opt(st)
	}

	if st.cleanup.errCh == nil {
		st.cleanup.errCh = make(chan error)
	}

	if cleanupInterval != 0 {
		ctx, cancel := context.WithCancel(context.Background())
		st.lifetime.cancel = cancel

		st.lifetime.wg.Add(1)
		go func() {
			defer st.lifetime.wg.Done()

			t := time.NewTicker(cleanupInterval)
			defer t.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					st.cleanup.Lock()
					if err := st.runCleanup(ctx); err != nil && !errors.Is(err, context.Canceled) {
						select {
						case st.cleanup.errCh <- err:
						default:
							// Slow consumer.
						}
					}
					st.cleanup.Unlock()
				}
			}
		}()
	} else {
		close(st.cleanup.errCh)
	}

	return st, nil
}

// Close stops the cleanup service.
// It always returns nil as an error, used to implement io.Closer interface.
func (st *SQLiteStore) Close() error {
	if st.lifetime.cancel != nil {
		st.lifetime.cancel()
		st.cleanup.Lock()
		close(st.cleanup.errCh)
		st.cleanup.errCh = nil
		st.cleanup.Unlock()
		st.lifetime.wg.Wait()
	}

	return nil
}

// Create inserts provided session into the store and ensures
// that it is deleted when expiration time is due.
func (st *SQLiteStore) Create(ctx context.Context, s sessionup.Session) error {
	data, err := json.Marshal(newRecord(s))
	if err != nil {
		// unlikely to happen
		return err
	}

	_, err = sq.Insert(st.table).
		SetMap(map[string]interface{}{
			"id":         s.ID,
			"user_key":   s.UserKey,
			"expires_at": s.ExpiresAt,
			"data":       data,
		}).
		RunWith(st.db).
		ExecContext(ctx)

	return err
}

// FetchByID retrieves a session from the store by the provided ID.
// The second returned value indicates whether the session was found
// or not (true == found), error will be nil if session is not found.
func (st *SQLiteStore) FetchByID(ctx context.Context, id string) (sessionup.Session, bool, error) {
	// ss will always be of length 1 or 0.
	ss, err := st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"id": id})
	})
	if err != nil {
		return sessionup.Session{}, false, err
	}

	if len(ss) == 0 {
		return sessionup.Session{}, false, nil
	}

	return ss[0], true, nil
}

// FetchByUserKey retrieves all sessions associated with the
// provided user key. If none are found, both return values will be nil.
func (st *SQLiteStore) FetchByUserKey(ctx context.Context, key string) ([]sessionup.Session, error) {
	return st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"user_key": key})
	})
}

// DeleteByID deletes the session from the store by the provided ID.
// If session is not found, this function will be no-op.
func (st *SQLiteStore) DeleteByID(ctx context.Context, id string) error {
	st.cleanup.Lock()
	defer st.cleanup.Unlock()

	// ss will always be of length 1 or 0.
	ss, err := st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"id": id})
	})
	if err != nil {
		// unlikely to happen
		return err
	}

	if len(ss) == 0 {
		return nil
	}

	_, err = sq.Delete(st.table).
		Where("id = ?", id).
		RunWith(st.db).
		ExecContext(ctx)
	if err != nil {
		// unlikely to happen
		return err
	}

	if st.deletionFn != nil {
		st.deletionFn(ctx, ss[0])
	}

	return nil
}

// DeleteByUserKey deletes all sessions associated with the provided user key,
// except those whose IDs are provided as last argument.
// If none are found, this function will no-op.
func (st *SQLiteStore) DeleteByUserKey(ctx context.Context, key string, expIDs ...string) error {
	st.cleanup.Lock()
	defer st.cleanup.Unlock()

	ss, err := st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.And{
			sq.Eq{"user_key": key},
			sq.NotEq{"id": expIDs},
		})
	})
	if err != nil {
		// unlikely to happen
		return err
	}

	ids := make([]string, len(ss))
	for i, ses := range ss {
		ids[i] = ses.ID
	}

	_, err = sq.Delete(st.table).
		Where(sq.Eq{"id": ids}).
		RunWith(st.db).
		ExecContext(ctx)
	if err != nil {
		// unlikely to happen
		return err
	}

	if st.deletionFn != nil {
		for _, ses := range ss {
			st.deletionFn(ctx, ses)
		}
	}

	return nil
}

// CleanupErr returns a channel that should be used to read and handle errors
// that occurred during cleanup process. If channel is not used, it will discard
// the errors to not block the cleanup. If there is doubt that the reader will
// be fast enough to read errors, channel buffer size can be increased.
func (st SQLiteStore) CleanupErr() <-chan error {
	st.cleanup.RLock()
	defer st.cleanup.RUnlock()

	return st.cleanup.errCh
}

// runCleanup removes all expired records from the store by their expiration time.
func (st *SQLiteStore) runCleanup(ctx context.Context) error {
	ss, err := st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.LtOrEq{
			"expires_at": time.Now(),
		})
	})
	if err != nil {
		return err
	}

	if len(ss) == 0 {
		return nil
	}

	ids := make([]string, len(ss))
	for i, ses := range ss {
		ids[i] = ses.ID
	}

	_, err = sq.Delete(st.table).
		Where(sq.Eq{"id": ids}).
		RunWith(st.db).
		ExecContext(ctx)

	if st.deletionFn != nil {
		for _, ses := range ss {
			st.deletionFn(ctx, ses)
		}
	}

	return err
}

func (st *SQLiteStore) selectSessions(
	ctx context.Context,
	br sq.BaseRunner,
	dec func(sq.SelectBuilder) sq.SelectBuilder,
) ([]sessionup.Session, error) {

	rows, err := dec(sq.Select("id", "user_key", "expires_at", "data").
		From(st.table).
		RunWith(br)).
		QueryContext(ctx)

	if err != nil {
		// unlikely to happen
		return nil, err
	}

	defer rows.Close()

	var ss []sessionup.Session
	for rows.Next() {
		var (
			ses  sessionup.Session
			data []byte
		)

		if err := rows.Scan(&ses.ID, &ses.UserKey, &ses.ExpiresAt, &data); err != nil {
			// unlikely to happen
			return nil, err
		}

		var r record
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, err
		}

		ss = append(ss, r.toSession(ses.ID, ses.UserKey, ses.ExpiresAt))
	}

	return ss, nil
}

// record is used to store session data in store as a blob.
type record struct {
	// CreatedAt specifies a point in time when this session
	// was created.
	CreatedAt time.Time `json:"created_at"`

	// IP specifies an IP address that was used to create
	// this session.
	IP net.IP `json:"ip"`

	// Agent specifies the User-Agent data that was used
	// to create this session.
	Agent struct {
		OS      string `json:"os"`
		Browser string `json:"browser"`
	} `json:"agent"`

	// Meta specifies a map of metadata associated with
	// the session.
	Meta map[string]string `json:"meta"`
}

// newRecord creates a fresh instance of new record.
func newRecord(s sessionup.Session) record {
	return record{
		CreatedAt: s.CreatedAt,
		IP:        s.IP,
		Agent:     s.Agent,
		Meta:      s.Meta,
	}
}

// toSession returns sessionup.Session data from the record.
func (r *record) toSession(id, uk string, et time.Time) sessionup.Session {
	return sessionup.Session{
		CreatedAt: r.CreatedAt,
		ExpiresAt: et,
		ID:        id,
		UserKey:   uk,
		IP:        r.IP,
		Agent:     r.Agent,
		Meta:      r.Meta,
	}
}

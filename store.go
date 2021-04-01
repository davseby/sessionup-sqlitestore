package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

// SQLiteStore is a SQLite implementation of sessionup.Store.
type SQLiteStore struct {
	db              *sql.DB
	table           string
	errCh           chan error
	cancel          context.CancelFunc
	cleanupInterval time.Duration
}

// New creates and returns a fresh intance of SQLiteStore.
// Table parameter determines table name which is used to store and manage
// sessions, it cannot be an empty string.
// Cleanup interval parameter is an interval time between each clean up. If
// this interval is equal to zero, cleanup won't be executed. Cannot be less than
// zero.
func New(db *sql.DB, table string, cleanupInterval time.Duration) (*SQLiteStore, error) {
	if table == "" {
		return nil, ErrInvalidTable
	}

	if cleanupInterval < 0 {
		return nil, ErrInvalidInterval
	}

	if _, err := db.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(255) PRIMARY KEY,
		user_key VARCHAR(255) NOT NULL,
		expires_at DATETIME NOT NULL,
		data BLOB NOT NULL
	)`, table)); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	st := &SQLiteStore{
		db:              db,
		table:           table,
		errCh:           make(chan error),
		cancel:          cancel,
		cleanupInterval: cleanupInterval,
	}

	if cleanupInterval != 0 {
		go func() {
			t := time.NewTicker(cleanupInterval)
			defer t.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := st.cleanup(ctx); err != nil && err != context.Canceled {
						st.errCh <- err
					}
				}
			}
		}()
	}

	return st, nil
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
	var (
		uk   string
		et   time.Time
		data []byte
	)

	if err := sq.Select("user_key", "expires_at", "data").
		From(st.table).
		Where("id = ?", id).
		RunWith(st.db).
		ScanContext(ctx, &uk, &et, &data); err != nil {

		return sessionup.Session{}, false, err
	}

	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return sessionup.Session{}, false, err
	}

	return r.toSession(id, uk, et), true, nil
}

// FetchByUserKey retrieves all sessions associated with the
// provided user key. If none are found, both return values will be nil.
func (st *SQLiteStore) FetchByUserKey(ctx context.Context, key string) ([]sessionup.Session, error) {
	rows, err := sq.Select("id", "expires_at", "data").
		From(st.table).
		Where("user_key = ?", key).
		RunWith(st.db).
		QueryContext(ctx)

	if err != nil {
		// unlikely to happen
		return nil, err
	}

	defer rows.Close()

	var ss []sessionup.Session
	for rows.Next() {
		var (
			id   string
			et   time.Time
			data []byte
		)

		if err := rows.Scan(&id, &et, &data); err != nil {
			// unlikely to happen
			return nil, err
		}

		var r record
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, err
		}

		ss = append(ss, r.toSession(id, key, et))
	}

	return ss, nil
}

// DeleteByID deletes the session from the store by the provided ID.
// If session is not found, this function will be no-op.
func (st *SQLiteStore) DeleteByID(ctx context.Context, id string) error {
	_, err := sq.Delete(st.table).
		Where("id = ?", id).
		RunWith(st.db).
		ExecContext(ctx)

	return err
}

// DeleteByUserKey deletes all sessions associated with the provided user key,
// except those whose IDs are provided as last argument.
// If none are found, this function will no-op.
func (st *SQLiteStore) DeleteByUserKey(ctx context.Context, key string, expIDs ...string) error {
	_, err := sq.Delete(st.table).
		Where(sq.And{
			sq.Eq{"user_key": key},
			sq.NotEq{"id": expIDs},
		}).
		RunWith(st.db).
		ExecContext(ctx)

	return err
}

// CleanupErr returns a channel that should be used to read and handle errors
// that occurred during cleanup process. Whenever the cleanup service is active,
// errors from this channel will have to be drained, otherwise cleanup won't be able
// to continue its process.
func (st SQLiteStore) CleanupErr() <-chan error {
	return st.errCh
}

// Close stops the cleanup service.
// It always returns nil as an error, used to implement io.Closer interface.
func (st *SQLiteStore) Close() error {
	st.cancel()
	close(st.errCh)

	return nil
}

// cleanup removes all expired records from the store by their expiration time.
func (st *SQLiteStore) cleanup(ctx context.Context) error {
	_, err := sq.Delete(st.table).
		Where(sq.LtOrEq{
			"expires_at": time.Now(),
		}).
		RunWith(st.db).
		ExecContext(ctx)

	return err
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

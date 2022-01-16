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

// SQLiteStore is a SQLite implementation of sessionup.Store.
type SQLiteStore struct {
	db    *sql.DB
	table string

	deletion struct {
		mu sync.Mutex

		nextID uint64
		fns    map[uint64]func(context.Context, sessionup.Session)
	}
}

// New creates and returns a fresh intance of SQLiteStore.
func New(db *sql.DB, table string) (*SQLiteStore, error) {
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			user_key TEXT NOT NULL,
			expires_at DATETIME NOT NULL,
			created_at DATETIME NOT NULL,
			ip TEXT,
			agent_os TEXT,
			agent_browser TEXT,
			meta BLOB
		)`, table)); err != nil {

		return nil, err
	}

	st := &SQLiteStore{
		db:    db,
		table: table,
	}

	st.deletion.fns = make(map[uint64]func(context.Context, sessionup.Session))

	return st, nil
}

// NewWithCleanup creates and returns a fresh instance of SQLiteStore and
// additionally spins up cleanup go routine. To manage new go routine
// a cleanup errors channel and cleanup close delegate function is returned.
func NewWithCleanup(
	db *sql.DB,
	table string,
	dur time.Duration,
) (*SQLiteStore, <-chan error, func(), error) {

	if dur <= 0 {
		return nil, nil, nil, ErrInvalidInterval
	}

	st, err := New(db, table)
	if err != nil {
		return nil, nil, nil, err
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := st.Cleanup(ctx, dur)
			if errors.Is(err, ctx.Err()) {
				return
			}

			select {
			case errCh <- err:
			default:
				// slow receiver.
			}
		}
	}()

	return st, errCh, func() {
		cancel()
		wg.Wait()

		// drain errors channel.
		close(errCh)
		for range errCh {
		}
	}, nil
}

// Cleanup periodically removes all expired records from the store by their
// expiration time.
// Duration specifies how often cleanup should be ran.
// If duration is less than zero, an ErrInvalidInterval error will be returned.
func (st *SQLiteStore) Cleanup(ctx context.Context, dur time.Duration) error {
	if dur <= 0 {
		return ErrInvalidInterval
	}

	tc := time.NewTimer(dur)
	defer tc.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tc.C:
			if err := st.removeExpiredSessions(ctx); err != nil {
				return err
			}

			tc.Reset(dur)
		}
	}
}

// OnDeletion sets provided handler to be executed whenever a session is deleted.
// Unsubscribe method is returned, that allows to unset the handler.
func (st *SQLiteStore) OnDeletion(fn func(context.Context, sessionup.Session)) func() {
	st.deletion.mu.Lock()
	defer st.deletion.mu.Unlock()

	id := st.deletion.nextID
	st.deletion.nextID++
	st.deletion.fns[id] = fn

	return func() {
		st.deletion.mu.Lock()
		delete(st.deletion.fns, id)
		st.deletion.mu.Unlock()
	}
}

// Create inserts provided session into the store.
func (st *SQLiteStore) Create(ctx context.Context, session sessionup.Session) error {
	var (
		data []byte
		err  error
	)

	if session.Meta != nil {
		data, err = json.Marshal(session.Meta)
		if err != nil {
			// unlikely to happen.
			return err
		}
	}

	_, err = sq.Insert(st.table).
		SetMap(map[string]interface{}{
			"id":            session.ID,
			"user_key":      session.UserKey,
			"expires_at":    session.ExpiresAt,
			"created_at":    session.CreatedAt,
			"ip":            session.IP.String(),
			"agent_os":      session.Agent.OS,
			"agent_browser": session.Agent.Browser,
			"meta":          data,
		}).
		RunWith(st.db).
		ExecContext(ctx)

	return err
}

// FetchByID retrieves a session from the store by the provided ID.
// The second returned value indicates whether the session was found
// or not (true == found), error will be nil if session is not found.
// If session is expired, it will be treated as not found.
func (st *SQLiteStore) FetchByID(ctx context.Context, id string) (sessionup.Session, bool, error) {
	sessions, err := st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"id": id})
	})
	if err != nil {
		return sessionup.Session{}, false, err
	}

	if len(sessions) == 0 || sessions[0].ExpiresAt.Before(time.Now()) {
		return sessionup.Session{}, false, nil
	}

	return sessions[0], true, nil
}

// FetchByUserKey retrieves all sessions associated with the
// provided user key. If none are found, both return values will be nil.
// If list contains expired sessions, they will not be returned.
func (st *SQLiteStore) FetchByUserKey(ctx context.Context, key string) ([]sessionup.Session, error) {
	ss, err := st.selectSessions(ctx, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"user_key": key})
	})
	if err != nil {
		return nil, err
	}

	var (
		valid  []sessionup.Session
		tstamp = time.Now()
	)

	for _, session := range ss {
		if !session.ExpiresAt.Before(tstamp) {
			valid = append(valid, session)
		}
	}

	return valid, nil
}

// DeleteByID deletes the session from the store by the provided ID.
// If session is found OnDeletion handlers will be executed.
// If session is not found, this function will be no-op.
func (st *SQLiteStore) DeleteByID(ctx context.Context, id string) error {
	tx, err := st.beginImmediateTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck // this error is meaningless.

	sessions, err := st.selectSessions(ctx, tx, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"id": id})
	})
	if err != nil {
		// unlikely to happen.
		return err
	}

	if len(sessions) == 0 {
		return nil
	}

	_, err = sq.Delete(st.table).
		Where("id = ?", id).
		RunWith(tx).
		ExecContext(ctx)
	if err != nil {
		// unlikely to happen
		return err
	}

	if err := tx.Commit(); err != nil {
		// unlikely to happen.
		return err
	}

	for _, fn := range st.deletion.fns {
		go fn(ctx, sessions[0])
	}

	return nil
}

// DeleteByUserKey deletes all sessions associated with the provided user key,
// except those whose IDs are provided as last argument.
// For each deleted session all OnDeletion handlers will be executed.
// If none are found, this function will no-op.
func (st *SQLiteStore) DeleteByUserKey(ctx context.Context, key string, expIDs ...string) error {
	tx, err := st.beginImmediateTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck // this error is meaningless.

	sessions, err := st.selectSessions(ctx, tx, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.And{
			sq.Eq{"user_key": key},
			sq.NotEq{"id": expIDs},
		})
	})
	if err != nil {
		// unlikely to happen
		return err
	}

	ids := make([]string, len(sessions))
	for i, session := range sessions {
		ids[i] = session.ID
	}

	_, err = sq.Delete(st.table).
		Where(sq.Eq{"id": ids}).
		RunWith(tx).
		ExecContext(ctx)
	if err != nil {
		// unlikely to happen
		return err
	}

	if err := tx.Commit(); err != nil {
		// unlikely to happen.
		return err
	}

	for _, session := range sessions {
		for _, fn := range st.deletion.fns {
			go fn(ctx, session)
		}
	}

	return nil
}

func (st *SQLiteStore) selectSessions(
	ctx context.Context,
	br sq.BaseRunner,
	dec func(sq.SelectBuilder) sq.SelectBuilder,
) ([]sessionup.Session, error) {

	rows, err := dec(sq.
		Select(
			"id",
			"user_key",
			"expires_at",
			"created_at",
			"ip",
			"agent_os",
			"agent_browser",
			"meta",
		).
		From(st.table).
		RunWith(br),
	).QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	defer rows.Close() // nolint: errcheck // this error is meaningless.

	var sessions []sessionup.Session
	for rows.Next() {
		var (
			session sessionup.Session
			ip      string
			data    []byte
		)

		if err := rows.Scan(
			&session.ID,
			&session.UserKey,
			&session.ExpiresAt,
			&session.CreatedAt,
			&ip,
			&session.Agent.OS,
			&session.Agent.Browser,
			&data,
		); err != nil {
			// unlikely to happen.
			return nil, err
		}

		if len(data) > 0 {
			if err := json.Unmarshal(data, &session.Meta); err != nil {
				// unlikely to happen.
				return nil, err
			}
		}

		session.IP = net.ParseIP(ip)
		sessions = append(sessions, session)
	}

	return sessions, nil
}

func (st *SQLiteStore) removeExpiredSessions(ctx context.Context) error {
	tx, err := st.beginImmediateTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck // this error is meaningless.

	sessions, err := st.selectSessions(ctx, tx, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.LtOrEq{
			"expires_at": time.Now().UTC(),
		})
	})
	if err != nil {
		// unlikely to happen.
		return err
	}

	if len(sessions) == 0 {
		return nil
	}

	ids := make([]string, len(sessions))
	for i, session := range sessions {
		ids[i] = session.ID
	}

	_, err = sq.Delete(st.table).
		Where(sq.Eq{"id": ids}).
		RunWith(tx).
		ExecContext(ctx)
	if err != nil {
		// unlikely to happen.
		return err
	}

	if err := tx.Commit(); err != nil {
		// unlikely to happen.
		return err
	}

	for _, session := range sessions {
		for _, fn := range st.deletion.fns {
			go fn(ctx, session)
		}
	}

	return nil
}

func (st *SQLiteStore) beginImmediateTransaction(ctx context.Context) (*sql.Tx, error) {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec("ROLLBACK; BEGIN IMMEDIATE")
	if err != nil {
		// unlikely to happen.
		return nil, err
	}

	return tx, nil
}

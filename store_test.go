package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/swithek/sessionup"
)

const _table = "sessions"

func prepDB(t *testing.T) *sql.DB {
	path := filepath.Join(t.TempDir(), "sqlite.db")

	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			user_key TEXT NOT NULL,
			expires_at DATETIME NOT NULL,
			created_at DATETIME NOT NULL,
			ip TEXT,
			agent_os TEXT,
			agent_browser TEXT,
			meta BLOB
		)`, _table))

	require.NoError(t, err)
	require.NoError(t, db.Ping())

	return db
}

func Test_New(t *testing.T) {
	// Closed DB returns an error.
	db := prepDB(t)
	require.NoError(t, db.Close())

	st, err := New(db, _table)
	assert.Empty(t, st)
	assert.Error(t, err)

	// Success.
	st, err = New(prepDB(t), _table)
	require.NoError(t, err)
	assert.NotNil(t, st.deletion.fns)
}

func Test_NewWithCleanup(t *testing.T) {
	// Closed DB returns an error.
	db := prepDB(t)
	require.NoError(t, db.Close())

	st, errCh, cancel, err := NewWithCleanup(db, _table, time.Second)
	assert.Empty(t, st)
	assert.Nil(t, errCh)
	assert.Nil(t, cancel)
	assert.Error(t, err)

	// Invalid interval.
	db = prepDB(t)

	st, errCh, cancel, err = NewWithCleanup(db, _table, 0)
	assert.Empty(t, st)
	assert.Nil(t, errCh)
	assert.Nil(t, cancel)
	assert.Error(t, err)

	// Success.
	db = prepDB(t)

	st, errCh, cancel, err = NewWithCleanup(db, _table, time.Second)
	require.NoError(t, err)
	assert.NotNil(t, errCh)
	assert.NotNil(t, cancel)
	assert.NotNil(t, st.deletion.fns)

	cancel()

	// Error channel is returning errors.
	db = prepDB(t)

	st, errCh, cancel, err = NewWithCleanup(db, _table, time.Millisecond*50)
	require.NoError(t, err)
	assert.NotNil(t, errCh)
	assert.NotNil(t, cancel)
	assert.NotNil(t, st.deletion.fns)
	require.NoError(t, db.Close())
	require.Error(t, <-errCh)

	cancel()
}

func Test_SQLiteStore_Cleanup(t *testing.T) {
	// Closed DB returns an error.
	db := prepDB(t)
	require.NoError(t, db.Close())

	st := &SQLiteStore{
		db:    db,
		table: _table,
	}

	assert.Error(t, st.Cleanup(context.Background(), 1))

	st.db = prepDB(t)

	// Empty sessions should not return an error.
	assert.NoError(t, st.removeExpiredSessions(context.Background()))

	// Invalid interval.
	assert.Equal(t, ErrInvalidInterval, st.Cleanup(context.Background(), 0))

	// Success.
	tstamp := time.Now().UTC()
	sessions := []sessionup.Session{
		{
			ID:        "123",
			ExpiresAt: tstamp.Add(time.Hour),
		},
		{
			ID:        "124",
			ExpiresAt: tstamp.Add(-100 * time.Hour),
		},
		{
			ID:        "125",
			ExpiresAt: tstamp.Add(time.Hour),
		},
	}

	for _, session := range sessions {
		mustInsert(t, st.db, session)
	}

	ch := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())

	var (
		calls int
		mu    sync.Mutex
	)

	st.deletion.fns = map[uint64]func(sessionup.Session){
		0: func(session sessionup.Session) {
			assert.Equal(t, sessions[1], session)
			close(ch)
			cancel()

			mu.Lock()
			calls++
			mu.Unlock()
		},
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := st.Cleanup(ctx, 1)
		assert.Equal(t, ctx.Err(), err)
	}()

	<-ch
	wg.Wait()

	mu.Lock()
	assert.Equal(t, 1, calls)
	mu.Unlock()
}

func Test_SQLiteStore_OnDeletion(t *testing.T) {
	st := &SQLiteStore{}
	st.deletion.fns = make(map[uint64]func(sessionup.Session))

	blockCh := make(chan struct{})

	defer close(blockCh)

	// Don't wait for handler completion
	var called bool
	unsub := st.OnDeletion(func(_ context.Context, _ sessionup.Session) {
		<-blockCh
		called = true
	})

	assert.Len(t, st.deletion.fns, 1)
	st.deletion.fns[0](sessionup.Session{})

	unsub(false)
	assert.Len(t, st.deletion.fns, 0)
	assert.False(t, called)

	// Wait for handler completion
	unsub = st.OnDeletion(func(_ context.Context, _ sessionup.Session) {
		called = true
	})

	assert.Len(t, st.deletion.fns, 1)
	st.deletion.fns[1](sessionup.Session{})

	unsub(true)

	assert.Len(t, st.deletion.fns, 0)
	assert.True(t, called)
}

func Test_SQLiteStore_Create(t *testing.T) {
	st := &SQLiteStore{
		db:    prepDB(t),
		table: _table,
	}

	// Duplicate primary key.
	mustInsert(t, st.db, sessionup.Session{
		ID: "123",
	})

	assertEqualError(t,
		sqlite3.ErrConstraint,
		st.Create(context.Background(), sessionup.Session{
			ID: "123",
		}),
	)

	session := sessionup.Session{
		ID: "555",
		Meta: map[string]string{
			"test": "test",
		},
	}

	// Successfully created new session.
	require.NoError(t, st.Create(context.Background(), session))

	sessions := mustSelect(t, st.db, func(b sq.SelectBuilder) sq.SelectBuilder {
		return b.Where(sq.Eq{"id": session.ID})
	})

	require.Len(t, sessions, 1)
	assert.Equal(t, session, sessions[0])
}

func Test_SQLiteStore_FetchByID(t *testing.T) {
	st := &SQLiteStore{
		db:    prepDB(t),
		table: _table,
	}

	// Closed database returns an error.
	st.db.Close() // nolint: errcheck // error is meaningless.

	session, ok, err := st.FetchByID(context.Background(), "123")
	assert.Empty(t, session)
	assert.False(t, ok)
	assert.Error(t, err)

	st.db = prepDB(t)

	// Not found.
	session, ok, err = st.FetchByID(context.Background(), "123")
	assert.Empty(t, session)
	assert.False(t, ok)
	assert.NoError(t, err)

	// Not found as session is expired.
	expired := sessionup.Session{
		ID:        "126",
		ExpiresAt: time.Now().Add(-time.Hour).UTC(),
	}

	mustInsert(t, st.db, expired)

	session, ok, err = st.FetchByID(context.Background(), expired.ID)
	assert.Empty(t, session)
	assert.False(t, ok)
	assert.NoError(t, err)

	// Successfully fetched by ID.
	expected := sessionup.Session{
		ID:        "123",
		ExpiresAt: time.Now().Add(time.Hour).UTC(),
	}

	mustInsert(t, st.db, expected)

	session, ok, err = st.FetchByID(context.Background(), expected.ID)
	assert.Equal(t, expected, session)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_SQLiteStore_FetchByUserKey(t *testing.T) {
	st := &SQLiteStore{
		db:    prepDB(t),
		table: _table,
	}

	// Closed db returns an error.
	st.db.Close() // nolint: errcheck // error is meaningless.

	sessions, err := st.FetchByUserKey(context.Background(), "123")
	assert.Empty(t, sessions)
	assert.Error(t, err)

	st.db = prepDB(t)

	// Not found.
	sessions, err = st.FetchByUserKey(context.Background(), "123")
	assert.Empty(t, sessions)
	assert.NoError(t, err)

	// Successfully fetched by UserKey.
	mocked := []sessionup.Session{
		{
			ID:        "1",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		},
		{
			ID:        "2",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
		{
			ID:        "3",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		},
		{
			ID:        "4",
			UserKey:   "124",
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		},
	}

	for _, session := range mocked {
		mustInsert(t, st.db, session)
	}

	sessions, err = st.FetchByUserKey(context.Background(), "123")
	assert.Equal(t, append(mocked[:1], mocked[2]), sessions)
	assert.NoError(t, err)
}

func Test_SQLiteStore_DeleteByID(t *testing.T) {
	st := &SQLiteStore{
		db:    prepDB(t),
		table: _table,
	}

	// Closed db returns an error.
	st.db.Close() // nolint: errcheck // error is meaningless.

	assert.Error(t, st.DeleteByID(context.Background(), "123"))

	st.db = prepDB(t)

	// Not found.
	assert.NoError(t, st.DeleteByID(context.Background(), "123"))

	// Successfully deleted by ID.
	mocked := []sessionup.Session{
		{
			ID:        "1",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		},
		{
			ID:        "2",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
	}

	for _, session := range mocked {
		mustInsert(t, st.db, session)
	}

	called := make([]bool, 2)
	st.deletion.fns = map[uint64]func(sessionup.Session){
		0: func(session sessionup.Session) {
			assert.Equal(t, mocked[1], session)
			called[0] = true
		},
		1: func(session sessionup.Session) {
			assert.Equal(t, mocked[1], session)
			called[1] = true
		},
	}

	assert.NoError(t, st.DeleteByID(context.Background(), "2"))
	assert.True(t, called[0])
	assert.True(t, called[1])
}

func Test_SQLiteStore_DeleteByUserKey(t *testing.T) {
	st := &SQLiteStore{
		db:    prepDB(t),
		table: _table,
	}

	// Closed db returns an error.
	st.db.Close() // nolint: errcheck // error is meaningless.

	assert.Error(t, st.DeleteByUserKey(context.Background(), "123"))

	st.db = prepDB(t)

	// Not found.
	assert.NoError(t, st.DeleteByUserKey(context.Background(), "123"))

	// Successfully deleted by UserKey.
	mocked := []sessionup.Session{
		{
			ID:        "1",
			UserKey:   "124",
			ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
		{
			ID:        "2",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(time.Hour).UTC(),
		},
		{
			ID:        "3",
			UserKey:   "124",
			ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
		{
			ID:        "4",
			UserKey:   "123",
			ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
		{
			ID:        "5",
			UserKey:   "124",
			ExpiresAt: time.Now().Add(-time.Hour).UTC(),
		},
	}

	for _, session := range mocked {
		mustInsert(t, st.db, session)
	}

	visited := make(map[string]struct{})

	st.deletion.fns = map[uint64]func(sessionup.Session){
		0: func(session sessionup.Session) {

			visited[session.ID] = struct{}{}
			for _, expected := range mocked {
				if expected.ID == session.ID {
					assert.Equal(t, expected, session)
				}
			}
		},
	}

	assert.NoError(t, st.DeleteByUserKey(context.Background(), "124", "5"))

	_, ok := visited["1"]
	assert.True(t, ok)

	_, ok = visited["3"]
	assert.True(t, ok)
}

func assertEqualError(t *testing.T, en sqlite3.ErrNo, err error) {
	nerr, ok := err.(sqlite3.Error)
	require.True(t, ok)
	assert.Equal(t, en, nerr.Code)
}

func mustInsert(t *testing.T, db *sql.DB, session sessionup.Session) {
	t.Helper()

	data, err := json.Marshal(session.Meta)
	require.NoError(t, err)

	_, err = sq.Insert(_table).
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
		RunWith(db).
		Exec()

	require.NoError(t, err)
}

func mustSelect(
	t *testing.T,
	db *sql.DB,
	dec func(b sq.SelectBuilder) sq.SelectBuilder,
) []sessionup.Session {

	t.Helper()

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
		From(_table).
		RunWith(db),
	).Query()

	require.NoError(t, err)
	defer rows.Close() // nolint: errcheck // this error is meaningless.

	var sessions []sessionup.Session
	for rows.Next() {
		var (
			session sessionup.Session
			ip      string
			data    []byte
		)

		require.NoError(t, rows.Scan(
			&session.ID,
			&session.UserKey,
			&session.ExpiresAt,
			&session.CreatedAt,
			&ip,
			&session.Agent.OS,
			&session.Agent.Browser,
			&data,
		))

		if len(data) > 0 {
			require.NoError(t, json.Unmarshal(data, &session.Meta))
		}

		session.IP = net.ParseIP(ip)
		sessions = append(sessions, session)
	}

	return sessions
}

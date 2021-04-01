package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/swithek/sessionup"
)

func Test_New(t *testing.T) {
	// invalid table
	st, err := New(&sql.DB{}, "", time.Second)
	require.Equal(t, ErrInvalidTable, err)
	assert.Nil(t, st)

	// invalid cleanup interval
	st, err = New(&sql.DB{}, "ab", time.Second*-1)
	require.Equal(t, ErrInvalidInterval, err)
	assert.Nil(t, st)

	path := filepath.Join(t.TempDir(), "sqlite.db")

	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)

	// invalid table name
	st, err = New(db, "a b", time.Second)
	equalError(t, sqlite3.ErrError, err)
	assert.Nil(t, st)

	// success
	tb := "test"
	st, err = New(db, tb, time.Millisecond*5)
	require.NoError(t, err)
	assert.NotNil(t, st.db)
	assert.NotNil(t, st.cancel)
	assert.NotNil(t, st.errCh)
	assert.Equal(t, st.table, tb)
	assert.Equal(t, st.cleanupInterval, time.Millisecond*5)

	mustInsert(t, db, tb, sessionup.Session{
		ID: "123",
	})

	// auto deletion works
	assert.Eventually(t, func() bool {
		rows, err := sq.Select("*").
			From(tb).
			RunWith(db).Query()
		require.NoError(t, err)
		defer rows.Close()

		return !rows.Next()
	}, time.Second, time.Millisecond*5)

	// stops auto deletion process
	require.NoError(t, st.Close())
	time.Sleep(time.Millisecond * 30)

	mustInsert(t, db, tb, sessionup.Session{
		ID: "123",
	})
	time.Sleep(time.Millisecond * 30)

	rows, err := sq.Select("*").
		From(tb).
		Where("id = ?", 123).
		RunWith(db).Query()
	require.NoError(t, err)
	assert.True(t, rows.Next())
	rows.Close()

	db.Close()

	file, err = os.Create(path)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)

	st, err = New(db, tb, time.Millisecond*10)
	require.NoError(t, err)
	db.Close()

	// error returned in cleanup process
	assert.Eventually(t, func() bool {
		require.Error(t, <-st.CleanupErr())
		return true
	}, time.Second, time.Millisecond*5)
}

func Test_Store(t *testing.T) {
	suite.Run(t, &Suite{})
}

type Suite struct {
	suite.Suite

	path  string
	table string

	st *SQLiteStore
	db *sql.DB
}

func (s *Suite) SetupSuite() {
	s.path = filepath.Join(s.T().TempDir(), "sqlite.db")
	s.table = "test"

	file, err := os.Create(s.path)
	s.Require().NoError(err)
	s.Require().NoError(file.Close())

	db, err := sql.Open("sqlite3", s.path)
	s.Require().NoError(err)

	s.db = db
	s.st, err = New(db, s.table, 0)
	s.Require().NoError(err)
}

func (s *Suite) TearDownSuite() {
	s.Require().NoError(s.db.Close())
}

func (s *Suite) TearDownTest() {
	_, err := sq.Delete(s.table).
		RunWith(s.db).
		Exec()

	s.Require().NoError(err)
}

func (s *Suite) Test_SQLiteStore_Create() {
	// duplicate error
	mustInsert(s.T(), s.db, s.table, sessionup.Session{
		ID: "123",
	})

	equalError(s.T(), sqlite3.ErrConstraint, s.st.Create(context.Background(), sessionup.Session{
		ID: "123",
	}))

	// success
	s1 := sessionup.Session{
		ID: "555",
	}

	s.Require().NoError(s.st.Create(context.Background(), s1))

	rows, err := sq.Select("*").
		From(s.table).
		Where("id = ?", s1.ID).
		RunWith(s.db).Query()

	s.Require().NoError(err)
	s.Assert().True(rows.Next())
	rows.Close()
}

func (s *Suite) Test_SQLiteStore_FetchByID() {
	// not found
	s1, ok, err := s.st.FetchByID(context.Background(), "1")
	s.Assert().Empty(s1)
	s.Assert().False(ok)
	s.Assert().Equal("sql: no rows in result set", err.Error())

	// malformed data
	_, err = sq.Insert(s.table).
		SetMap(map[string]interface{}{
			"id":         "2",
			"user_key":   "abc",
			"expires_at": time.Now().Add(time.Minute),
			"data":       "{",
		}).
		RunWith(s.db).
		Exec()
	s.Require().NoError(err)

	s2, ok, err := s.st.FetchByID(context.Background(), "2")
	s.Assert().Empty(s2)
	s.Assert().False(ok)
	s.Assert().Equal("unexpected end of JSON input", err.Error())

	// success
	res := sessionup.Session{
		ID: "3",
	}

	mustInsert(s.T(), s.db, s.table, res)

	s3, ok, err := s.st.FetchByID(context.Background(), "3")
	s.Assert().Equal(res, s3)
	s.Assert().True(ok)
	s.Assert().NoError(err)
}

func (s *Suite) Test_SQLiteStore_FetchByUserKey() {
	// not found
	ss, err := s.st.FetchByUserKey(context.Background(), "1")
	s.Assert().Empty(ss)
	s.Assert().NoError(err)

	// malformed data
	_, err = sq.Insert(s.table).
		SetMap(map[string]interface{}{
			"id":         "abc",
			"user_key":   "2",
			"expires_at": time.Now().Add(time.Minute),
			"data":       "{",
		}).
		RunWith(s.db).
		Exec()
	s.Require().NoError(err)

	ss, err = s.st.FetchByUserKey(context.Background(), "2")
	s.Assert().Empty(ss)
	s.Assert().Equal("unexpected end of JSON input", err.Error())

	// success
	res := []sessionup.Session{
		{
			UserKey: "3",
		},
	}

	mustInsert(s.T(), s.db, s.table, res[0])

	ss, err = s.st.FetchByUserKey(context.Background(), "3")
	s.Assert().Equal(res, ss)
	s.Assert().NoError(err)
}

func (s *Suite) Test_SQLiteStore_DeleteByID() {
	// no records
	s.Assert().NoError(s.st.DeleteByID(context.Background(), "1"))

	// success
	mustInsert(s.T(), s.db, s.table, sessionup.Session{
		ID: "123",
	})

	rows, err := sq.Select("*").
		From(s.table).
		Where("id = ?", "123").
		RunWith(s.db).Query()
	require.NoError(s.T(), err)
	assert.True(s.T(), rows.Next())
	rows.Close()

	s.Assert().NoError(s.st.DeleteByID(context.Background(), "123"))

	rows, err = sq.Select("*").
		From(s.table).
		Where("id = ?", "123").
		RunWith(s.db).Query()
	require.NoError(s.T(), err)
	assert.False(s.T(), rows.Next())
	rows.Close()
}

func (s *Suite) Test_SQLiteStore_DeleteByUserKey() {
	// no records
	s.Assert().NoError(s.st.DeleteByUserKey(context.Background(), "1"))

	// success
	mustInsert(s.T(), s.db, s.table, sessionup.Session{
		UserKey: "123",
	})

	rows, err := sq.Select("*").
		From(s.table).
		Where("user_key = ?", "123").
		RunWith(s.db).Query()
	require.NoError(s.T(), err)
	assert.True(s.T(), rows.Next())
	rows.Close()

	s.Assert().NoError(s.st.DeleteByUserKey(context.Background(), "123"))

	rows, err = sq.Select("*").
		From(s.table).
		Where("user_key = ?", "123").
		RunWith(s.db).Query()
	require.NoError(s.T(), err)
	assert.False(s.T(), rows.Next())
	rows.Close()
}

func equalError(t *testing.T, en sqlite3.ErrNo, err error) {
	nerr, ok := err.(sqlite3.Error)
	require.True(t, ok)
	assert.Equal(t, en, nerr.Code)
}

func mustInsert(t *testing.T, db *sql.DB, table string, s sessionup.Session) {
	t.Helper()

	data, err := json.Marshal(newRecord(s))
	require.NoError(t, err)

	_, err = sq.Insert(table).
		SetMap(map[string]interface{}{
			"id":         s.ID,
			"user_key":   s.UserKey,
			"expires_at": s.ExpiresAt,
			"data":       data,
		}).
		RunWith(db).
		Exec()
	require.NoError(t, err)
}
# sessionup-sqlitestore
[![GoDoc](https://godoc.org/github.com/davseby/sessionup-sqlitestore?status.png)](https://godoc.org/github.com/davseby/sessionup-sqlitestore)
[![Test coverage](http://gocover.io/_badge/github.com/davseby/sessionup-sqlitestore)](https://gocover.io/github.com/davseby/sessionup-sqlitestore)
[![Go Report Card](https://goreportcard.com/badge/github.com/davseby/sessionup-sqlitestore)](https://goreportcard.com/report/github.com/davseby/sessionup-sqlitestore)

This is an [SQLite](https://github.com/mattn/go-sqlite3) session store implementation for [sessionup](https://github.com/swithek/sessionup)

## Installation

To install simply use:

```
go get github.com/davseby/sessionup-sqlitestore
```

## Usage

To create and use new SQLiteStore use `New` method. A working sqlite3 driver is 
required along with the table name that should be used to store sessions.

```go
db, err := sql.Open("sqlite3", path)
if err != nil {
      // handle error
}

store, err := sqlitestore.New(db, "sessions")
if err != nil {
      // handle error
}

manager := sessionup.NewManager(store)
```

Expired sessions should be periodically deleted as manager will no longer use
them. It can be done by either using DeleteByID or DeleteByUserKey methods or
by using Cleanup. Cleanup is a blocking method that periodically deletes all
expired sessions. It accepts context and interval parameters. Context is used to
close the procedure and interval describes how often the action should be 
performed.

```go

var wg sync.WaitGroup
ctx, cancel := context.WithCancel(context.Background())

wg.Add(1)
go func() {
	// we can use for loop, in case cleanup returns an error, we can
	// restart the process after handling the error.
	for {
		defer wg.Done()

		err := store.Cleanup(ctx, 15 * time.Minute)
		if errors.Is(err, ctx.Err()) {
			return
		}

		// handle error
	}
}()

// to gracefully close cleanup
cancel()
wg.Wait()
```

Cleanup can also be started when creating SQLiteStore using NewWithCleanup.
Additionally it returns error channel and close/cancel delegate function.

```go

db, err := sql.Open("sqlite3", path)
if err != nil {
      // handle error
}

store, errCh, cancel, err := sqlitestore.NewWithCleanup(db, "sessions", 15 * time.Minute)
if err != nil {
      // handle error
}

var wg sync.WaitGroup
wg.Add(1)
go func() {
	defer wg.Done()
	for err := range errCh {
		// log cleanup errors
	}
}()

manager := sessionup.NewManager(store)

// graceful application close
cancel()
wg.Wait() 

```

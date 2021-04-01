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

To create and use new SQLiteStore use `New` method. It accepts three parameters, the
first one being an open sqlite3 database. Then a table name is required in which sessions 
will be stored and managed. The table parameter cannot be an empty string and it has to be
a valid table name. Lastly it is required that you specify the duration between cleanup intervals, 
if the provided value is zero the cleanup process won't be started. It cannot be less than zero.

```go
db, err := sql.Open("sqlite3", path)
if err != nil {
      // handle error
}

store, err := sqlitestore.New(db, "sessions", time.Minute)
if err != nil {
      // handle error
}

manager := sessionup.NewManager(store)
```

Don't forget to handle cleanup errors by using CleanupErr method. Channel should 
be used only for receiving errors. Whenever the cleanup service is active, errors 
from this channel will have to be drained, otherwise cleanup won't be able to 
continue its process.

```go
for {
      select {
            case err := <-store.CleanupErr():
                  // handle err
      }
}
```

If you want to close auto cleanup process simply use `Close` method. It won't close the
database so you will still be able to use all of the methods described by 
the sessionup store interface.
It will always return nil as an error, implements io.Closer interface.

```go
store.Close()
```

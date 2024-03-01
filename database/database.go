package database

import (
	"database/sql"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dbInstance *sql.DB
	once       sync.Once
)

// getDBInstance returns a singleton instance of the database connection.
func GetDBInstance() *sql.DB {
	once.Do(func() {
		// Open a new database connection.
		dsn := "root:brahim@tcp(localhost:3306)/test"
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(err)
		}
		dbInstance = db
	})
	return dbInstance
}

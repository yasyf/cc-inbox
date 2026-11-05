package store

import "database/sql"

func useHandle(st *Store, db *sql.DB) {
	st.db, st.rw = db, db
}

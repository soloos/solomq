module soloos/swal

go 1.12

replace (
	soloos/common v0.0.0 => /soloos/common
	soloos/sdbone v0.0.0 => /soloos/sdbone
	soloos/silicon v0.0.0 => /soloos/silicon
)

require (
	github.com/go-sql-driver/mysql v1.4.1
	github.com/google/flatbuffers v1.11.0
	github.com/mattn/go-sqlite3 v1.10.0
	soloos/common v0.0.0
	soloos/sdbone v0.0.0
	soloos/silicon v0.0.0 // indirect
)

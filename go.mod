module soloos/solomq

go 1.12

require (
	github.com/go-sql-driver/mysql v1.4.1
	github.com/google/flatbuffers v1.11.0
	github.com/mattn/go-sqlite3 v1.11.0
	soloos/common v0.0.0
	soloos/solodb v0.0.0
	soloos/solofs v0.0.0 // indirect
)

replace (
	soloos/common v0.0.0 => /soloos/common
	soloos/soloboat v0.0.0 => /soloos/soloboat
	soloos/solodb v0.0.0 => /soloos/solodb
	soloos/solofs v0.0.0 => /soloos/solofs
	soloos/solomq v0.0.0 => /soloos/solomq
)

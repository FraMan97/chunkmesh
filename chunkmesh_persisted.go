package chunkmesh

type chunkmeshPersisted struct {
	ChunckSize int
	Files      map[string]file
	Chunks     map[string]chunk
	Versions   map[string]version
}

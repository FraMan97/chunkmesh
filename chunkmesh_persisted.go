package chunkmesh

type chunkmeshPersisted struct {
	ChunkSize int
	Files     map[string]file
	Chunks    map[string]chunk
	Versions  map[string]version
}

package chunkmesh

type file struct {
	Name        string   `json:"name"`
	LastVersion string   `json:"last_version"`
	Versions    []string `json:"versions"`
}

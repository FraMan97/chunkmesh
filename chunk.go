package chunkmesh

type chunk struct {
	Id                 string   `json:"id"`
	Padding            int      `json:"padding"`
	AssociatedVersions []string `json:"associated_versions"`
	RefCount           int      `json:"ref_count"`
}

package chunkmesh

type chunk struct {
	Id          string `json:"id"`
	Compression bool   `json:"compression"`
	RefCount    int    `json:"ref_count"`
}

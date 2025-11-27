package models

type Version struct {
	Id          string   `json:"id"`
	FileName    string   `json:"file_name"`
	Size        int      `json:"size"`
	Chunks      []string `json:"chunks"`
	PrevVersion string   `json:"prev_version"`
}

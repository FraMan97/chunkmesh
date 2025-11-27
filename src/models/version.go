package models

import "time"

type Version struct {
	Id          string    `json:"id"`
	Retention   int       `json:"retention"`
	CreatedAt   time.Time `json:"created_at"`
	FileName    string    `json:"file_name"`
	Size        int       `json:"size"`
	Chunks      []string  `json:"chunks"`
	PrevVersion string    `json:"prev_version"`
}

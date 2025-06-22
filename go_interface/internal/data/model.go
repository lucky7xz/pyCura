package data

// ConfigFile represents metadata about a configuration file
type ConfigFile struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	FileType string `json:"file_type"` // "json" or "toml"
	Size     int64  `json:"size_bytes"`
}

// Selected tracks which config file is currently selected in the UI
type Selected struct {
	Index int
}

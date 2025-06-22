package python

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"go_interface/internal/data"
)

// Executor handles Python script execution and file listing
type Executor struct {
	configDir string
}

// NewExecutor creates a new Executor
func NewExecutor(configDir string) *Executor {
	return &Executor{
		configDir: configDir,
	}
}

// ListConfigFiles returns all JSON and TOML files in the config directory
func (e *Executor) ListConfigFiles() ([]data.ConfigFile, error) {
	var configFiles []data.ConfigFile

	err := filepath.Walk(e.configDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get file extension
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".json" || ext == ".toml" {
			fileType := strings.TrimPrefix(ext, ".")
			
			configFiles = append(configFiles, data.ConfigFile{
				Path:     path,
				Name:     info.Name(),
				FileType: fileType,
				Size:     info.Size(),
			})
		}

		return nil
	})

	return configFiles, err
}

// ExecutePythonScript will eventually execute src.cura.py with the selected config file
// For now it's just a placeholder
func (e *Executor) ExecutePythonScript(configFile data.ConfigFile) (string, error) {
	// This is a placeholder for future implementation
	return "Would execute src.cura.py with config file: " + configFile.Path, nil
}

package python

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
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

// ExecutePythonScript executes src.cura.py with the selected config file
func (e *Executor) ExecutePythonScript(configFile data.ConfigFile, stdin io.Reader) (string, error) {
	// Find the script location more robustly
	// First try looking for the script in the directly where the TUI is running
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	
	// Try multiple locations for the script
	possibleLocations := []string{
		// From CWD
		filepath.Join(cwd, "../src/cura.py"),
		filepath.Join(cwd, "../../src/cura.py"),
		// From config file location
		filepath.Join(filepath.Dir(configFile.Path), "../src/cura.py"),
		// Default looking up for pyCura root
	}
	
	// Navigate up from config file until we find the pyCura directory
	srcDir := filepath.Dir(configFile.Path)
	for i := 0; i < 5; i++ { // Limit depth to avoid infinite loop
		if filepath.Base(srcDir) == "pyCura" || srcDir == "/" {
			break
		}
		srcDir = filepath.Dir(srcDir)
	}
	
	// Add the found path
	possibleLocations = append(possibleLocations, filepath.Join(srcDir, "src", "cura.py"))
	
	// Find the first location that exists
	srcPath := ""
	for _, path := range possibleLocations {
		if _, err := os.Stat(path); err == nil {
			srcPath = path
			break
		}
	}
	
	// If no script found, return an error
	if srcPath == "" {
		return "Error: Could not find src/cura.py script in any expected location", fmt.Errorf("could not find script")
	}
	
	// Read the config file content first (to use as potential input)
	configContent, err := os.ReadFile(configFile.Path)
	if err != nil {
		return fmt.Sprintf("Error reading config file: %v", err), err
	}
	
	// Create command to execute Python script
	cmd := exec.Command("python3", srcPath, "-c", configFile.Path)
	
	// Connect stdin - use provided stdin if available, otherwise assume no stdin needed
	if stdin != nil {
		cmd.Stdin = stdin
	}
	
	// Capture both stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	
	// Set working directory to the directory containing the script
	cmd.Dir = filepath.Dir(srcPath)
	
	// Execute the command
	err = cmd.Run()
	
	// Format the result
	result := fmt.Sprintf("Executing: python3 %s -c %s\n\n", srcPath, configFile.Path)
	result += fmt.Sprintf("Config file content:\n%s\n\n", string(configContent))
	
	if stdout.Len() > 0 {
		result += "Output:\n" + stdout.String() + "\n"
	}
	
	if stderr.Len() > 0 {
		result += "Errors:\n" + stderr.String() + "\n"
	}
	
	if err != nil {
		result += fmt.Sprintf("Execution failed: %v\n", err)
		return result, err
	}
	
	return result, nil
}

// ReadFile reads the contents of a file
func (e *Executor) ReadFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

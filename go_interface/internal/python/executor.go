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
	
	// Instead of trying to run a command directly, let's just display the help to show available commands
	cmd := exec.Command("python3", srcPath, "-h")
	
	// Provide default input for interactive prompts
	// Looking at the script, it will prompt for target_data_structures ("What target(s) to inspect? (cb/dd/both)")
	// We'll provide "both" as the default answer
	defaultInput := "both\n"
	if stdin != nil {
		// If custom input is provided, use it instead
		cmd.Stdin = stdin
	} else {
		// Otherwise use our default input
		cmd.Stdin = strings.NewReader(defaultInput)
	}
	
	// Capture both stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	
	// Set working directory to the directory containing the script
	cmd.Dir = filepath.Dir(srcPath)
	
	// Execute the command
	err = cmd.Run()
	
	// Format the result with helpful information
	result := fmt.Sprintf("Config File: %s\n\n", configFile.Path)
	result += fmt.Sprintf("Python Script: %s\n\n", srcPath)
	result += fmt.Sprintf("Config Content:\n%s\n\n", string(configContent))
	
	// Add the output from help command
	if stdout.Len() > 0 {
		result += "Available Commands:\n" + stdout.String() + "\n"
	}
	
	// Also add any error output
	if stderr.Len() > 0 {
		result += "Script Messages:\n" + stderr.String() + "\n"
	}
	
	result += "\nTo execute specific commands with this config file, use the command line.\n"
	result += fmt.Sprintf("Example: python3 %s %s run\n", srcPath, configFile.Path)
	result += "Note: This script requires interactive input which is best handled in a terminal.\n"
	
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

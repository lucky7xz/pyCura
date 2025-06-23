package python

import (
	"bufio"
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

	// We don't need to read the config file content anymore since we're not displaying it
	// Just check if the file exists and is readable
	_, err = os.Stat(configFile.Path)
	if err != nil {
		return fmt.Sprintf("Error accessing config file: %v", err), err
	}

	// For Python to find the 'src' module, we need to run from the pyCura root directory
	// We no longer need to identify the pyCura root as our wrapper script handles that

	// First, look for a virtual environment in standard locations
	pyCuraRoot := "/home/lucky/dotfiles/scripts/pyCura"
	possibleEnvPaths := []string{
		filepath.Join(pyCuraRoot, "venv", "bin", "activate"),
		filepath.Join(pyCuraRoot, "env", "bin", "activate"),
		filepath.Join(pyCuraRoot, ".venv", "bin", "activate"),
	}

	// Extract just the config name from the path
	configName := filepath.Base(configFile.Path)
	configName = strings.TrimSuffix(configName, filepath.Ext(configName))
	
	// Create a shell command that activates the virtualenv before running the Python script
	shellCmd := fmt.Sprintf(
		"cd %s && python3 -c \"import sys, os; sys.path.insert(0, '%s'); os.environ['PYTHONPATH'] = '%s' + os.pathsep + os.environ.get('PYTHONPATH', ''); import runpy; runpy.run_path('%s', run_name='__main__')\" %s run",
		pyCuraRoot,  // cd to the pyCura root
		pyCuraRoot,  // Add root to sys.path
		pyCuraRoot,  // Set PYTHONPATH
		srcPath,     // Run the actual script
		configName,  // Pass just the config name
	)

	// Find the first existing virtualenv
	venvPath := ""
	for _, path := range possibleEnvPaths {
		if _, err := os.Stat(path); err == nil {
			venvPath = path
			break
		}
	}

	// If we found a virtualenv, activate it
	if venvPath != "" {
		shellCmd = fmt.Sprintf("source %s && %s", venvPath, shellCmd)
	}

	// Execute through bash to handle environment activation
	cmd := exec.Command("bash", "-c", shellCmd)

	// The config file path and help flag are already included in the shell command

	// Provide extensive default input for interactive prompts
	// The script has multiple interactive prompts including confirmations
	// Create a large number of default responses to prevent EOF errors
	defaultInput := "both\n" + // Answer to "What target(s) to inspect? (cb/dd/both)"
		"y\n" +     // Confirmation for domain pre-processing
		"y\n" +     // Confirmation for any other prompts
		"y\n" +     // Additional prompts
		"y\n" +     // More confirmations
		"y\n" +     // Even more confirmations
		"y\n" +     // Additional safety confirmations
		"y\n" +     // Additional safety confirmations
		"y\n" +     // Additional safety confirmations
		"y\n" +     // Additional safety confirmations
		"y\n"       // Final backup confirmation
	if stdin != nil {
		// If custom input is provided, use it instead
		cmd.Stdin = stdin
	} else {
		// Otherwise use our default input
		cmd.Stdin = strings.NewReader(defaultInput)
	}

	// Create pipes for real-time output capture
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "Error creating stdout pipe", err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "Error creating stderr pipe", err
	}
	
	// No status tracking needed for now
	
	// Start the command (doesn't wait for it to complete)
	err = cmd.Start()
	if err != nil {
		return "Error starting command", err
	}
	
	// Collect output in real-time
	var stdout, stderr bytes.Buffer
	
	// Create a channel to signal when reading is done
	done := make(chan bool)
	
	// Read stdout in a goroutine and filter out unwanted lines
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			
			// No status tracking here
			
			// Skip all project_manager info lines and empty lines after filtering
			if !strings.Contains(line, "project_manager") && 
			   !strings.Contains(line, "INFO:src") &&
			   strings.TrimSpace(line) != "" {
				stdout.WriteString(line + "\n")
			}
		}
		done <- true
	}()
	
	// Read stderr in a goroutine
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text() + "\n"
			stderr.WriteString(line)
		}
		done <- true
	}()
	
	// No status monitoring needed
	
	// Wait for both stdout and stderr to be read
	<-done
	<-done
	
	// Status channel removed
	
	// Wait for the command to finish
	err = cmd.Wait()

	// Format the result with helpful information
	result := fmt.Sprintf("Config File: %s\n\n", configFile.Path)
	result += fmt.Sprintf("Python Script: %s\n\n", srcPath)
	result += fmt.Sprintf("Executed Command:\n%s\n\n", shellCmd)
	
	// Status bar will be implemented in bubbletea component instead
	
	// Show real-time output first
	result += "===== EXECUTION OUTPUT =====\n"
	if stdout.Len() > 0 {
		result += stdout.String() + "\n"
	}
	if stderr.Len() > 0 {
		result += "STDERR:\n" + stderr.String() + "\n"
	}
	result += "===== END OUTPUT =====\n\n"
	
	// Don't include config content in the output as requested

	// We've already included the output in the EXECUTION OUTPUT section above

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

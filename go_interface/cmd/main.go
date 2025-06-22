package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"go_interface/internal/python"
	"go_interface/internal/tui"
)

func main() {
	// Get the project root directory
	projRoot := "/home/lucky/dotfiles/scripts/pyCura"
	configDir := filepath.Join(projRoot, "config_files")

	// Check if config directory exists
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		log.Fatalf("Config directory not found: %s", configDir)
	}

	// Initialize the Python executor
	executor := python.NewExecutor(configDir)

	// Get the list of config files
	configFiles, err := executor.ListConfigFiles()
	if err != nil {
		log.Fatalf("Failed to list config files: %v", err)
	}

	// Check if any config files were found
	if len(configFiles) == 0 {
		fmt.Println("No JSON or TOML config files found in:", configDir)
		os.Exit(0)
	}

	// Start the TUI
	if err := tui.Run(executor, configFiles); err != nil {
		log.Fatalf("TUI error: %v", err)
	}
}

package highlighter

import (
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Colors for syntax highlighting
var (
	// JSON colors
	keyColor       = lipgloss.NewStyle().Foreground(lipgloss.Color("#89CFF0")) // Baby blue
	stringColor    = lipgloss.NewStyle().Foreground(lipgloss.Color("#98FB98")) // Pale green
	numberColor    = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFA07A")) // Light salmon
	booleanColor   = lipgloss.NewStyle().Foreground(lipgloss.Color("#D8BFD8")) // Thistle
	nullColor      = lipgloss.NewStyle().Foreground(lipgloss.Color("#D3D3D3")) // Light gray
	bracketColor   = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFD700")) // Gold
	
	// TOML colors (similar but slightly different hues)
	tomlKeyColor     = lipgloss.NewStyle().Foreground(lipgloss.Color("#87CEFA")) // Light sky blue
	tomlStringColor  = lipgloss.NewStyle().Foreground(lipgloss.Color("#90EE90")) // Light green
	tomlNumberColor  = lipgloss.NewStyle().Foreground(lipgloss.Color("#F08080")) // Light coral
	tomlBooleanColor = lipgloss.NewStyle().Foreground(lipgloss.Color("#DDA0DD")) // Plum
	tomlBracketColor = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFF00")) // Yellow
	tomlHeaderColor  = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF69B4")).Bold(true) // Hot pink
)

// Simple regex patterns for highlighting
var (
	jsonKeyPattern     = regexp.MustCompile(`"([^"]+)"(\s*):`)
	jsonStringPattern  = regexp.MustCompile(`(\s*)"([^"]*)"`)
	jsonNumberPattern  = regexp.MustCompile(`(\s*)(-?\d+(\.\d+)?)`)
	jsonBooleanPattern = regexp.MustCompile(`(\s*)(true|false)`)
	jsonNullPattern    = regexp.MustCompile(`(\s*)(null)`)
	jsonBracketPattern = regexp.MustCompile(`[\[\]{}]`)
	
	tomlKeyPattern     = regexp.MustCompile(`^([^=\[]+)(\s*)=`)
	tomlStringPattern  = regexp.MustCompile(`=(\s*)"([^"]*)"`)
	tomlNumberPattern  = regexp.MustCompile(`=(\s*)(-?\d+(\.\d+)?)`)
	tomlBooleanPattern = regexp.MustCompile(`=(\s*)(true|false)`)
	tomlHeaderPattern  = regexp.MustCompile(`^\[([^\]]+)\]`)
)

// HighlightJSON applies simple syntax highlighting to JSON content
func HighlightJSON(content string) string {
	// Apply highlighting with regexes - simple but effective approach
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		// Replace key patterns
		line = jsonKeyPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := jsonKeyPattern.FindStringSubmatch(match)
			if len(submatch) > 2 {
				return keyColor.Render("\"" + submatch[1] + "\"") + submatch[2] + ":"
			}
			return match
		})
		
		// Replace string patterns
		line = jsonStringPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := jsonStringPattern.FindStringSubmatch(match)
			if len(submatch) > 2 {
				return submatch[1] + stringColor.Render("\"" + submatch[2] + "\"")
			}
			return match
		})
		
		// Replace number patterns
		line = jsonNumberPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := jsonNumberPattern.FindStringSubmatch(match)
			if len(submatch) > 2 && !strings.Contains(match, "\"") {
				return submatch[1] + numberColor.Render(submatch[2])
			}
			return match
		})
		
		// Replace boolean patterns
		line = jsonBooleanPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := jsonBooleanPattern.FindStringSubmatch(match)
			if len(submatch) > 2 && !strings.Contains(match, "\"") {
				return submatch[1] + booleanColor.Render(submatch[2])
			}
			return match
		})
		
		// Replace null patterns
		line = jsonNullPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := jsonNullPattern.FindStringSubmatch(match)
			if len(submatch) > 2 && !strings.Contains(match, "\"") {
				return submatch[1] + nullColor.Render(submatch[2])
			}
			return match
		})
		
		// Replace brackets
		line = jsonBracketPattern.ReplaceAllStringFunc(line, func(match string) string {
			return bracketColor.Render(match)
		})
		
		lines[i] = line
	}
	
	return strings.Join(lines, "\n")
}

// HighlightTOML applies simple syntax highlighting to TOML content
func HighlightTOML(content string) string {
	// Apply highlighting with regexes
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		// Replace headers [section]
		line = tomlHeaderPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := tomlHeaderPattern.FindStringSubmatch(match)
			if len(submatch) > 1 {
				return tomlHeaderColor.Render("[" + submatch[1] + "]")
			}
			return match
		})
		
		// Replace key patterns
		line = tomlKeyPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := tomlKeyPattern.FindStringSubmatch(match)
			if len(submatch) > 2 {
				return tomlKeyColor.Render(submatch[1]) + submatch[2] + "="
			}
			return match
		})
		
		// Replace string patterns
		line = tomlStringPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := tomlStringPattern.FindStringSubmatch(match)
			if len(submatch) > 2 {
				return "=" + submatch[1] + tomlStringColor.Render("\"" + submatch[2] + "\"")
			}
			return match
		})
		
		// Replace number patterns
		line = tomlNumberPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := tomlNumberPattern.FindStringSubmatch(match)
			if len(submatch) > 2 {
				return "=" + submatch[1] + tomlNumberColor.Render(submatch[2])
			}
			return match
		})
		
		// Replace boolean patterns
		line = tomlBooleanPattern.ReplaceAllStringFunc(line, func(match string) string {
			submatch := tomlBooleanPattern.FindStringSubmatch(match)
			if len(submatch) > 2 {
				return "=" + submatch[1] + tomlBooleanColor.Render(submatch[2])
			}
			return match
		})
		
		lines[i] = line
	}
	
	return strings.Join(lines, "\n")
}

// Highlight determines the file type and applies appropriate highlighting
func Highlight(content, fileType string) string {
	switch strings.ToLower(fileType) {
	case "json":
		return HighlightJSON(content)
	case "toml":
		return HighlightTOML(content)
	default:
		return content
	}
}

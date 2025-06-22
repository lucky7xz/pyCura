package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"go_interface/internal/data"
	"go_interface/internal/highlighter"
	"go_interface/internal/python"
)

// Define some friendly colors
var (
	highlightColor    = lipgloss.Color("#FDBCB4") // Soft coral pink
	titleColor        = lipgloss.Color("#66CDAA") // Medium aquamarine
	textColor         = lipgloss.Color("#F5F5F5") // White smoke
	secondaryTextColor = lipgloss.Color("#E6E6FA") // Lavender
	accentColor       = lipgloss.Color("#FFD700") // Gold
	bgColor           = lipgloss.Color("#282c34") // Dark background
	buttonColor       = lipgloss.Color("#5F9EA0") // Cadet Blue
	
	titleStyle = lipgloss.NewStyle().
		Foreground(titleColor).
		Bold(true).
		MarginLeft(2)
	
	itemStyle = lipgloss.NewStyle().
		Foreground(textColor)

	selectedItemStyle = lipgloss.NewStyle().
		Foreground(highlightColor).
		Bold(true)

	infoStyle = lipgloss.NewStyle().
		Foreground(secondaryTextColor).
		Italic(true).
		MarginTop(1).
		MarginLeft(2)
	
	accentStyle = lipgloss.NewStyle().
		Foreground(accentColor).
		Bold(true)
	
	buttonStyle = lipgloss.NewStyle().
		Foreground(textColor).
		Background(buttonColor).
		Padding(0, 2).
		MarginRight(1)
	
	activeButtonStyle = buttonStyle.Copy().
		Background(lipgloss.Color("#8FBC8F"))  // Dark Sea Green
)

// Item represents a config file item in the list
type Item struct {
	file data.ConfigFile
}

// Implement the list.Item interface for Item
func (i Item) Title() string {
	return i.file.Name
}

func (i Item) Description() string {
	return fmt.Sprintf("%s • %d bytes", 
		strings.ToUpper(i.file.FileType),
		i.file.Size)
}

func (i Item) FilterValue() string {
	return i.file.Name
}

// keyMap defines the keybindings for the application
type keyMap struct {
	Up    key.Binding
	Down  key.Binding
	Enter key.Binding
	Exec  key.Binding
	Quit  key.Binding
	Help  key.Binding
}

// newKeyMap creates a new keymap with default key bindings
func newKeyMap() keyMap {
	return keyMap{
		Up: key.NewBinding(
			key.WithKeys("up", "k"),
			key.WithHelp("↑/k", "up"),
		),
		Down: key.NewBinding(
			key.WithKeys("down", "j"),
			key.WithHelp("↓/j", "down"),
		),
		Enter: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "view file"),
		),
		Exec: key.NewBinding(
			key.WithKeys("e"),
			key.WithHelp("e", "execute script"),
		),
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q", "quit"),
		),
		Help: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "toggle help"),
		),
	}
}

// ShortHelp returns keybindings to be shown in the mini help view.
func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Enter, k.Exec, k.Quit, k.Help}
}

// FullHelp returns keybindings for the expanded help view.
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.Enter, k.Exec},
		{k.Quit, k.Help},
	}
}

// Model represents the application state
type Model struct {
	list           list.Model
	selectedFile   *data.ConfigFile
	executor       *python.Executor
	keyMap         keyMap
	help           help.Model
	showHelp       bool
	viewport       viewport.Model
	executionResult string
	viewMode       viewMode
	fileContent    string
}

// NewModel creates a new instance of the application model
func NewModel(executor *python.Executor, configFiles []data.ConfigFile) Model {
	// Convert config files to list items
	items := make([]list.Item, len(configFiles))
	for i, file := range configFiles {
		items[i] = Item{file: file}
	}

	// Create a new list
	listModel := list.New(items, list.NewDefaultDelegate(), 0, 0)
	listModel.Title = "Configuration Files"
	listModel.SetShowStatusBar(false)
	listModel.SetFilteringEnabled(false)
	listModel.SetShowHelp(false)
	listModel.Styles.Title = titleStyle
	listModel.Styles.FilterPrompt = accentStyle
	
	// Create viewport for displaying file contents and execution results
	viewportModel := viewport.New(0, 0)
	viewportModel.Style = lipgloss.NewStyle().Foreground(secondaryTextColor)

	return Model{
		list:      listModel,
		executor:  executor,
		keyMap:    newKeyMap(),
		help:      help.New(),
		viewport:  viewportModel,
	}
}

// Init initializes the Bubble Tea model
func (m Model) Init() tea.Cmd {
	return nil
}

// viewMode indicates what is currently being displayed in the viewport
type viewMode int

const (
	viewModeNone viewMode = iota
	viewModeContent
	viewModeExecution
)

// Update handles messages and user input
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var cmds []tea.Cmd

	
	
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		h, v := lipgloss.NewStyle().Margin(1, 2).GetFrameSize()
		m.list.SetSize(msg.Width-h, msg.Height/2-v)
		
		m.viewport.Width = msg.Width - h
		m.viewport.Height = msg.Height/2 - v
		
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keyMap.Quit):
			return m, tea.Quit
			
		case key.Matches(msg, m.keyMap.Enter):
			if i, ok := m.list.SelectedItem().(Item); ok {
				selectedFile := i.file
				m.selectedFile = &selectedFile
				
				// Read and highlight file content
				content, err := m.ReadFileContent(selectedFile)
				if err != nil {
					m.executionResult = fmt.Sprintf("Error reading file: %v", err)
					m.viewMode = viewModeExecution
				} else {
					m.fileContent = content
					m.viewMode = viewModeContent
				}
				m.viewport.SetContent(m.getContent())
				m.viewport.GotoTop()
			}
			
		case key.Matches(msg, m.keyMap.Help):
			m.showHelp = !m.showHelp
			
		case key.Matches(msg, m.keyMap.Exec):
			if m.selectedFile != nil {
				// For simplicity, directly use file content as potential input
				// This assumes the config file may contain any necessary input parameters
				result, err := m.executor.ExecutePythonScript(*m.selectedFile, nil)
				if err != nil {
					m.executionResult = fmt.Sprintf("Error executing script: %v", err)
				} else {
					m.executionResult = result
				}
				m.viewMode = viewModeExecution
				m.viewport.SetContent(m.getContent())
				// Auto-scroll to the bottom when showing execution results
				m.viewport.GotoBottom()
			}
		}
	}

	// Update list and viewport
	m.list, cmd = m.list.Update(msg)
	cmds = append(cmds, cmd)
	
	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

// ReadFileContent reads and highlights file content
func (m Model) ReadFileContent(file data.ConfigFile) (string, error) {
	// First, read the raw file content
	rawContent, err := m.executor.ReadFile(file.Path)
	if err != nil {
		return "", err
	}
	
	// Apply syntax highlighting based on file type
	highlightedContent := highlighter.Highlight(rawContent, file.FileType)
	return highlightedContent, nil
}

// getContent returns the appropriate content for the viewport based on view mode
func (m Model) getContent() string {
	switch m.viewMode {
	case viewModeContent:
		return m.fileContent
	case viewModeExecution:
		return m.executionResult
	default:
		return ""
	}
}

// View renders the UI
func (m Model) View() string {
	var sb strings.Builder

	sb.WriteString(m.list.View())
	sb.WriteString("\n")
	
	if m.selectedFile != nil {
		// Add visual buttons for actions
		viewButtonStyle := buttonStyle
		execButtonStyle := buttonStyle
		
		// Highlight the active button based on view mode
		if m.viewMode == viewModeContent {
			viewButtonStyle = activeButtonStyle
		} else if m.viewMode == viewModeExecution {
			execButtonStyle = activeButtonStyle
		}
		
		sb.WriteString("\n")
		sb.WriteString(viewButtonStyle.Render("[Enter] View Content"))
		sb.WriteString(execButtonStyle.Render("[E] Run Script"))
		sb.WriteString("\n\n")
		
		sb.WriteString(accentStyle.Render("Selected file: " + m.selectedFile.Name))
		
		// Show message about what's being displayed
		switch m.viewMode {
		case viewModeContent:
			sb.WriteString(" " + infoStyle.Render("(Showing file content)"))
		case viewModeExecution:
			sb.WriteString(" " + infoStyle.Render("(Showing execution result)"))
		}
		
		sb.WriteString("\n\n")
		sb.WriteString(m.viewport.View())
	} else {
		sb.WriteString("\n" + infoStyle.Render("Select a config file to view its contents or run the script"))
	}
	
	if m.showHelp {
		sb.WriteString("\n\n" + m.help.View(m.keyMap))
	} else {
		sb.WriteString("\n\n" + infoStyle.Render("Press ? for help"))
	}

	return sb.String()
}

// Run starts the Bubble Tea application
func Run(executor *python.Executor, configFiles []data.ConfigFile) error {
	model := NewModel(executor, configFiles)
	p := tea.NewProgram(model, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

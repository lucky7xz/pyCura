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
			key.WithHelp("enter", "select"),
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
	return []key.Binding{k.Up, k.Down, k.Enter, k.Quit, k.Help}
}

// FullHelp returns keybindings for the expanded help view.
func (k keyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{
		{k.Up, k.Down, k.Enter},
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
	
	// Create viewport for displaying execution results
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
				
				result, err := m.executor.ExecutePythonScript(selectedFile)
				if err != nil {
					m.executionResult = fmt.Sprintf("Error: %v", err)
				} else {
					m.executionResult = result
				}
				m.viewport.SetContent(m.executionResult)
			}
			
		case key.Matches(msg, m.keyMap.Help):
			m.showHelp = !m.showHelp
		}
	}

	// Update list and viewport
	m.list, cmd = m.list.Update(msg)
	cmds = append(cmds, cmd)
	
	m.viewport, cmd = m.viewport.Update(msg)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

// View renders the UI
func (m Model) View() string {
	var sb strings.Builder

	sb.WriteString(m.list.View())
	sb.WriteString("\n\n")
	
	if m.selectedFile != nil {
		sb.WriteString(accentStyle.Render("Selected file: " + m.selectedFile.Name))
		sb.WriteString("\n\n")
		sb.WriteString(m.viewport.View())
	} else {
		sb.WriteString(infoStyle.Render("Select a config file to run with src.cura.py"))
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

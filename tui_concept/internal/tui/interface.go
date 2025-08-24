package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"go_interface/internal/data"
	"go_interface/internal/highlighter"
	"go_interface/internal/python"
)

// Define some friendly colors
var (
	highlightColor     = lipgloss.Color("#FDBCB4") // Soft coral pink
	titleColor         = lipgloss.Color("#66CDAA") // Medium aquamarine
	textColor          = lipgloss.Color("#F5F5F5") // White smoke
	secondaryTextColor = lipgloss.Color("#E6E6FA") // Lavender
	accentColor        = lipgloss.Color("#FFD700") // Gold
	bgColor            = lipgloss.Color("#282c34") // Dark background
	buttonColor        = lipgloss.Color("#5F9EA0") // Cadet Blue

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
				Background(lipgloss.Color("#8FBC8F")) // Dark Sea Green

	helpStyle = lipgloss.NewStyle().Padding(1, 2)
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
	scriptRunning  bool
	scriptStatus   string
	progressPercent int
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
		list:     listModel,
		executor: executor,
		keyMap:   newKeyMap(),
		help:     help.New(),
		viewport: viewportModel,
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

// A message to indicate the script has finished executing
type scriptFinishedMsg struct {
	result string
	err    error
}

// A message to trigger the actual script execution after the UI has updated
type startScriptExecMsg struct{}

// A command to execute the python script asynchronously
// A command to execute the python script asynchronously
func execScriptCmd(executor *python.Executor, file data.ConfigFile) tea.Cmd {
	return func() tea.Msg {
		result, err := executor.ExecutePythonScript(file, nil)
		return scriptFinishedMsg{result: result, err: err}
	}
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

	// This message is received when the script has finished executing.
	case scriptFinishedMsg:
		m.scriptRunning = false
		m.scriptStatus = "Completed execution"
		m.progressPercent = 100
		if msg.err != nil {
			m.executionResult = fmt.Sprintf("Error executing script: %v", msg.err)
		} else {
			m.executionResult = msg.result
		}
		m.viewport.SetContent(m.getContent())
		m.viewport.GotoBottom()
		return m, nil

	// This message is received after the UI has had a chance to redraw.
	case startScriptExecMsg:
		// Now we can safely start the actual script execution.
		if m.selectedFile != nil {
			return m, execScriptCmd(m.executor, *m.selectedFile)
		}
		return m, nil

	case tea.KeyMsg:
		// Don't handle keys if a script is running.
		if m.scriptRunning {
			return m, nil
		}

		switch {
		case key.Matches(msg, m.keyMap.Quit):
			return m, tea.Quit

		case key.Matches(msg, m.keyMap.Enter):
			if i, ok := m.list.SelectedItem().(Item); ok {
				selectedFile := i.file
				m.selectedFile = &selectedFile
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

		// This is the first step of the two-step execution process.
		case key.Matches(msg, m.keyMap.Exec):
			if m.selectedFile != nil {
				m.scriptRunning = true
				m.scriptStatus = "Starting Python script..."
				m.progressPercent = 5
				m.viewMode = viewModeExecution
				m.executionResult = fmt.Sprintf("Executing Python script with config: %s...\n\nPlease wait...", m.selectedFile.Name)
				m.viewport.SetContent(m.getContent())
				// Return a command that sends a message to trigger the next step.
				return m, func() tea.Msg { return startScriptExecMsg{} }
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
// View renders the UI
func (m Model) View() string {
	// Help view, if active
	if m.showHelp {
		return helpStyle.Render(m.help.View(m.keyMap))
	}

	// Main content area
	var sb strings.Builder
	sb.WriteString(m.list.View())

	if m.selectedFile != nil {
		// Display status text above buttons if script is running
		if m.scriptRunning {
			sb.WriteString("\n")
			statusTextStyle := lipgloss.NewStyle().Padding(0, 1)
			processingStyle := lipgloss.NewStyle().Foreground(titleColor).Bold(true)
			sb.WriteString(statusTextStyle.Render("Status: " + processingStyle.Render(m.scriptStatus)))
		}

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

		// Display prettier progress bar below buttons if script is running
		if m.scriptRunning {
			sb.WriteString("\n") // Space below buttons

			barWidth := 40
			filledWidth := int(float64(barWidth) * float64(m.progressPercent) / 100.0)

			var bar strings.Builder
			for i := 0; i < barWidth; i++ {
				if i < filledWidth {
					bar.WriteString("━")
				} else {
					bar.WriteString(" ")
				}
			}

			progressBarStyle := lipgloss.NewStyle().
				Background(lipgloss.Color("#555")).
				Foreground(lipgloss.Color("#82E0AA"))

			percentStyle := lipgloss.NewStyle().Padding(0, 1).Foreground(textColor)

			sb.WriteString("\n")
			sb.WriteString(progressBarStyle.Render(bar.String()))
			sb.WriteString(percentStyle.Render(fmt.Sprintf(" %d%%", m.progressPercent)))
		}

		// Header for the content viewport
		sb.WriteString("\n\n")
		sb.WriteString(accentStyle.Render("Selected file: " + m.selectedFile.Name))
		if m.viewMode == viewModeExecution {
			sb.WriteString(" " + infoStyle.Render("(Showing execution result)"))
		}

		sb.WriteString("\n\n")
		sb.WriteString(m.viewport.View())
	} else {
		// Show instructions when no file is selected
		sb.WriteString("\n\n")
		sb.WriteString(infoStyle.Render("Select a config file to view its contents or run the script"))
		sb.WriteString("\n\n")
		sb.WriteString(infoStyle.Render("Press ? for help"))
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

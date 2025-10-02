#!/usr/bin/env python3
"""
Root-level TUI for pyCura with a dual-pane, interactive prompt design.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from queue import Queue
from typing import Optional, Callable

from textual.app import App, ComposeResult
from textual.containers import Vertical, Container
from textual.message import Message
from textual.widgets import (
    Header,
    Footer,
    Static,
    ListView,
    ListItem,
    Label,
    Button,
    ProgressBar,
    Rule,
)

REPO_ROOT = Path.cwd()
CONFIG_DIR = REPO_ROOT / "config_files"


# Message classes for robust, decoupled communication
class PromptState(Enum):
    SELECT_TARGET = auto()
    PRE_INSPECT = auto()
    POST_INSPECT = auto()
    EXPORT_CB = auto()
    EXPORT_DD = auto()
    DONE = auto()
    FAILED = auto()

@dataclass
class LogLine(Message):
    line: str

@dataclass
class ProgressUpdate(Message):
    progress: int

@dataclass
class StatusUpdate(Message):
    status: str

@dataclass
class PromptRequired(Message):
    state: PromptState

@dataclass
class ProcessFinished(Message):
    pass

@dataclass
class ProcessFailed(Message):
    error: str


def _resolve_python_interpreter(repo_root: Path) -> Path:
    candidates: list[Path] = []
    venv_env = os.environ.get("VIRTUAL_ENV")
    if venv_env:
        p = Path(venv_env)
        candidates.append(p / ("Scripts" if os.name == "nt" else "bin") / "python")
    for dirname in (".venv", "venv"):
        p = repo_root / dirname
        candidates.append(p / ("Scripts" if os.name == "nt" else "bin") / "python")
    candidates.append(Path(sys.executable))
    for cand in candidates:
        if cand.exists():
            return cand
    return Path(sys.executable)


class ConfigScreen(Static):
    def __init__(self, on_select: Callable[[str], None]):
        super().__init__()
        self.on_select = on_select

    def compose(self) -> ComposeResult:
        yield Label("Select a configuration file:", classes="title")
        files = sorted([p.name for p in CONFIG_DIR.iterdir() if p.is_file() and p.suffix in (".json", ".toml")])
        if not files:
            yield Label("No config files found in config_files/.", classes="hint")
        else:
            yield ListView(*[ListItem(Label(name)) for name in files], id="config-list")
        yield Label("Use ↑/↓ and Enter to select, or q to quit.", classes="help")

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        label = event.item.query_one(Label)
        self.on_select(str(label.renderable))


class PromptWidget(Static):
    def __init__(self, title: str, options: list[str], on_answer: Callable[[str], None]):
        super().__init__()
        self._title_text = title
        self._options = options
        self.on_answer = on_answer

    def compose(self) -> ComposeResult:
        yield Label(self._title_text, classes="title")
        yield Rule()
        for option in self._options:
            yield Button(option, variant="primary", name=option.lower())

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.name:
            self.on_answer(event.button.name)


class RunDashboard(Static):
    def __init__(self, config_filename: str):
        super().__init__()
        self.config_filename = config_filename
        self.python_path = _resolve_python_interpreter(REPO_ROOT)
        self.log_lines: list[str] = []
        self._proc: Optional[subprocess.Popen] = None
        self._input_queue: Queue[str] = Queue()

    def compose(self) -> ComposeResult:
        # Use a vertical layout: Controls on top, Logs on bottom
        with Vertical():
            with Vertical(id="top-pane"):
                yield Label("Controls", classes="pane-title")
                yield Static("Status: Starting...", id="status-display")
                yield Container(id="prompt-container")
            with Vertical(id="bottom-pane"):
                yield Label("Logs", classes="pane-title")
                yield ProgressBar(id="progress-bar", total=100)
                yield Static("Starting...", id="log-view")

    def on_mount(self) -> None:
        self.run_worker(self._run_cura_script, exclusive=True, thread=True)

    def _run_cura_script(self) -> None:
        try:
            self.post_message(StatusUpdate("Initializing..."))
            config_base = Path(self.config_filename).stem
            cmd = [str(self.python_path), "-m", "src.cura", config_base, "run"]
            
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT),
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            self.post_message(StatusUpdate("Running..."))

            for line in iter(self._proc.stdout.readline, ""):
                self._process_line(line)
            
            self._proc.wait()
            if self._proc.returncode != 0:
                self.post_message(ProcessFailed(f"Process exited with code {self._proc.returncode}"))
            else:
                self.post_message(ProcessFinished())

        except Exception as e:
            self.post_message(ProcessFailed(str(e)))

    def _process_line(self, line: str):
        self.post_message(LogLine(line.strip()))
        
        l = line.lower()
        # Progress updates
        if "initialized project-manager" in l: self.post_message(ProgressUpdate(10))
        elif "--- running edits ---" in l: self.post_message(ProgressUpdate(30))
        elif "editing completed" in l: self.post_message(ProgressUpdate(60))
        elif "project processing completed" in l: self.post_message(ProgressUpdate(90))
        elif "exported" in l: self.post_message(ProgressUpdate(100))

        # Prompt detection and handling
        prompt_map = {
            "What target(s) to inspect?": PromptState.SELECT_TARGET,
            "Press 'y' and enter to run initial inspections": PromptState.PRE_INSPECT,
            "Press 'y' and Enter to run post-transformation inspections": PromptState.POST_INSPECT,
            "Would you like to export the codebook keys?": PromptState.EXPORT_CB,
            "Would you like to export the domain data?": PromptState.EXPORT_DD,
        }
        for prompt_text, state in prompt_map.items():
            if prompt_text in line:
                self.post_message(PromptRequired(state))
                self.post_message(StatusUpdate("Awaiting Input..."))
                # Block and wait for input from the queue
                answer = self._input_queue.get()
                if self._proc and self._proc.stdin:
                    self._proc.stdin.write(answer)
                    self._proc.stdin.flush()
                self.post_message(StatusUpdate("Running..."))
                break

    def _update_log_view(self):
        log_view = self.query_one("#log-view", Static)
        log_view.update("\n".join(self.log_lines[-200:]))

    # --- Message Handlers ---
    def on_log_line(self, message: LogLine) -> None:
        self.log_lines.append(message.line)
        self._update_log_view()

    def on_progress_update(self, message: ProgressUpdate) -> None:
        self.query_one("#progress-bar", ProgressBar).update(progress=message.progress)

    def on_status_update(self, message: StatusUpdate) -> None:
        self.query_one("#status-display", Static).update(f"Status: {message.status}")

    def on_prompt_required(self, message: PromptRequired) -> None:
        self._render_prompt(message.state)

    def on_process_finished(self, message: ProcessFinished) -> None:
        self.query_one("#progress-bar", ProgressBar).update(progress=100)
        self.post_message(StatusUpdate("Finished"))
        self._render_prompt(PromptState.DONE)

    def on_process_failed(self, message: ProcessFailed) -> None:
        self.log_lines.append(f"\n[bold red]ERROR: {message.error}[/]")
        self._update_log_view()
        self.post_message(StatusUpdate("Failed"))
        self._render_prompt(PromptState.FAILED)

    def _render_prompt(self, state: PromptState) -> None:
        container = self.query_one("#prompt-container")
        container.remove_children()
        widget: Optional[Static] = None
        prompt_map = {
            PromptState.SELECT_TARGET: ("Step 1: Select Target", ["Codebook", "Domain Data", "Both"]),
            PromptState.PRE_INSPECT: ("Step 2: Initial Inspections?", ["Yes", "No"]),
            PromptState.POST_INSPECT: ("Step 3: Post-Transform Inspections?", ["Yes", "No"]),
            PromptState.EXPORT_CB: ("Step 4: Export Codebook?", ["Yes", "No"]),
            PromptState.EXPORT_DD: ("Step 5: Export Domain Data?", ["Yes", "No"]),
        }
        if state in prompt_map:
            title, options = prompt_map[state]
            widget = PromptWidget(title, options, self._answer_prompt)
        elif state == PromptState.DONE:
            widget = Label("Run complete. Press q to quit.")
        elif state == PromptState.FAILED:
            widget = Label("[bold red]Run failed.[/] Press q to quit.")

        if widget:
            container.mount(widget)

    def _answer_prompt(self, answer: str) -> None:
        self.query_one("#prompt-container").remove_children()
        cli_answer_map = {"codebook": "cb", "domain data": "dd", "both": "both", "yes": "y", "no": "n"}
        cli_answer = cli_answer_map.get(answer, answer) + "\n"
        self._input_queue.put(cli_answer)


class PyCuraTUI(App):
    CSS_PATH = "tui.css"
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self):
        super().__init__()
        self.config_filename: Optional[str] = None

    def action_quit(self) -> None:
        self.exit()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Container(id="app-body")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#app-body").mount(ConfigScreen(self._on_config_select))

    def _on_config_select(self, filename: str) -> None:
        self.config_filename = filename
        body = self.query_one("#app-body")
        body.remove_children()
        body.mount(RunDashboard(filename))

def main():
    if not (REPO_ROOT / "src").exists() or not CONFIG_DIR.exists():
        print("Please run this TUI from the repository root.")
        sys.exit(2)
    app = PyCuraTUI()
    app.run()

if __name__ == "__main__":
    main()

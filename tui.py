#!/usr/bin/env python3
"""
Root-level TUI for pyCura with a dual-pane, interactive prompt design.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Callable, Literal

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
    Input,
    ProgressBar,
)
from textual.validation import Validator, ValidationResult

# --- Global Constants ---
REPO_ROOT = Path.cwd()
CONFIG_DIR = REPO_ROOT / "config_files"

# --- Message classes for one-way data flow ---
@dataclass
class ScriptOutput(Message):
    """A message containing raw output from the script."""
    line: str

@dataclass
class ProcessFinished(Message):
    """A message indicating the script has finished."""
    return_code: int

@dataclass
class ProcessFailed(Message):
    """A message indicating the script failed to start."""
    error: str

# --- Conversation State Machine ---
class ConversationState(Enum):
    INIT = auto()
    AWAITING_TARGET = auto()
    AWAITING_PRE_INSPECT = auto()
    AWAITING_POST_INSPECT = auto()
    AWAITING_EXPORT_CB = auto()
    AWAITING_EXPORT_DD = auto()
    PROCESSING = auto()
    DONE = auto()

# --- Input Validation ---
class YesNoValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if value.lower() in ("y", "n", ""):
            return self.success()
        return self.failure("Only 'y' or 'n' allowed.")

class TargetValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if value.lower() in ("cb", "dd", "both"):
            return self.success()
        return self.failure("Only 'cb', 'dd', or 'both' allowed.")

# --- UI Components ---

def _resolve_python_interpreter(repo_root: Path) -> Path:
    # (Implementation unchanged)
    candidates: list[Path] = []
    venv_env = os.environ.get("VIRTUAL_ENV")
    if venv_env:
        p = Path(venv_env)
        candidates.append(p / ("Scripts" if os.name == "nt" else "bin") / "python")
    for dirname in (".venv", "venv"):
        p = repo_root / dirname
        candidates.append(p / ("Scripts"if os.name == "nt" else "bin") / "python")
    candidates.append(Path(sys.executable))
    for cand in candidates:
        if cand.exists():
            return cand
    return Path(sys.executable)


class ConfigScreen(Static):
    # (Implementation unchanged)
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


class RunDashboard(Static):
    """The 'Conversation Manager' UI."""
    def __init__(self, config_filename: str):
        super().__init__()
        self.config_filename = config_filename
        self.python_path = _resolve_python_interpreter(REPO_ROOT)
        self.log_lines: list[str] = []
        self._proc: Optional[subprocess.Popen] = None
        self.convo_state: ConversationState = ConversationState.INIT
        self.target: Optional[Literal["cb", "dd", "both"]] = None

    def compose(self) -> ComposeResult:
        with Vertical():
            with Vertical(id="top-pane"):
                yield Label(" ", id="prompt-label")
                yield Input(placeholder="...", id="main-input", disabled=True)
            with Vertical(id="bottom-pane"):
                yield Label("Logs", classes="pane-title")
                yield ProgressBar(id="progress-bar", total=100)
                yield Static("Initializing...", id="log-view")

    def on_mount(self) -> None:
        self.run_worker(self._run_cura_script, exclusive=True, thread=True)
        self._next_convo_step()

    def _run_cura_script(self) -> None:
        """'Dumb' Worker: Relays script output line-by-line."""
        try:
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
                errors='replace',
                bufsize=1
            )
            
            for line in iter(self._proc.stdout.readline, ""):
                self.post_message(ScriptOutput(line))
            
            self._proc.wait()
            self.post_message(ProcessFinished(self._proc.returncode))

        except Exception as e:
            self.post_message(ProcessFailed(str(e)))

    def _next_convo_step(self):
        """Drives the conversation forward based on the current state."""
        if self.convo_state == ConversationState.INIT:
            self.convo_state = ConversationState.AWAITING_TARGET
            self._activate_input(
                "Step 1/5: What target(s) to inspect? (cb/dd/both)",
                TargetValidator(),
                placeholder="cb/dd/both"
            )
        elif self.convo_state == ConversationState.AWAITING_TARGET:
            self.convo_state = ConversationState.AWAITING_PRE_INSPECT
            self._activate_input(
                "Step 2/5: Run initial inspections? (y/n, Enter for n)",
                YesNoValidator(),
                placeholder="n"
            )
        elif self.convo_state == ConversationState.AWAITING_PRE_INSPECT:
            self.convo_state = ConversationState.AWAITING_POST_INSPECT
            self._activate_input(
                "Step 3/5: Run post-transformation inspections? (y/n, Enter for n)",
                YesNoValidator(),
                placeholder="n"
            )
        elif self.convo_state == ConversationState.AWAITING_POST_INSPECT:
            if self.target in ("cb", "both"):
                self.convo_state = ConversationState.AWAITING_EXPORT_CB
                self._activate_input(
                    "Step 4/5: Export codebook keys? (y/n, Enter for y)",
                    YesNoValidator(),
                    placeholder="y"
                )
            elif self.target == "dd": # Skip to the dd export question
                self.convo_state = ConversationState.AWAITING_EXPORT_DD
                self._next_convo_step()
        elif self.convo_state == ConversationState.AWAITING_EXPORT_CB:
            if self.target == "both":
                self.convo_state = ConversationState.AWAITING_EXPORT_DD
                self._next_convo_step()
            else: # cb only, so we are done
                self.convo_state = ConversationState.DONE
                self._next_convo_step()
        elif self.convo_state == ConversationState.AWAITING_EXPORT_DD:
            if self.target in ("dd", "both"):
                 self._activate_input(
                    "Step 5/5: Export domain data? (y/n, Enter for y)",
                    YesNoValidator(),
                    placeholder="y"
                )
            self.convo_state = ConversationState.DONE
        elif self.convo_state == ConversationState.DONE:
            self.query_one("#prompt-label", Label).update("Run complete.")


    def _activate_input(self, label: str, validator: Validator, placeholder: str):
        """Configures and enables the main input widget for the user."""
        main_input = self.query_one("#main-input", Input)
        self.query_one("#prompt-label", Label).update(label)
        main_input.disabled = False
        main_input.validators = [validator]
        main_input.placeholder = placeholder
        main_input.value = ""
        main_input.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handles the user pressing Enter in the input box."""
        main_input = event.input
        if not main_input.is_valid:
            return

        answer = main_input.value or main_input.placeholder
        main_input.disabled = True
        self.query_one("#prompt-label", Label).update("Processing...")

        # Store the target for conditional questions
        if self.convo_state == ConversationState.AWAITING_TARGET:
            self.target = answer.lower()

        # Send the answer to the script
        if self._proc and self._proc.stdin:
            try:
                self._proc.stdin.write(answer + "\n")
                self._proc.stdin.flush()
            except (IOError, ValueError):
                self.post_message(ProcessFailed("Failed to write to process."))
        
        self.convo_state = ConversationState.PROCESSING # Wait for script output

    def on_script_output(self, message: ScriptOutput) -> None:
        """Receives raw output, displays it, and checks if it's time for the next question."""
        line = message.line.strip()
        if not line:
            return

        self.log_lines.append(line)
        self._update_log_view()
        
        # Update progress
        l = line.lower()
        if "initialized project-manager" in l: self.query_one("#progress-bar", ProgressBar).update(progress=10)
        elif "--- running edits ---" in l: self.query_one("#progress-bar", ProgressBar).update(progress=30)
        elif "editing completed" in l: self.query_one("#progress-bar", ProgressBar).update(progress=60)
        elif "project processing completed" in l: self.query_one("#progress-bar", ProgressBar).update(progress=90)
        elif "exported" in l: self.query_one("#progress-bar", ProgressBar).update(progress=100)

        # If we were processing, check if the script is now asking the next question
        if self.convo_state == ConversationState.PROCESSING:
            prompt_triggers = {
                "What target(s) to inspect?": ConversationState.AWAITING_TARGET,
                "run initial inspections": ConversationState.AWAITING_PRE_INSPECT,
                "run post-transformation inspections": ConversationState.AWAITING_POST_INSPECT,
                "export the codebook keys?": ConversationState.AWAITING_EXPORT_CB,
                "export the domain data?": ConversationState.AWAITING_EXPORT_DD,
            }
            for trigger, state in prompt_triggers.items():
                if trigger in line:
                    self.convo_state = state
                    self._next_convo_step()
                    break

    def on_process_finished(self, message: ProcessFinished) -> None:
        if message.return_code == 0:
            self.query_one("#prompt-label", Label).update("Run complete.")
        else:
            self.query_one("#prompt-label", Label).update(f"[bold red]Run Failed (Code {message.return_code})[/]")
        self.query_one("#main-input", Input).disabled = True

    def on_process_failed(self, message: ProcessFailed) -> None:
        self.query_one("#prompt-label", Label).update(f"[bold red]Error: {message.error}[/]")
        self.query_one("#main-input", Input).disabled = True

    def _update_log_view(self):
        log_view = self.query_one("#log-view", Static)
        log_view.update("\n".join(self.log_lines[-200:]))


class PyCuraTUI(App):
    CSS_PATH = "tui.css"
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self):
        super().__init__()
        self.config_filename: Optional[str] = None

    def action_quit(self) -> None:
        self.exit()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
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

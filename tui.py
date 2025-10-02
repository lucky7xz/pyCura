#!/usr/bin/env python3
"""
Root-level TUI for pyCura, designed around a modular conversation manager.
"""

from __future__ import annotations

import os
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
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
    Button,
)
from textual.validation import Validator, ValidationResult

# --- Global Constants ---
REPO_ROOT = Path.cwd()
CONFIG_DIR = REPO_ROOT / "config_files"

# --- Message classes for one-way data flow ---
@dataclass
class ScriptOutput(Message):
    line: str

@dataclass
class ProcessFinished(Message):
    return_code: int

@dataclass
class ProcessFailed(Message):
    error: str

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

class ResetTypeValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if value in ("1", "2"):
            return self.success()
        return self.failure("Only '1' or '2' allowed.")

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
        candidates.append(p / ("Scripts" if os.name == "nt" else "bin") / "python")
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
        yield ListView(*[ListItem(Label(name)) for name in files], id="config-list")

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        self.on_select(str(event.item.query_one(Label).renderable))

class CommandScreen(Static):
    """A screen to select the command to run (Run or Reset)."""
    def __init__(self, on_select: Callable[[str], None]):
        super().__init__()
        self.on_select = on_select

    def compose(self) -> ComposeResult:
        yield Label("Select a command:", classes="title")
        yield Button("Run", variant="primary", id="run")
        yield Button("Reset", variant="error", id="reset")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.on_select(event.button.id)

class ProcessDashboard(Static):
    """Abstract base class for a dashboard that runs a script and manages a conversation."""
    def __init__(self, config_filename: str, command: str):
        super().__init__()
        self.config_filename = config_filename
        self.command = command
        self.python_path = _resolve_python_interpreter(REPO_ROOT)
        self.log_lines: list[str] = []
        self._proc: Optional[subprocess.Popen] = None

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
        self.run_worker(self._run_script, exclusive=True, thread=True)
        self._next_convo_step()

    def _run_script(self) -> None:
        """'Dumb' Worker: Relays script output line-by-line."""
        try:
            config_base = Path(self.config_filename).stem
            cmd = [str(self.python_path), "-m", "src.cura", config_base, self.command]
            self._proc = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                cwd=str(REPO_ROOT), text=True, encoding='utf-8', errors='replace', bufsize=1
            )
            for line in iter(self._proc.stdout.readline, ""):
                self.post_message(ScriptOutput(line))
            self._proc.wait()
            self.post_message(ProcessFinished(self._proc.returncode))
        except Exception as e:
            self.post_message(ProcessFailed(str(e)))

    def _activate_input(self, label: str, validator: Validator, placeholder: str):
        main_input = self.query_one("#main-input", Input)
        self.query_one("#prompt-label", Label).update(label)
        main_input.disabled = False
        main_input.validators = [validator]
        main_input.placeholder = placeholder
        main_input.value = ""
        main_input.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if not event.input.is_valid:
            return
        answer = event.value or event.input.placeholder
        event.input.disabled = True
        self.query_one("#prompt-label", Label).update("Processing...")
        self._handle_answer(answer)

    def on_script_output(self, message: ScriptOutput) -> None:
        line = message.line.strip()
        if not line: return
        self.log_lines.append(line)
        self._update_log_view()
        self._process_output_line(line)

    def on_process_finished(self, message: ProcessFinished) -> None:
        label = self.query_one("#prompt-label", Label)
        if message.return_code == 0:
            label.update("Run complete.")
        else:
            label.update(f"[bold red]Run Failed (Code {message.return_code})[/]")
        self.query_one("#main-input", Input).disabled = True

    def on_process_failed(self, message: ProcessFailed) -> None:
        self.query_one("#prompt-label", Label).update(f"[bold red]Error: {message.error}[/]")
        self.query_one("#main-input", Input).disabled = True

    def _update_log_view(self):
        self.query_one("#log-view", Static).update("\n".join(self.log_lines[-200:]))

    def _next_convo_step():
        raise NotImplementedError
    def _handle_answer(self, answer: str):
        raise NotImplementedError
    def _process_output_line(self, line: str):
        raise NotImplementedError

class RunDashboard(ProcessDashboard):
    """Conversation manager for the 'run' command."""
    def __init__(self, config_filename: str):
        super().__init__(config_filename, "run")
        self.convo_state = ConversationState.INIT
        self.target: Optional[Literal["cb", "dd", "both"]] = None

    def _next_convo_step(self):
        # (Implementation from previous version, slightly adapted)
        if self.convo_state == ConversationState.INIT:
            self.convo_state = ConversationState.AWAITING_TARGET
            self._activate_input("Step 1/5: What target(s) to inspect? (cb/dd/both)", TargetValidator(), "cb")
        elif self.convo_state == ConversationState.AWAITING_TARGET:
            self.convo_state = ConversationState.AWAITING_PRE_INSPECT
            self._activate_input("Step 2/5: Run initial inspections? (y/n, Enter for n)", YesNoValidator(), "n")
        elif self.convo_state == ConversationState.AWAITING_PRE_INSPECT:
            self.convo_state = ConversationState.AWAITING_POST_INSPECT
            self._activate_input("Step 3/5: Run post-transformation inspections? (y/n, Enter for n)", YesNoValidator(), "n")
        elif self.convo_state == ConversationState.AWAITING_POST_INSPECT:
            if self.target in ("cb", "both"):
                self.convo_state = ConversationState.AWAITING_EXPORT_CB
                self._activate_input("Step 4/5: Export codebook keys? (y/n, Enter for y)", YesNoValidator(), "y")
            elif self.target == "dd":
                self.convo_state = ConversationState.AWAITING_EXPORT_DD
                self._next_convo_step()
        elif self.convo_state == ConversationState.AWAITING_EXPORT_CB:
            if self.target == "both":
                self.convo_state = ConversationState.AWAITING_EXPORT_DD
                self._next_convo_step()
            else:
                self.convo_state = ConversationState.DONE
        elif self.convo_state == ConversationState.AWAITING_EXPORT_DD:
            if self.target in ("dd", "both"):
                self._activate_input("Step 5/5: Export domain data? (y/n, Enter for y)", YesNoValidator(), "y")
            self.convo_state = ConversationState.DONE

    def _handle_answer(self, answer: str):
        if self.convo_state == ConversationState.AWAITING_TARGET:
            self.target = answer.lower()
        if self._proc and self._proc.stdin:
            self._proc.stdin.write(answer + "\n")
            self._proc.stdin.flush()
        self.convo_state = ConversationState.PROCESSING

    def _process_output_line(self, line: str):
        # Progress bar logic
        l = line.lower()
        progress_bar = self.query_one("#progress-bar", ProgressBar)
        if "initialized project-manager" in l: progress_bar.update(progress=10)
        elif "--- running edits ---" in l: progress_bar.update(progress=30)
        elif "editing completed" in l: progress_bar.update(progress=60)
        elif "project processing completed" in l: progress_bar.update(progress=90)
        elif "exported" in l: progress_bar.update(progress=100)

        # Trigger for next conversation step
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

class ResetDashboard(ProcessDashboard):
    """Conversation manager for the 'reset' command."""
    class ResetState(Enum):
        INIT = auto()
        AWAITING_TYPE = auto()
        AWAITING_CONFIRMATION = auto()
        PROCESSING = auto()

    def __init__(self, config_filename: str):
        super().__init__(config_filename, "reset")
        self.convo_state = self.ResetState.INIT
        self.reset_type_desc = ""

    def _next_convo_step(self):
        if self.convo_state == self.ResetState.INIT:
            self.convo_state = self.ResetState.AWAITING_TYPE
            self._activate_input("Step 1/2: What to reset? (1: Output, 2: Entire Project)", ResetTypeValidator(), "1")

    def _handle_answer(self, answer: str):
        if self.convo_state == self.ResetState.AWAITING_TYPE:
            self.reset_type_desc = "ONLY the data output" if answer == "1" else "the ENTIRE project"
            self.convo_state = self.ResetState.AWAITING_CONFIRMATION
            self._activate_input(f"Step 2/2: Confirm reset of {self.reset_type_desc}? (y/n, Enter for n)", YesNoValidator(), "n")
        elif self.convo_state == self.ResetState.AWAITING_CONFIRMATION:
            self.convo_state = self.ResetState.PROCESSING

        if self._proc and self._proc.stdin:
            self._proc.stdin.write(answer + "\n")
            self._proc.stdin.flush()

    def _process_output_line(self, line: str):
        # The reset script is simple and doesn't require complex output parsing.
        # We just wait for it to finish.
        pass

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
        body.mount(CommandScreen(self._on_command_select))

    def _on_command_select(self, command: str) -> None:
        body = self.query_one("#app-body")
        body.remove_children()
        if command == "run":
            body.mount(RunDashboard(self.config_filename))
        elif command == "reset":
            body.mount(ResetDashboard(self.config_filename))

def main():
    if not (REPO_ROOT / "src").exists() or not CONFIG_DIR.exists():
        print("Please run this TUI from the repository root.")
        sys.exit(2)
    app = PyCuraTUI()
    app.run()

if __name__ == "__main__":
    main()
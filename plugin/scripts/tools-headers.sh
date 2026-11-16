#!/bin/sh
printf '{"X-Claude-Pid":"%s"' "$PPID"
for name in CLAUDE_CONFIG_DIR CLAUDE_LONG_RUNNING_DRIVE CLAUDE_LONG_RUNNING_LANE CCI_PARENT_SESSION CCI_AGENT_ID CCI_HOME CCI_UNSENT CODEX_THREAD_ID ORCA_TERMINAL_HANDLE ORCA_CLI_BIN_DIR ORCA_LAUNCH_RUN SHELL; do
	eval "value=\${$name-}"
	[ -n "$value" ] && printf ',"X-Cci-Env-%s":"%s"' "$name" "$(printf '%s' "$value" | sed 's/[\\"]/\\&/g')"
done
printf '}\n'

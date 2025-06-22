#!/usr/bin/env python3

# This is a wrapper script that helps execute the main pyCura script
# with the proper Python path configuration

import os
import sys
import subprocess

# Get the absolute path to the pyCura root directory
current_dir = os.path.dirname(os.path.abspath(__file__))
pycura_root = os.path.dirname(current_dir)  # Go up one level to pyCura root

# Add the pyCura root to Python path
sys.path.insert(0, pycura_root)

# The main script path
main_script = os.path.join(pycura_root, 'src', 'cura.py')

# Pass all arguments to the main script
cmd = [sys.executable, main_script] + sys.argv[1:]

# Execute the command with the proper environment
env = os.environ.copy()
env['PYTHONPATH'] = pycura_root + ':' + env.get('PYTHONPATH', '')

# Run the script and capture output
result = subprocess.run(cmd, env=env, text=True, capture_output=True)

# Print output and error to stdout for the Go application to capture
print(result.stdout)
if result.stderr:
    print("ERRORS:")
    print(result.stderr)

# Exit with the same code as the script
sys.exit(result.returncode)

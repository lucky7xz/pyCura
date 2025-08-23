# Python 3.12 venv Setup on Ubuntu 22.04

## Problem

When attempting to create a Python 3.12 virtual environment on Ubuntu 22.04, the process may fail with an error related to the `ensurepip` module. This prevents the successful creation of a `venv` with `pip` installed.

## Solution

The workaround is to create the virtual environment without `pip` initially, and then install `pip` manually.

### Steps:

1.  **Create the venv without pip:**
    ```bash
    python3.12 -m venv venv --without-pip
    ```

2.  **Download the official pip installer:**
    ```bash
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    ```

3.  **Install pip into the new venv:**
    ```bash
    ./venv/bin/python get-pip.py
    ```

4.  **Clean up the installer:**
    ```bash
    rm get-pip.py
    ```

## Rationale

The `ensurepip` module, responsible for bootstrapping `pip` into a new virtual environment, appears to have issues on some Ubuntu 22.04 systems with Python 3.12. By creating the `venv` with the `--without-pip` flag, we bypass this problematic module. We then use the official `get-pip.py` script to manually install `pip`, which is a more reliable method in this specific scenario.

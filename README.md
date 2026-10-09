# 1. Prerequisites and Setup

Ensure Python 3.9+ and `pip` are installed.

```bash
# Initialize virtual environment
python3 -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# Upgrade pip and install dependencies
python3 -m pip install --upgrade pip
pip install grpcio grpcio-tools pytest
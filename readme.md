# MELODY

## Environment Setup

### Prerequisites
- Python (>=3.8)
- [Poetry](https://python-poetry.org/docs/#installation)

### Setup
1. **Install Dependencies**: `poetry install`
2. **Activate Environment**: `poetry shell`

### Verify Setup
Ensure correct Python version: `poetry run python --version`

### Initialise Database
1. **Create Database**: `poetry run python -m initialise_database.py`

## Jupyter Notebooks
1. **Install Jupyter**: `poetry add jupyter` (if not in `pyproject.toml`)
2. **Start Notebook**: `poetry run jupyter notebook`

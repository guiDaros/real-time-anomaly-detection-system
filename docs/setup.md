# Environment Setup

## WSL2

Project runs inside Ubuntu WSL2.

Project path:

```bash
/home/guilhermedaros/projects/anomaly-detection-system
```

---

## Open Project

Inside Ubuntu:

```bash
cd ~/projects/anomaly-detection-system
code .
```

---

## Activate Python Environment

```bash
source $(poetry env info --path)/bin/activate
```

---

## Dependency Management

Install dependencies:

```bash
poetry install
```

Add dependency:

```bash
poetry add package-name
```

---

## Current Stack

- Python 3.11
- Poetry
- Docker Desktop
- WSL2 Ubuntu

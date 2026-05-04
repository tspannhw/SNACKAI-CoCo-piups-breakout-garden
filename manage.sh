#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/venv"

usage() {
    echo "Usage: ./manage.sh {build|test|run|run-simulate|run-dashboard|lint|clean}"
    echo ""
    echo "Commands:"
    echo "  build          Install dependencies into venv"
    echo "  test           Run pytest suite (simulation mode)"
    echo "  run            Run the sensor streaming app"
    echo "  run-simulate   Run in simulation mode (no hardware required)"
    echo "  run-dashboard  Launch local Streamlit dashboard"
    echo "  lint           Run flake8 linter"
    echo "  clean          Remove venv and caches"
    exit 1
}

ensure_venv() {
    if [ ! -d "${VENV_DIR}" ]; then
        echo "Creating virtual environment..."
        python3 -m venv "${VENV_DIR}"
    fi
    source "${VENV_DIR}/bin/activate"
}

cmd_build() {
    ensure_venv
    pip install --upgrade pip
    pip install -r "${SCRIPT_DIR}/requirements.txt"
    echo "Build complete."
}

cmd_test() {
    ensure_venv
    python -m pytest "${SCRIPT_DIR}/tests/" -v --tb=short
}

cmd_run() {
    ensure_venv
    python -m app.main "$@"
}

cmd_run_simulate() {
    ensure_venv
    python -m app.main --simulate "$@"
}

cmd_run_dashboard() {
    ensure_venv
    pip install streamlit --quiet
    streamlit run "${SCRIPT_DIR}/dashboard/dashboard.py"
}

cmd_lint() {
    ensure_venv
    pip install flake8 --quiet
    flake8 "${SCRIPT_DIR}/app/" --max-line-length=120 --ignore=E501,W503
}

cmd_clean() {
    rm -rf "${VENV_DIR}" "${SCRIPT_DIR}/__pycache__" "${SCRIPT_DIR}/.pytest_cache"
    find "${SCRIPT_DIR}" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    echo "Cleaned."
}

case "${1:-}" in
    build)         cmd_build ;;
    test)          cmd_test ;;
    run)           shift; cmd_run "$@" ;;
    run-simulate)  shift; cmd_run_simulate "$@" ;;
    run-dashboard) cmd_run_dashboard ;;
    lint)          cmd_lint ;;
    clean)         cmd_clean ;;
    *)             usage ;;
esac

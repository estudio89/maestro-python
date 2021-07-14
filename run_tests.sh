set -e
if [ $# -eq 0 ]; then
	pytest -s --cov maestro --cov-report html --mypy
	open htmlcov/index.html
else
	pytest -s ${@:1} --mypy
fi
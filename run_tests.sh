# Ex: ./run_tests.sh tests/firestore/test_full_sync.py::FirestoreFullSyncTest::test_full_sync_no_conflict
set -e
if [ $# -eq 0 ]; then
	pytest -s --cov maestro --cov-report html --mypy
	open htmlcov/index.html
else
	pytest -s ${@:1} --mypy
fi
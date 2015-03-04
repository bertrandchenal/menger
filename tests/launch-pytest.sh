export PYTHONPATH=../
export PATH=../bin/:$PATH
py.test -s --maxfail=1 #  --cov menger --cov-report term-missing --cov-report html

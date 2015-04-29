export PYTHONPATH=../
export PATH=../bin/:$PATH

OPTS=""

for i in "$@"
do
    case $i in
        -c|--coverage)
            OPTS="--cov menger --cov-report term-missing --cov-report html"
            shift
            ;;
        -h|--help)
            echo "Usage: launch-pytest [-c|--coverage]"
            exit 0
            ;;
    esac
done

py.test -s --maxfail=1  --tb=native $OPTS $@

#!/bin/bash

# Ensure dependencies are up to date
poetry install

# 0 = profiling off
# 1 = profiling on
PROFILE=1


# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --profile) PROFILE=1 ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Function to generate profile report
generate_report() {
    echo "Generating profile report..."
    poetry run python -c "import pstats; p = pstats.Stats('output.prof'); p.sort_stats('cumulative').print_stats()" > profile_report.txt
    echo "Profiling complete. Check profile_report.txt for results."
}

if [ $PROFILE -eq 1 ]; then
    echo "Running with profiling..."
    # Run with cProfile
    poetry run python -m cProfile -o output.prof src/web_crawler/main.py
    
    # Generate report even if interrupted
    generate_report
else
    echo "Running without profiling..."
    # Run the Python script normally
    poetry run python src/web_crawler/main.py
fi

# Trap ctrl-c and call generate_report()
trap generate_report INT
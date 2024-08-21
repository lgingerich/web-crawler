#!/bin/bash

# Ensure dependencies are up to date
poetry install

# 0 = profiling off
# 1 = cProfile
# 2 = line_profiler
# 3 = scalene
PROFILE=0

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --profile) PROFILE=1 ;;
        --line-profile) PROFILE=2 ;;
        --scalene) PROFILE=3 ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Function to generate profile report for cProfile
generate_cprofile_report() {
    echo "Generating cProfile report..."
    poetry run python -c "import pstats; p = pstats.Stats('output.prof'); p.sort_stats('cumulative').print_stats()" > profile_report.txt
    echo "Profiling complete. Check profile_report.txt for results."
}

# Function to generate line profile report
generate_lineprofile_report() {
    echo "Generating line profile report..."
    poetry run python -m line_profiler output.lprof > line_profile_report.txt
    echo "Line profiling complete. Check line_profile_report.txt for results."
}

# Function to generate Scalene report
generate_scalene_report() {
    echo "Scalene profiling complete. Check scalene_report.html for results."
}

# Trap ctrl-c and call appropriate report generation
trap 'if [ $PROFILE -eq 1 ]; then generate_cprofile_report; elif [ $PROFILE -eq 2 ]; then generate_lineprofile_report; elif [ $PROFILE -eq 3 ]; then generate_scalene_report; fi' INT

if [ $PROFILE -eq 1 ]; then
    echo "Running with cProfile..."
    poetry run python -m cProfile -o output.prof src/web_crawler/main.py
    generate_cprofile_report
elif [ $PROFILE -eq 2 ]; then
    echo "Running with line_profiler..."
    poetry run kernprof -l -o output.lprof src/web_crawler/main.py
    generate_lineprofile_report
elif [ $PROFILE -eq 3 ]; then
    echo "Running with Scalene..."
    poetry run python -m scalene --html --outfile scalene_report.html src/web_crawler/main.py
    if [ $? -eq 0 ]; then
        echo "Scalene profiling complete. Check scalene_report.html for results."
    else
        echo "Scalene profiling failed. Please check if Scalene is properly installed and the script path is correct."
    fi
else
    echo "Running without profiling..."
    poetry run python src/web_crawler/main.py
fi
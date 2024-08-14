# Performance Benchmarks

| Date       | URL's Crawled per Second (Total) | URL's Crawled per Second (Successful) | Crawl Success Rate (%) | Commit Hash                              |
|------------|----------------------------------|---------------------------------------|------------------------|------------------------------------------|
| 2024-08-14 | 0.27                             | 0.16                                  | 56%                    | 267531120494ea6d4ecb291b66ec7fc561361e09 |


# Development

## Setting Up the Development Environment

To install dependencies, run:

```bash
poetry install
```

This will install all the dependencies defined in your `pyproject.toml` file.

## Activating the Virtual Environment

To activate the project's virtual environment, run:

```bash
poetry shell
```

This will spawn a shell with the virtual environment activated, allowing you to run 
Python scripts and commands within the virtual environment context.

## Running Tests

To run the test suite with pytest, use:

```bash
poetry run pytest
```

This will execute all tests found in the `tests/` directory.

## Formatting Code

To format your code automatically with Black, run:

```bash
poetry run black .
```

Black will reformat your files in place to adhere to its style guide.

## Linting Code

To lint your code with Ruff, run:

```bash
poetry run ruff .
```

Ruff will analyze your code for potential errors and style issues.



# Notes / To Do

scalable version of this consumer/scraper should be k8s with multiple pods

for repeated scraping, store a hash of robots.txt and the page content for each url

add more metadata for each url
    successful_crawl_count
    failed_crawl_count
    
    
add automated handling for database/table setup and connection


"INFO - scraper - Metadata saved for amazonaws.com"
    - this logging message may not be properly setup for when the scraping fails


cleanup `metadata.json` 
    this shouldn't be created anymore

maybe not all url html data getting downloaded

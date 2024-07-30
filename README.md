# Python Template

This is a template for Python projects using Poetry for dependency management and 
packaging.

## Development

### Setting Up the Development Environment

To install dependencies, run:

```bash
poetry install
```

This will install all the dependencies defined in your `pyproject.toml` file.

### Activating the Virtual Environment

To activate the project's virtual environment, run:

```bash
poetry shell
```

This will spawn a shell with the virtual environment activated, allowing you to run 
Python scripts and commands within the virtual environment context.

### Running Tests

To run the test suite with pytest, use:

```bash
poetry run pytest
```

This will execute all tests found in the `tests/` directory.

### Formatting Code

To format your code automatically with Black, run:

```bash
poetry run black .
```

Black will reformat your files in place to adhere to its style guide.

### Linting Code

To lint your code with Ruff, run:

```bash
poetry run ruff .
```

Ruff will analyze your code for potential errors and style issues.


# Notes / To Do

send waiting url's to kafka producer
    idea (untested):
        have max XX url's waiting in the queue from the producer
        once url's get consumed, backfill to keep the queue full

kafka queue feeds consumer 

consumer = scraper

can't push data through kafka so need to save data on the consumer/scraper side

scalable version of this consumer/scraper should be k8s with multiple pods

for repeated scraping, store a hash of robots.txt and the page content for each url
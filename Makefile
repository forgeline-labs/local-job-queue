.PHONY: test lint format run

test:
	python3 -m unittest discover -s tests -v

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .

run:
	./serve --port 8080

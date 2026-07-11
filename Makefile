.PHONY: download build validate pipeline clean

START ?= 2010-01-01
END ?= 2026-07-08
SOURCE_PRIORITY ?= dukascopy,truefx,histdata
TIMEOUT ?= 120

download:
	python3 scripts/download_eurusd.py --source-priority $(SOURCE_PRIORITY) --start $(START) --end $(END) --timeout $(TIMEOUT)

build:
	python3 scripts/build_m15.py

validate:
	python3 scripts/validate_data.py

pipeline: download build validate

clean:
	rm -rf scripts/__pycache__

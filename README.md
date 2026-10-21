# finra_otc
Retrieve FINRA OTC Transparency Data

## Setup
```zsh
conda env create -f environment.yaml
conda activate finra-otc
pre-commit install
```

and
```shell
pip install -e ".[dev,test]"
```

## Usage
```shell
python retrieve.py 2026-01-01   # download files updated since this date to data/
python to_parquet.py            # combine data/ into data/otc.parquet
```

`to_parquet.py` keeps only the newest version of each republished week, and
writes one row group per weekly file.

## Tests
```shell
pytest
```

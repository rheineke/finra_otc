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

## Tests
```shell
pytest
```

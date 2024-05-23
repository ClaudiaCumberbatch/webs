"""Molecular Design Workflow.

This workflow is based on the
[Molecular Design with Parsl](https://github.com/ExaWorks/molecular-design-parsl-demo/blob/6c0dbc598091634074cd3a8f23815819ab77f91e/0_molecular-design-with-parsl.ipynb).

## Installation

This workflow has certain dependencies which require Conda to install.
To get started, create a Conda environment, install the Conda dependencies,
and then install the `webs` package into the Conda environment.

```bash
conda create -p $PWD/env python=3.11
conda activate ./env
conda install -c conda-forge xtb-python==22.1
pip install -e .[moldesign]
```

## Data

The data needs to be downloaded first.
```bash
curl -o data/QM9-search.tsv https://raw.githubusercontent.com/ExaWorks/molecular-design-parsl-demo/main/data/QM9-search.tsv
```

## Running

```bash
python -m webs.run moldesign --dataset data/QM9-search.tsv --executor process-pool --max-processes 4
```

Additional parameters are available with `python -m webs.run moldesign --help`.
"""  # noqa: E501

from __future__ import annotations

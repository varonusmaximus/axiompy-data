# axiompy-data

Distribution **`axiompy-data`** adds the `axiompy.data` namespace (pip merges it with core `axiompy`).

Requires **`axiompy`>=2** (core). Install from PyPI when published, or install core from source first.

```bash
pip install "axiompy>=2,<3"
pip install "axiompy-data[test-all]>=2,<3"
```

For local development with sibling checkouts:

```bash
pip install -e ../axiompy
pip install -e ".[test-all]"
```

Repository: [https://github.com/varonusmaximus/axiompy-data](https://github.com/varonusmaximus/axiompy-data)

If **`axiompy` is not yet on PyPI**, install it from Git before installing this package:

```bash
pip install "git+https://github.com/varonusmaximus/axiompy.git"
pip install -e ".[test-all]"
```

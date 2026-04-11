# SDM Project

## Prerequisites

- Python 3.10+
- Neo4j running locally (default connection: `neo4j://127.0.0.1:7687`, user: `neo4j`, password: `sdmproject`)
- Required Python packages:
  ```bash
  pip install neo4j pandas lxml
  ```

### Updating Neo4j credentials

If your Neo4j instance uses a different URI, username, or password, update the constants at the top of each of the following files before running them:

- `A/A.2/UploadCSV.py`
- `A/A.3/UploadUpdateCSV.py`
- `B/B1.py`, `B/B2.py`, `B/B3.py`, `B/B4.py`

```python
NEO4J_URI      = "neo4j://127.0.0.1:7687"  # change to your URI
NEO4J_USER     = "neo4j"                    # change to your username
NEO4J_PASSWORD = "sdmproject"               # change to your password
```

---

## A.1 — Graph Schema Design

No code to run. The proposed property graph schema is documented in:

- `A/A.1/Initial schema.png` — visual diagram of nodes, relationships, and properties

---

## A.2 — Data Generation and Import

All commands must be run from inside the `A/A.2/` directory.

**Step 1 — Download the DBLP source files**

Download the following two files from [https://dblp.org/xml/](https://dblp.org/xml/) and place them in `A/A.2/dblp_data/`:

- `dblp.xml.gz` — the compressed DBLP dataset
- `dblp.dtd` — the DTD schema file

Then decompress the XML file manually or with:

```bash
# Linux / macOS
gunzip A/A.2/dblp_data/dblp.xml.gz

# Windows (PowerShell)
Expand-Archive -Path A\A.2\dblp_data\dblp.xml.gz -DestinationPath A\A.2\dblp_data\
# Alternatively use 7-Zip or any archive tool to extract dblp.xml from dblp.xml.gz
```

**Step 2 — Parse the XML into raw CSVs**

Run from inside `A/A.2/`:

```bash
cd A/A.2
python dblp_data/XMLToCSV.py dblp_data/dblp.xml dblp_data/dblp.dtd dblp_data/output.csv --neo4j --relations author:WRITES cite:CITED_IN
```

This generates one CSV file per DBLP record type (e.g. `output_article.csv`, `output_inproceedings.csv`, `output_proceedings.csv`, ...) plus their corresponding header files, all inside `dblp_data/`. This step can take several minutes depending on hardware.

**Step 3 — Generate the graph-ready CSVs**

Reads the raw CSVs from `dblp_data/` and writes node and relationship CSVs to `nodes_and_relations/`.

```bash
python FormatCSV.py
```

**Step 4 — Import into Neo4j**

Reads the CSVs from `nodes_and_relations/` and populates the Neo4j database with nodes, relationships, constraints, and indexes.

```bash
python UploadCSV.py
```

> Neo4j must be running before executing this step. The script reads the CSV files directly from the local filesystem — no need to copy files to the Neo4j import directory.

---

## A.3 — Schema Evolution

Extends the graph with `Type_of_institution` nodes linked to authors via `AFFILIATED_WITH`, and adds the `is_approved` property to all `REVIEWS` edges.

All commands must be run from inside the `A/A.3/` directory. **A.2 must have been completed first.**

**Step 1 — Generate the update CSV files**

Reads authors and reviews from `A.2/nodes_and_relations/` and writes update CSVs to `update_data/`.

```bash
cd A/A.3
python FormatUpdateCSV.py
```

**Step 2 — Apply the updates to Neo4j**

```bash
python UploadUpdateCSV.py
```

---

## B — Analytical Queries

Four independent scripts, each running one Cypher query against the live Neo4j database. **A.2 and A.3 must have been completed first.**

All commands can be run from the project root or from inside `B/`.

| Script | Description |
|--------|-------------|
| `B1.py` | Top 3 most cited papers per conference/workshop |
| `B2.py` | Community of each conference (authors in ≥ 4 editions) |
| `B3.py` | Impact Factor of journals (2-year citation window) |
| `B4.py` | H-index of authors (top 50) |

It is recommended to redirect the output to a file for easier inspection:

```bash
python B/B1.py > B/B1.out
python B/B2.py > B/B2.out
python B/B3.py > B/B3.out
python B/B4.py > B/B4.out
```

To print directly to the terminal instead, run without redirection:

```bash
python B/B1.py
```

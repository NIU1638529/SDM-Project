#!/usr/bin/env python3
"""
UploadUpdateCSV.py — Apply the A.3 schema evolution to the Neo4j graph.

Reads CSVs from update_data/ and:
  1. Creates Type_of_institution nodes (University / Private).
  2. Creates AFFILIATED_WITH edges from each Author to one institution.
  3. Sets is_approved property on existing REVIEWS edges.

Run FormatUpdateCSV.py first to generate the CSVs.
"""
import csv
from pathlib import Path
from neo4j import GraphDatabase

NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "sdmproject"

CSV_DIR = Path("update_data")


def get_csv_data(filename):
    path = CSV_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}. Run FormatUpdateCSV.py first.")
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def run_batch_query(session, label, query, data, batch_size=1000):
    if not data:
        print(f"  Skipping {label} — no data.")
        return
    print(f"  Processing {label} ({len(data)} rows)...")
    for i in range(0, len(data), batch_size):
        session.run(f"UNWIND $rows AS row {query}", rows=data[i:i + batch_size]).consume()
    print(f"  Done {label}.")


def run_updates(uri, user, password):
    print(f"Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:

        # --- 1. Type_of_institution nodes ---
        print("\nLoading Type_of_institution nodes...")
        run_batch_query(session, "Type_of_institution nodes", """
            MERGE (i:Type_of_institution {name: row.institution_name})
        """, get_csv_data("institution_node.csv"))

        # --- 2. AFFILIATED_WITH (Author -> Type_of_institution) ---
        print("\nLoading AFFILIATED_WITH relationships...")
        run_batch_query(session, "AFFILIATED_WITH (Author -> Type_of_institution)", """
            MATCH (a:Author {name: row.author_name})
            MATCH (i:Type_of_institution {name: row.institution_name})
            MERGE (a)-[:AFFILIATED_WITH]->(i)
        """, get_csv_data("affiliated_with_relation.csv"))

        # --- 3. REVIEWS edges with is_approved ---
        # MERGE creates the relationship if it doesn't exist yet (extra reviewers from A.3)
        # and matches the existing one if it does (base reviewers from A.2).
        print("\nLoading REVIEWS edges with is_approved...")
        run_batch_query(session, "REVIEWS with is_approved", """
            MATCH (a:Author {name: row.author_name})
            MATCH (p:Paper {id: row.paper_id})
            MERGE (a)-[r:REVIEWS]->(p)
            SET r.is_approved = row.is_approved
        """, get_csv_data("reviews_approved_relation.csv"))

        # --- Verification ---
        print("\nChecking that update was successful...")

        q1 = (
            "MATCH (a:Author)-[:AFFILIATED_WITH]->(i:Type_of_institution)\n"
            "RETURN i.name AS institution, count(a) AS total\n"
            "ORDER BY institution"
        )
        print(f"\n  Query: {q1}")
        rows = session.run(q1).data()
        print("  Result:")
        for r in rows:
            print(f"    {r['institution']}: {r['total']} authors")

        q2 = (
            "MATCH ()-[r:REVIEWS]->()\n"
            "RETURN r.is_approved AS approved, count(r) AS total\n"
            "ORDER BY approved"
        )
        print(f"\n  Query: {q2}")
        rows = session.run(q2).data()
        print("  Result:")
        for r in rows:
            print(f"    is_approved = {r['approved']}: {r['total']} edges")

    driver.close()
    print("\nUpdate completed.")
    print("\nTo explore the updated graph, run these queries in Neo4j Desktop:")
    print("")
    print("  // Authors per institution type")
    print("  MATCH (a:Author)-[:AFFILIATED_WITH]->(i:Type_of_institution)")
    print("  RETURN i.name AS institution, count(a) AS total ORDER BY institution")
    print("")
    print("  // REVIEWS approval breakdown")
    print("  MATCH ()-[r:REVIEWS]->()")
    print("  RETURN r.is_approved AS approved, count(r) AS total ORDER BY approved")
    print("")
    print("  // Papers with the most reviewers (should show 5 for selected workshops/journals)")
    print("  MATCH ()-[r:REVIEWS]->(p:Paper)")
    print("  RETURN p.title AS paper, count(r) AS num_reviewers")
    print("  ORDER BY num_reviewers DESC LIMIT 10")
    print("")
    print("  // Reviewers of a specific paper (replace the title)")
    print("  MATCH (a:Author)-[r:REVIEWS]->(p:Paper)")
    print("  WHERE p.title = '<paper title here>'")
    print("  RETURN a.name AS reviewer, r.is_approved AS approved")


if __name__ == "__main__":
    run_updates(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

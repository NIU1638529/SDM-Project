#!/usr/bin/env python3
import csv
from pathlib import Path
from neo4j import GraphDatabase

# NEO4J configuration ()
NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "sdmproject"

# Directory where your CSV files are located
CSV_DIR = Path(r"./nodes_and_relations") 

def get_csv_data(filename):
    """Reads local CSV using semicolon delimiter and returns a list of rows."""
    path = CSV_DIR / filename
    if not path.exists():
        print(f"  Error: {filename} not found in {CSV_DIR}")
        return []
    with open(path, mode='r', encoding='utf-8') as f:
        return list(csv.DictReader(f, delimiter=';'))

def run_batch_query(session, label, query, data, batch_size=1000):
    """Sends data to Neo4j in batches using UNWIND for automation."""
    if not data:
        return
    print(f"  Processing {label} ({len(data)} rows).")
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        # UNWIND $rows AS row turns the Python list into Cypher rows
        session.run(f"UNWIND $rows AS row {query}", rows=batch).consume()
    print(f"  Finished {label}.")

def run_neo4j_import(uri, user, password):
    print(f"Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:

        # We create the CONSTRAINTS and INDEXES ---
        print("\nCreating constraints and indexes...")
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper)      REQUIRE p.title IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author)     REQUIRE a.name  IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (j:Journal)    REQUIRE j.name  IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Volume)     REQUIRE v.id    IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Conference) REQUIRE c.name  IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Edition)    REQUIRE e.id    IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Keyword)    REQUIRE k.topic IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (p:Paper) ON (p.id)"
        ]
        for q in constraints:
            session.run(q)

        # We create the NODES using batch Size 5000
        print("\nLoading nodes:")
        n_batch = 5000

        run_batch_query(session, "Paper nodes", """
            MERGE (p:Paper {title: row.title})
            SET p.id = row.paper_id, p.pages = row.pages, p.doi = row.doi,
                p.abstract = row.abstract, p.year = toInteger(row.year),
                p.citation_count = toInteger(row.citation_count)
        """, get_csv_data('paper_node.csv'), n_batch)

        run_batch_query(session, "Author nodes", """
            MERGE (a:Author {name: row.name})
        """, get_csv_data('author_node.csv'), n_batch)

        run_batch_query(session, "Journal nodes", """
            MERGE (j:Journal {name: row.journal_name})
        """, get_csv_data('journal_node.csv'), n_batch)

        run_batch_query(session, "Volume nodes", """
            MERGE (v:Volume {id: row.volume_id})
            SET v.number = row.number
        """, get_csv_data('volume_node.csv'), n_batch)

        run_batch_query(session, "Conference nodes", """
            MERGE (c:Conference {name: row.conf_name})
            SET c.type = row.type
        """, get_csv_data('conference_node.csv'), n_batch)

        run_batch_query(session, "Edition nodes", """
            MERGE (e:Edition {id: row.edition_id})
            SET e.title = row.title, e.number = row.edition_number, e.city = row.city
        """, get_csv_data('edition_node.csv'), n_batch)

        run_batch_query(session, "Keyword nodes", """
            MERGE (k:Keyword {topic: row.topic_name})
        """, get_csv_data('topic_node.csv'), n_batch)

        # We create the RELATIONSHIPS using batch Size 1000
        print("\nLoading relationships:")
        r_batch = 1000

        run_batch_query(session, "WRITES (Author -> Paper)", """
            MATCH (a:Author {name: row.author_id})
            MATCH (p:Paper {id: row.paper_id})
            MERGE (a)-[r:WRITES]->(p)
            SET r.role = row.role
        """, get_csv_data('writes_relation.csv'), r_batch)

        run_batch_query(session, "PUBLISHED_IN (Paper -> Volume)", """
            MATCH (p:Paper {id: row.paper_id})
            MATCH (v:Volume {id: row.container_id})
            MERGE (p)-[:PUBLISHED_IN]->(v)
        """, get_csv_data('published_in_relation.csv'), r_batch)

        run_batch_query(session, "PUBLISHED_IN (Paper -> Edition)", """
            MATCH (p:Paper {id: row.paper_id})
            MATCH (e:Edition {id: row.container_id})
            MERGE (p)-[:PUBLISHED_IN]->(e)
        """, get_csv_data('published_in_relation.csv'), r_batch)

        run_batch_query(session, "BELONGS_TO (Volume -> Journal)", """
            MATCH (v:Volume {id: row.child_id})
            MATCH (j:Journal {name: row.parent_id})
            MERGE (v)-[:BELONGS_TO]->(j)
        """, get_csv_data('belongs_to_relation.csv'), r_batch)

        run_batch_query(session, "BELONGS_TO (Edition -> Conference)", """
            MATCH (e:Edition {id: row.child_id})
            MATCH (c:Conference {name: row.parent_id})
            MERGE (e)-[:BELONGS_TO]->(c)
        """, get_csv_data('belongs_to_relation.csv'), r_batch)

        run_batch_query(session, "REVIEWS (Author -> Paper)", """
            MATCH (a:Author {name: row.author_name})
            MATCH (p:Paper {id: row.paper_id})
            MERGE (a)-[:REVIEWS]->(p)
        """, get_csv_data('reviews_relation.csv'), r_batch)

        run_batch_query(session, "CITES (Paper -> Paper)", """
            MATCH (ps:Paper {id: row.paper_id_source})
            MATCH (pt:Paper {id: row.paper_id_target})
            MERGE (ps)-[r:CITES]->(pt)
            SET r.citation_year = toInteger(row.citation_year)
        """, get_csv_data('cites_relation.csv'), r_batch)

        run_batch_query(session, "FOCUSED_ON (Paper -> Keyword)", """
            MATCH (p:Paper {id: row.paper_id})
            MATCH (k:Keyword {topic: row.topic_name})
            MERGE (p)-[:FOCUSED_ON]->(k)
        """, get_csv_data('focused_on_relation.csv'), r_batch)

    driver.close()
    print("\nImport completed successfully.")

if __name__ == "__main__":
    run_neo4j_import(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
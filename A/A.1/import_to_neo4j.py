#! /usr/bin/env python3
from neo4j import GraphDatabase

# --- NEO4J CONFIGURATION ---
NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "adal2003" 

def run_neo4j_import(uri, user, password):
    print(f"Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    # List of tuples: (Label for the print, Cypher query)
    steps = [
        ("Constraints and Indexes", "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper) REQUIRE p.title IS UNIQUE"),
        ("Constraints and Indexes", "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.name IS UNIQUE"),
        ("Constraints and Indexes", "CREATE CONSTRAINT IF NOT EXISTS FOR (j:Journal) REQUIRE j.name IS UNIQUE"),
        ("Constraints and Indexes", "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Volume) REQUIRE v.id IS UNIQUE"),
        ("Constraints and Indexes", "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Conference) REQUIRE c.name IS UNIQUE"),
        ("Constraints and Indexes", "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Edition) REQUIRE e.id IS UNIQUE"),
        ("Constraints and Indexes", "CREATE INDEX IF NOT EXISTS FOR (p:Paper) ON (p.id)"),

        ("Paper nodes", "LOAD CSV WITH HEADERS FROM 'file:///paper_node.csv' AS row FIELDTERMINATOR ';' MERGE (p:Paper {title: row.title}) SET p.id = row.paper_id, p.pages = row.pages, p.doi = row.doi, p.abstract = row.abstract, p.year = toInteger(row.year)"),
        ("Author nodes", "LOAD CSV WITH HEADERS FROM 'file:///author_node.csv' AS row FIELDTERMINATOR ';' MERGE (a:Author {name: row.name})"),
        ("Journal nodes", "LOAD CSV WITH HEADERS FROM 'file:///journal_node.csv' AS row FIELDTERMINATOR ';' MERGE (j:Journal {name: row.journal_name})"),
        ("Volume nodes", "LOAD CSV WITH HEADERS FROM 'file:///volume_node.csv' AS row FIELDTERMINATOR ';' MERGE (v:Volume {id: row.volume_id}) SET v.number = row.number"),
        ("Conference nodes", "LOAD CSV WITH HEADERS FROM 'file:///conference_node.csv' AS row FIELDTERMINATOR ';' MERGE (c:Conference {name: row.conf_name}) SET c.type = row.type"),
        ("Edition nodes", "LOAD CSV WITH HEADERS FROM 'file:///edition_node.csv' AS row FIELDTERMINATOR ';' MERGE (e:Edition {id: row.edition_id}) SET e.title = row.title, e.number = row.edition_number, e.city = row.city"),
        
        ("WRITES relations", "LOAD CSV WITH HEADERS FROM 'file:///writes_relation.csv' AS row FIELDTERMINATOR ';' MATCH (a:Author {name: row.author_id}) MATCH (p:Paper {id: row.paper_id}) MERGE (a)-[r:WRITES]->(p) SET r.role = row.role"),
        ("PUBLISHED_IN relations", "LOAD CSV WITH HEADERS FROM 'file:///published_in_relation.csv' AS row FIELDTERMINATOR ';' MATCH (p:Paper {id: row.paper_id}) MATCH (c) WHERE (c:Volume OR c:Edition) AND c.id = row.container_id MERGE (p)-[:PUBLISHED_IN]->(c)"),
        ("BELONGS_TO relations", "LOAD CSV WITH HEADERS FROM 'file:///belongs_to_relation.csv' AS row FIELDTERMINATOR ';' MATCH (child) WHERE child.id = row.child_id MATCH (parent) WHERE (parent:Journal OR parent:Conference) AND parent.name = row.parent_id MERGE (child)-[:BELONGS_TO]->(parent)"),
        ("REVIEWS relations", "LOAD CSV WITH HEADERS FROM 'file:///reviews_relation.csv' AS row FIELDTERMINATOR ';' MATCH (a:Author {name: row.author_name}) MATCH (p:Paper {id: row.paper_id}) MERGE (a)-[:REVIEWS]->(p)"),
        ("CITES relations", "LOAD CSV WITH HEADERS FROM 'file:///cites_relation.csv' AS row FIELDTERMINATOR ';' MATCH (ps:Paper {id: row.paper_id_source}) MATCH (pt:Paper {id: row.paper_id_target}) MERGE (ps)-[:CITES]->(pt)")
    ]

    with driver.session() as session:
        for i, (label, query) in enumerate(steps):
            print(f"Step {i+1} of {len(steps)}: Processing {label}...")
            session.run(query)
                
    driver.close()
    print("\nIMPORT COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    run_neo4j_import(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
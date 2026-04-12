#!/usr/bin/env python3
"""
Query C1 — Defining the Database Community.
"""
main = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if main not in sys.path:
    sys.path.append(main)

import Configuration

NEO4J_URI      = Configuration.NEO4J_URI
NEO4J_USER     = Configuration.NEO4J_USER
NEO4J_PASSWORD = Configuration.NEO4J_PASSWORD

#NEO4J_URI      = "neo4j://127.0.0.1:7687"
#NEO4J_USER     = "neo4j"
#NEO4J_PASSWORD = "adal2003"

QUERY = """
MERGE (comm:Community {field: "Databases"})
WITH comm
MATCH (k:Keyword)
WHERE k.topic IN [
    "data management", "indexing", "data modeling", 
    "big data", "data processing", "data storage", "data querying"
]
MERGE (comm)-[:COVERS]->(k)
RETURN comm.field AS community, collect(k.topic) AS topics
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    results = session.run(QUERY).data()
rows = []
for r in results:
    rows.append({
        "community":    r["community"],
        "all_keywords": ", ".join(r["topics"]) 
    })

driver.close()

df = pd.DataFrame(rows)
pd.set_option('display.max_colwidth', None) 
pd.set_option('display.expand_frame_repr', False)

print("\n--- Stage 1: Community 'Databases' Defined ---")

print(df.to_string(index=False, justify='left'))
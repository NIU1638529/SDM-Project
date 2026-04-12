#!/usr/bin/env python3
"""
Query C2 — Identifying Database Venues.
Threshold: 90% of papers must have database keywords.
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
MATCH (c:Community {field: "Databases"})
MATCH (v)<-[:BELONGS_TO]-(container)<-[:PUBLISHED_IN]-(p:Paper)
WHERE v:Conference OR v:Journal
WITH c, v, count(DISTINCT p) AS total_p
OPTIONAL MATCH (v)<-[:BELONGS_TO]-()<-[:PUBLISHED_IN]-(p_db:Paper)-[:FOCUSED_ON]->(k:Keyword)<-[:COVERS]-(c)
WITH c, v, total_p, count(DISTINCT p_db) AS db_p
WHERE total_p > 0 AND (toFloat(db_p) / total_p) >= 0.9
MERGE (v)-[:RELATED_TO]->(c)
RETURN c.field AS community, collect({
    name: v.name, 
    total: total_p, 
    db_count: db_p, 
    ratio: toFloat(db_p)/total_p
}) AS venues_list
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    results = session.run(QUERY).data()

rows = []
for r in results:
    # Descomponemos la colección 'venues_list'
    for v in r["venues_list"]:
        rows.append({
            "Community": r["community"],
            "Venue Name": v["name"],
            "Total Papers": v["total"],
            "DB Papers": v["db_count"],
            "Affinity": f"{int(v['ratio']*100)}%"
        })

driver.close()


df = pd.DataFrame(rows)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)

print("\n--- Stage 2: Database Community Venues (>= 90% Affinity) ---")
if not df.empty:
    print(df.to_string(index=False, justify='left'))
else:
    print("No venues met the 90% threshold. Maybe the data is too diverse?")
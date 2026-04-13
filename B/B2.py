#!/usr/bin/env python3
"""
Query 2 — Community of each conference/workshop.

Authors that have published papers in at least 4 different editions
of the same conference/workshop.
"""
from neo4j import GraphDatabase
import pandas as pd
import os
import sys
main = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if main not in sys.path:
    sys.path.append(main)

import Configuration

# 3. Asignas las variables (fíjate que ahora usamos Configuration con C mayúscula)
NEO4J_URI      = Configuration.NEO4J_URI
NEO4J_USER     = Configuration.NEO4J_USER
NEO4J_PASSWORD = Configuration.NEO4J_PASSWORD

#NEO4J_URI      = "neo4j://127.0.0.1:7687"
#NEO4J_USER     = "neo4j"
#NEO4J_PASSWORD = "sdmproject"

QUERY = """
MATCH (c:Conference)<-[:BELONGS_TO]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)<-[:WRITES]-(a:Author)
WITH c, a, count(DISTINCT e) AS editions_count
WHERE editions_count >= 4
RETURN c.name        AS conference,
       c.type        AS type,
       a.name        AS author,
       editions_count
ORDER BY conference, editions_count DESC
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    results = session.run(QUERY).data()

driver.close()

df = pd.DataFrame(results)
print(f"Total community members found: {len(df)}")
print(f"Conferences with community:    {df['conference'].nunique()}")
print(df.head(15).to_string(index=False))

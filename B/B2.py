#!/usr/bin/env python3
"""
Query 2 — Community of each conference/workshop.

Authors that have published papers in at least 4 different editions
of the same conference/workshop.
"""
from neo4j import GraphDatabase
import pandas as pd

NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "sdmproject"

QUERY = """
MATCH (a:Author)-[:WRITES]->(p:Paper)-[:PUBLISHED_IN]->(e:Edition)-[:BELONGS_TO]->(c:Conference)
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

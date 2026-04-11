#!/usr/bin/env python3
"""
Query 1 — Top 3 most cited papers of each conference/workshop.

Optimization: uses p.citation_count (pre-computed property) instead of
counting CITES relationships at query time with COUNT { ()-[:CITES]->(p) }.
"""
from neo4j import GraphDatabase
import pandas as pd

NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "sdmproject"

QUERY = """
MATCH (c:Conference)<-[:BELONGS_TO]-(e:Edition)<-[:PUBLISHED_IN]-(p:Paper)
WITH c, p, p.citation_count AS times_cited
ORDER BY c.name, times_cited DESC
WITH c, collect({title: p.title, times_cited: times_cited})[0..3] AS top3
RETURN c.name AS conference, c.type AS type, top3
ORDER BY conference
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    results = session.run(QUERY).data()

rows = []
for r in results:
    for rank, paper in enumerate(r["top3"], start=1):
        rows.append({
            "conference":  r["conference"],
            "type":        r["type"],
            "rank":        rank,
            "title":       paper["title"],
            "times_cited": paper["times_cited"],
        })

driver.close()

df = pd.DataFrame(rows)
print(f"Total conferences/workshops: {df['conference'].nunique()}")
print(df.to_string(index=False))

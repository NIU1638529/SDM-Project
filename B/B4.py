#!/usr/bin/env python3
"""
Query 4 — H-index of authors (top 50).

H-index = largest h such that the author has at least h papers each cited >= h times.

Optimization: uses p.citation_count (pre-computed property) instead of
COUNT { ()-[:CITES]->(p) }, avoiding the traversal of all CITES edges per paper.
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
MATCH (a:Author)-[:WRITES]->(p:Paper)
WITH a, p.citation_count AS citations
ORDER BY a.name, citations DESC
WITH a, collect(citations) AS citation_list
WITH a, citation_list, size(citation_list) AS total
WITH a, [i IN range(0, total - 1)
         WHERE citation_list[i] >= i + 1 | i + 1] AS h_values
RETURN a.name AS author,
       CASE WHEN size(h_values) > 0 THEN last(h_values) ELSE 0 END AS h_index
ORDER BY h_index DESC
LIMIT 50
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    results = session.run(QUERY).data()

driver.close()

df = pd.DataFrame(results)
print("Top 50 authors by h-index:")
print(df.to_string(index=False))

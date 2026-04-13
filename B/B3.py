#!/usr/bin/env python3
"""
Query 3 — Impact Factor of journals.

IF = total citations received by papers published in Y-1 and Y-2,
     divided by the number of papers published in Y-1 and Y-2,
     where Y is the most recent publication year of the journal.

Starting from the paper nodes in the window (indexed) avoids a full CITES scan.
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
// find the latest publication year per journal
MATCH (j:Journal)<-[:BELONGS_TO]-(v:Volume)<-[:PUBLISHED_IN]-(p:Paper)
WHERE p.year IS NOT NULL
WITH j, max(p.year) AS ref_year

// sum citation_count for papers published in the 2-year window (Y-1, Y-2)
MATCH (j)<-[:BELONGS_TO]-(v2:Volume)<-[:PUBLISHED_IN]-(p2:Paper)
WHERE p2.year IN [ref_year - 1, ref_year - 2]
WITH j, ref_year,
     count(p2)                          AS papers_published,
     sum(coalesce(p2.citation_count, 0)) AS citations_in_window

RETURN j.name             AS journal,
       ref_year,
       papers_published,
       citations_in_window,
       round(toFloat(citations_in_window) / papers_published, 3) AS impact_factor
ORDER BY impact_factor DESC
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:
    results = session.run(QUERY).data()

driver.close()

df = pd.DataFrame(results)
print(f"Journals with impact factor: {len(df)}")
print(df.to_string(index=False))

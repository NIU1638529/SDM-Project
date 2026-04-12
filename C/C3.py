#!/usr/bin/env python3
"""
C3.py — Identify the Top-100 papers of the Databases community.
Calculates the ranking based on citations coming from the community itself.
"""
from neo4j import GraphDatabase
import pandas as pd
import os
import sys


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

# The exact query we validated in the Neo4j Browser
QUERY = """
// Start by finding the Community ("Databases") and the connected Venues
MATCH (c:Community {field: "Databases"})<-[:RELATED_TO]-(v)
// Get all papers published in those top venues
MATCH (target_paper:Paper)-[:PUBLISHED_IN]->(container)-[:BELONGS_TO]->(v)
// Find which other papers cite our "target_papers"
MATCH (citing_paper:Paper)-[:CITES]->(target_paper)
// Require the citing paper to also belong to the community 
MATCH (citing_paper)-[:FOCUSED_ON]->(k:Keyword)<-[:COVERS]-(c)
// Group by target paper, count valid distinct citations, order, and limit to 100
WITH target_paper, v, count(DISTINCT citing_paper) AS num_citations
ORDER BY num_citations DESC
LIMIT 100

// Mark these top-100 papers in the graph with a label and a property for future use
SET target_paper:KeyPaper, target_paper.isKey = true

// Return the final list to verify the results
RETURN target_paper.title AS Paper, 
       v.name AS Venue, 
       num_citations AS Community_Citations
ORDER BY Community_Citations DESC
LIMIT 100
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

print("Executing C3 query: Searching for the most cited papers by the community...")

with driver.session() as session:
    results = session.run(QUERY).data()

driver.close()

rows = []
for i, r in enumerate(results, start=1):
    
    
    title = r["Paper"]
    title_resume = title[:10] + "..." if len(title) > 10 else title
    
    rows.append({
        "Rank": i,
        "Paper": title_resume,
        "Venue": r["Venue"],
        "Citations": r["Community_Citations"]
    })

df = pd.DataFrame(rows)

print("\n--- C3 RESULTS: KEY PAPERS IN DATABASES COMMUNITY ---")
if not df.empty:
    print(df.to_string(index=False))
    print(f"\nFound {len(df)} papers meeting the criteria.")
else:
    print("No results found.")
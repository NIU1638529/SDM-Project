#!/usr/bin/env python3#!/usr/bin/env python3
"""
C4.py — Identify Potential Reviewers and Gurus.
Authors of Top-100 papers become potential reviewers.
Authors with >= 2 Top-100 papers are labeled as Gurus.
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

QUERY = """
// Match authors who have written papers previously identified as KeyPapers
MATCH (expert:Author)-[:WRITES]->(kp:KeyPaper)

// Aggregate by author and count their total number of elite publications
WITH expert, count(kp) AS elite_works_count

//All identified authors assigned as potential reviewers
SET expert:PotentialReviewer, expert.is_community_expert = true

// 4. Identify the most prolific authors (Gurus) without filtering the reviewers
FOREACH (ignore IN CASE WHEN elite_works_count >= 2 THEN [1] ELSE [] END |
    SET expert:Guru
)
// Return the full list of all experts (both standard reviewers and Gurus),
RETURN expert.name AS ResearcherName, 
       elite_works_count AS PublicationCount, 
       labels(expert) AS CategoryTags 
ORDER BY PublicationCount DESC;
"""

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

print("Executing C4 query: Upgrading authors to Reviewers and Gurus.")

with driver.session() as session:
    results = session.run(QUERY).data()

driver.close()
rows = []
for i, r in enumerate(results, start=1):
    is_guru = "Guru" in r["CategoryTags"]
    
    rows.append({
        "Rank": i,
        "Author": r["ResearcherName"],           # <-- Corregido
        "Top-100 Papers": r["PublicationCount"], # <-- Corregido
        "Is Guru?": "YES" if is_guru else "No"   # <-- Corregido
    })

df = pd.DataFrame(rows)

print("\n--- C4 RESULTS: POTENTIAL REVIEWERS & GURUS ---")
if not df.empty:
    print(df.to_string(index=False))
    
    gurus_names = [r["ResearcherName"] for r in results if "Guru" in r["CategoryTags"]]
    
    total_reviewers = len(df)
    total_gurus = len(gurus_names)
    
    print(f"\nSummary:")
    print(f"- Total Potential Reviewers identified: {total_reviewers}")
    print(f"- Total Gurus identified (>=2 papers): {total_gurus}")
    
    # Si hay al menos un Guru, imprimimos su nombre
    if total_gurus > 0:
        print("- Guru Names:")
        for name in gurus_names:
            print(f"    * {name}")
else:
    print("No results found.")
#!/usr/bin/env python3
"""
PageRank — Identifies the most influential papers in the citation network.

Uses Neo4j GDS PageRank on a native projection of Paper nodes and CITES
relationships. Results are written back to Paper.pagerank for persistence,
so the score is available for downstream queries without re-running.

Requires: Neo4j Graph Data Science (GDS) plugin installed.
"""
from neo4j import GraphDatabase
import pandas as pd

NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "sdmproject"

GRAPH_NAME = "paper-citations"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:

    # Drop stale projection if it exists from a previous run
    try:
        session.run("CALL gds.graph.drop($name)", name=GRAPH_NAME).consume()
    except Exception:
        pass

    # Native projection: Paper nodes + CITES relationships
    # Native projections are loaded directly from the store — faster than Cypher projections
    proj = session.run("""
        CALL gds.graph.project($name, 'Paper', 'CITES')
        YIELD nodeCount, relationshipCount
    """, name=GRAPH_NAME).data()[0]
    print(f"Projected graph: {proj['nodeCount']} papers, {proj['relationshipCount']} citations")

    # Run PageRank in WRITE mode
    # WRITE mode persists scores to Paper.pagerank in a single transaction,
    # which is more efficient than STREAM mode for large graphs
    stats = session.run("""
        CALL gds.pageRank.write($name, {
            writeProperty:  'pagerank',
            dampingFactor:  0.85,
            maxIterations:  20,
            tolerance:      1e-7
        })
        YIELD nodePropertiesWritten, ranIterations, didConverge, computeMillis
    """, name=GRAPH_NAME).data()[0]

    print(f"Iterations: {stats['ranIterations']}  |  "
          f"Converged: {stats['didConverge']}  |  "
          f"Compute time: {stats['computeMillis']} ms")
    print(f"Properties written: {stats['nodePropertiesWritten']}\n")

    # Top 20 papers by PageRank score
    rows = session.run("""
        MATCH (p:Paper)
        WHERE p.pagerank IS NOT NULL
        RETURN p.title                  AS title,
               p.year                   AS year,
               p.citation_count         AS citation_count,
               round(p.pagerank, 6)     AS pagerank
        ORDER BY pagerank DESC
        LIMIT 20
    """).data()

    # Free the in-memory projected graph
    session.run("CALL gds.graph.drop($name)", name=GRAPH_NAME).consume()

driver.close()

df = pd.DataFrame(rows)
print("Top 20 most influential papers by PageRank:")
print(df.to_string(index=False))

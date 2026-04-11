#!/usr/bin/env python3
"""
Louvain — Detects communities of authors based on co-authorship.

Builds a co-authorship graph via Cypher projection: two authors are connected
if they have written at least one paper together. The edge weight equals the
number of papers they have co-authored.

Louvain community IDs are written back to Author.community for persistence.

Requires: Neo4j Graph Data Science (GDS) plugin installed.
"""
from neo4j import GraphDatabase
import pandas as pd

NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "sdmproject"

GRAPH_NAME = "author-coauthorship"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

with driver.session() as session:

    # Drop stale projection if it exists from a previous run
    try:
        session.run("CALL gds.graph.drop($name)", name=GRAPH_NAME).consume()
    except Exception:
        pass

    # Cypher projection: virtual co-authorship graph
    #
    # Authors are not directly linked in the schema, so we derive the graph:
    #   - Nodes : all Author nodes
    #   - Edges : (a1)-[weight=shared_papers]->(a2) for every pair that co-authored
    #
    # WHERE id(a1) < id(a2) avoids creating duplicate (a1->a2 and a2->a1) pairs,
    # halving the projection cost and preventing double-counting in the algorithm.
    proj = session.run("""
        CALL gds.graph.project.cypher(
            $name,
            'MATCH (a:Author) RETURN id(a) AS id',
            'MATCH (a1:Author)-[:WRITES]->(p:Paper)<-[:WRITES]-(a2:Author)
             WHERE id(a1) < id(a2)
             RETURN id(a1) AS source, id(a2) AS target, count(p) AS weight'
        )
        YIELD nodeCount, relationshipCount
    """, name=GRAPH_NAME).data()[0]
    print(f"Projected graph: {proj['nodeCount']} authors, "
          f"{proj['relationshipCount']} co-authorship pairs")

    # Run Louvain in WRITE mode
    # relationshipWeightProperty uses co-authorship count as edge weight:
    #   stronger connections (more shared papers) pull authors into the same community
    # includeIntermediateCommunities: false — only store the final partition,
    #   reducing memory usage significantly on large graphs
    stats = session.run("""
        CALL gds.louvain.write($name, {
            writeProperty:                  'community',
            relationshipWeightProperty:     'weight',
            includeIntermediateCommunities: false
        })
        YIELD communityCount, modularity, computeMillis, nodePropertiesWritten
    """, name=GRAPH_NAME).data()[0]

    print(f"Communities found: {stats['communityCount']}  |  "
          f"Modularity: {round(stats['modularity'], 4)}  |  "
          f"Compute time: {stats['computeMillis']} ms")
    print(f"Properties written: {stats['nodePropertiesWritten']}\n")

    # Top 10 communities by member count, with a sample of member names
    top_communities = session.run("""
        MATCH (a:Author)
        WHERE a.community IS NOT NULL
        WITH a.community AS community, count(a) AS size, collect(a.name)[0..5] AS sample_members
        ORDER BY size DESC
        LIMIT 10
        RETURN community, size, sample_members
    """).data()

    # Distribution: how many communities exist at each size bucket
    distribution = session.run("""
        MATCH (a:Author)
        WHERE a.community IS NOT NULL
        WITH a.community AS community, count(a) AS size
        RETURN size, count(community) AS num_communities
        ORDER BY size DESC
        LIMIT 15
    """).data()

    # Free the in-memory projected graph
    session.run("CALL gds.graph.drop($name)", name=GRAPH_NAME).consume()

driver.close()

print("Top 10 largest author communities:")
df = pd.DataFrame(top_communities)
print(df.to_string(index=False))

print("\nCommunity size distribution:")
df_dist = pd.DataFrame(distribution)
print(df_dist.to_string(index=False))

from neo4j import GraphDatabase

# --- NEO4J CONFIGURATION ---
NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "sdmproject"

def run_query(session, label, query):
    print(f"  ⏳ {label}...")
    result = session.run(query)
    summary = result.consume()
    print(f"  ✅ Done — nodes: {summary.counters.nodes_created}, "
          f"rels: {summary.counters.relationships_created}, "
          f"props: {summary.counters.properties_set}")

def run_updates(uri, user, password):
    print(f"🔗 Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:

        # --- 1. Type_of_institution node + AFFILIATED_WITH relation ---
        # Create the two institution nodes
        print("\n🏢 Creating Type_of_institution nodes...")

        run_query(session, "Type_of_institution nodes", """
            MERGE (i1:Type_of_institution {name: 'University'})
            MERGE (i2:Type_of_institution {name: 'Private'})
        """)

        # Link each author randomly to one institution (50/50)
        # rand() < 0.5 -> University, else -> Private
        run_query(session, "AFFILIATED_WITH (Author -> Type_of_institution)", """
            MATCH (a:Author)
            MATCH (i:Type_of_institution {name: CASE WHEN rand() < 0.5 THEN 'University' ELSE 'Private' END})
            MERGE (a)-[:AFFILIATED_WITH]->(i)
        """)

        # --- 2. is_approved property on REVIEWS edges ---
        # 80% Yes, 20% No
        print("\n📝 Adding is_approved property to REVIEWS edges...")

        run_query(session, "is_approved on REVIEWS", """
            MATCH ()-[r:REVIEWS]->()
            SET r.is_approved = CASE WHEN rand() < 0.8 THEN 'Yes' ELSE 'No' END
        """)

    driver.close()
    print("\n✅ Updates completed! Run these in Neo4j Browser to verify:")
    print("   MATCH (a:Author)-[:AFFILIATED_WITH]->(i:Type_of_institution)")
    print("   RETURN i.name AS institution, count(a) AS total")
    print("")
    print("   MATCH ()-[r:REVIEWS]->()")
    print("   RETURN r.is_approved AS approved, count(r) AS total")

if __name__ == "__main__":
    if NEO4J_PASSWORD == "":
        print("Missing Credentials")
    else:
        run_updates(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
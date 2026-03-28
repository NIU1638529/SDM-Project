#! /usr/bin/env python3
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

def run_neo4j_import(uri, user, password):
    print(f"🔗 Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:

        # --- 1. CONSTRAINTS & INDEXES ---
        print("\n📌 Creating constraints and indexes...")
        constraints = [
            ("Paper unique title",    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper)      REQUIRE p.title IS UNIQUE"),
            ("Author unique name",    "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author)     REQUIRE a.name  IS UNIQUE"),
            ("Journal unique name",   "CREATE CONSTRAINT IF NOT EXISTS FOR (j:Journal)    REQUIRE j.name  IS UNIQUE"),
            ("Volume unique id",      "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Volume)     REQUIRE v.id    IS UNIQUE"),
            ("Conference unique name","CREATE CONSTRAINT IF NOT EXISTS FOR (c:Conference) REQUIRE c.name  IS UNIQUE"),
            ("Edition unique id",     "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Edition)    REQUIRE e.id    IS UNIQUE"),
            ("Topic unique name",     "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Topic)      REQUIRE t.name  IS UNIQUE"),
            ("Paper index on id",     "CREATE INDEX IF NOT EXISTS FOR (p:Paper) ON (p.id)"),
        ]
        for label, q in constraints:
            session.run(q)
            print(f"  ✅ {label}")

        # --- 2. NODES ---
        print("\n📦 Loading nodes...")

        run_query(session, "Paper nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///paper_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (p:Paper {title: row.title})
            SET p.id       = row.paper_id,
                p.pages    = row.pages,
                p.doi      = row.doi,
                p.abstract = row.abstract,
                p.year     = toInteger(row.year)
        """)

        run_query(session, "Author nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///author_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (a:Author {name: row.name})
        """)

        run_query(session, "Journal nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///journal_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (j:Journal {name: row.journal_name})
        """)

        run_query(session, "Volume nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///volume_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (v:Volume {id: row.volume_id})
            SET v.number = row.number
        """)

        run_query(session, "Conference nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///conference_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (c:Conference {name: row.conf_name})
            SET c.type = row.type
        """)

        run_query(session, "Edition nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///edition_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (e:Edition {id: row.edition_id})
            SET e.title  = row.title,
                e.number = row.edition_number,
                e.city   = row.city
        """)

        run_query(session, "Topic nodes", """
            LOAD CSV WITH HEADERS FROM 'file:///topic_node.csv' AS row FIELDTERMINATOR ';'
            MERGE (t:Topic {name: row.topic_name})
        """)

        # --- 3. RELATIONSHIPS ---
        print("\n🔗 Loading relationships...")

        run_query(session, "WRITES (Author -> Paper)", """
            LOAD CSV WITH HEADERS FROM 'file:///writes_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (a:Author {name: row.author_id})
            MATCH (p:Paper  {id:   row.paper_id})
            MERGE (a)-[r:WRITES]->(p)
            SET r.role = row.role
        """)

        # Split PUBLISHED_IN to avoid OR on labels
        run_query(session, "PUBLISHED_IN (Paper -> Volume)", """
            LOAD CSV WITH HEADERS FROM 'file:///published_in_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (p:Paper  {id: row.paper_id})
            MATCH (v:Volume {id: row.container_id})
            MERGE (p)-[:PUBLISHED_IN]->(v)
        """)

        run_query(session, "PUBLISHED_IN (Paper -> Edition)", """
            LOAD CSV WITH HEADERS FROM 'file:///published_in_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (p:Paper   {id: row.paper_id})
            MATCH (e:Edition {id: row.container_id})
            MERGE (p)-[:PUBLISHED_IN]->(e)
        """)

        # Split BELONGS_TO to avoid OR on labels
        run_query(session, "BELONGS_TO (Volume -> Journal)", """
            LOAD CSV WITH HEADERS FROM 'file:///belongs_to_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (v:Volume  {id:   row.child_id})
            MATCH (j:Journal {name: row.parent_id})
            MERGE (v)-[:BELONGS_TO]->(j)
        """)

        run_query(session, "BELONGS_TO (Edition -> Conference)", """
            LOAD CSV WITH HEADERS FROM 'file:///belongs_to_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (e:Edition    {id:   row.child_id})
            MATCH (c:Conference {name: row.parent_id})
            MERGE (e)-[:BELONGS_TO]->(c)
        """)

        run_query(session, "REVIEWS (Author -> Paper)", """
            LOAD CSV WITH HEADERS FROM 'file:///reviews_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (a:Author {name: row.author_name})
            MATCH (p:Paper  {id:   row.paper_id})
            MERGE (a)-[:REVIEWS]->(p)
        """)

        # CITES now includes citation_year as edge property
        run_query(session, "CITES (Paper -> Paper)", """
            LOAD CSV WITH HEADERS FROM 'file:///cites_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (ps:Paper {id: row.paper_id_source})
            MATCH (pt:Paper {id: row.paper_id_target})
            MERGE (ps)-[r:CITES]->(pt)
            SET r.citation_year = toInteger(row.citation_year)
        """)

        run_query(session, "FOCUSED_ON (Paper -> Topic)", """
            LOAD CSV WITH HEADERS FROM 'file:///focused_on_relation.csv' AS row FIELDTERMINATOR ';'
            MATCH (p:Paper {id:   row.paper_id})
            MATCH (t:Topic {name: row.topic_name})
            MERGE (p)-[:FOCUSED_ON]->(t)
        """)

    driver.close()

    print("\n✅ Import completed! Run these in Neo4j Browser to verify:")
    print("   MATCH (n) RETURN labels(n)[0] AS label, count(n) AS total ORDER BY total DESC")
    print("   MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS total ORDER BY total DESC")

if __name__ == "__main__":
    run_neo4j_import(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
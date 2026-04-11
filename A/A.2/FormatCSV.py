#! /usr/bin/env python3
import csv
import os
import random

# --- CONFIGURACIÓN ---
LIMIT = 5000 
GLOBAL_AUTHORS = set()
TITLES_SEEN = set()
ALL_PAPER_IDS = []
INPUT_PATH  = r"dblp_data"
OUTPUT_PATH = r"nodes_and_relations"

# --- POOL DE CIUDADES ---
CITIES = [
    "Amsterdam", "Athens", "Austin", "Barcelona", "Beijing", "Berlin", "Boston", "Brussels", 
    "Budapest", "Chicago", "Copenhagen", "Dublin", "Edinburgh", "Florence", "Geneva",
    "Helsinki", "Hong Kong", "Istanbul", "Kyoto", "Lisbon", "London", "Los Angeles", 
    "Madrid", "Melbourne", "Milan", "Montreal", "Moscow", "Munich", "New York", "Oslo",
    "Paris", "Prague", "Rome", "San Francisco", "Seattle", "Seoul", "Shanghai", "Singapore", 
    "Stockholm", "Sydney", "Tokyo", "Toronto", "Vancouver", "Vienna", "Warsaw",
    "Washington D.C.", "Zurich"
]

# --- MAPA paper_id -> año de publicación (para calcular citas válidas por IF) ---
PAPER_YEAR = {}      # paper_id -> int(year)
# --- MAPA paper_id -> journal (para agrupar por journal al generar citas) ---
PAPER_JOURNAL = {}   # paper_id -> journal_name

# --- Contador de citas recibidas por cada paper ---
CITATION_COUNT = {}  # paper_id -> int

# --- TOPIC DETECTION: keyword -> topic mapping ---
# Keywords are matched case-insensitively against paper titles.
# A paper is only included in topic CSVs if at least one keyword matches.
TOPIC_KEYWORDS = {
    # We create the 7 topics of the recommender C and also other topics to make it realistic
    "data management": [
        "data management", "database management", "data administration", 
        "data curation", "transaction", "relational", "NoSQL", "data integration",
        "data governance", "master data", "metadata"
    ],
    "indexing": [
        "indexing", "index structures", "b-tree", "hash index", "inverted index", 
        "bitmap index", "search structures", "query optimization", "multidimensional index"
    ],
    "data modeling": [
        "data modeling", "data model", "entity-relationship", "ER model", "schema", 
        "ontology", "conceptual modeling", "knowledge representation", "unified modeling language"
    ],
    "big data": [
        "big data", "large-scale data", "massive datasets", "hadoop", "spark", 
        "mapreduce", "data warehouse", "OLAP", "cloud computing", "data lake"
    ],
    "data processing": [
        "data processing", "query processing", "transaction processing", 
        "stream processing", "log", "parallel computing", "throughput", "batch processing"
    ],
    "data storage": [
        "data storage", "storage", "persistent memory", "NVRAM", "flash", 
        "distributed storage", "file systems", "memory hierarchy", "solid state drive"
    ],
    "data querying": [
        "data querying", "query", "SQL", "SPARQL", "graph database", "query language", 
        "complex queries", "query optimization", "XQuery", "cypher"
    ],

    # We add other categories
    "Artificial Intelligence": [
        "artificial intelligence", "AI", "intelligent system", "knowledge representation",
        "expert system", "cognitive", "reasoning", "autonomous agent", "machine mentality",
        "singularity", "oracle AI", "brain emulation", "embodiment", "intentionality",
        "turing", "computation and cognition", "philosophy of AI", "AI safety"
    ],
    "Machine Learning": [
        "machine learning", "deep learning", "neural network", "convolutional", "recurrent",
        "transformer", "attention mechanism", "generative model", "GAN", "autoencoder",
        "reinforcement learning", "supervised", "unsupervised", "classification", "regression",
        "random forest", "gradient boosting", "XGBoost", "embedding", "fine-tuning",
        "transfer learning", "federated learning", "spiking neural", "hyperdimensional"
    ],
    "Computer Vision": [
        "computer vision", "image recognition", "object detection", "image segmentation",
        "visual", "convolutional neural", "optical flow", "image processing", "face recognition",
        "depth estimation", "3D reconstruction", "scene understanding", "tracking",
        "bounding box", "YOLO", "ResNet", "vision transformer", "hotspot detection",
        "foreground", "fracture surface", "mouth shape", "blink detection"
    ],
    "Natural Language Processing": [
        "natural language", "NLP", "text mining", "sentiment analysis", "named entity",
        "language model", "word embedding", "BERT", "GPT", "question answering",
        "machine translation", "summarization", "information extraction", "parsing",
        "speech recognition", "dialogue", "chatbot", "large language model", "LLM",
        "assertion failure", "RTL design"
    ],
    "Data Mining": [
        "data mining", "pattern recognition", "association rule", "clustering", "anomaly detection",
        "outlier", "frequent itemset", "sequential pattern", "bayesian", "decision tree",
        "feature selection", "dimensionality reduction", "knowledge discovery", "OLAP",
        "folksonomy", "ontology", "semantic enrichment", "interestingness measure",
        "proximity measure", "topological", "cognitive map", "user centered"
    ],
    "Computer Architecture": [
        "processor", "microprocessor", "VLSI", "FPGA", "circuit", "chip", "hardware",
        "synthesis", "placement", "routing", "timing", "clock", "power dissipation",
        "logic design", "gate", "flip-flop", "register", "pipeline", "cache",
        "memory hierarchy", "NoC", "network-on-chip", "SoC", "ASIC", "EDA",
        "layout", "floorplan", "design automation", "DAC", "verification", "simulation",
        "fault", "test generation", "testability", "scan", "ATPG", "fault-tolerant",
        "parasitic", "extraction", "interconnect", "wire", "buffer", "clock tree"
    ],
    "Distributed Systems": [
        "distributed", "cloud computing", "parallel", "concurrency", "fault tolerance",
        "consensus", "replication", "MapReduce", "Hadoop", "Spark", "microservice",
        "container", "kubernetes", "serverless", "load balancing", "message passing",
        "multiprocessor", "multi-core", "GPU", "GPGPU", "accelerator", "HLS",
        "high-level synthesis", "CGRA", "dataflow", "streaming"
    ],
    "Networks and Communications": [
        "network", "protocol", "routing", "wireless", "IoT", "internet of things",
        "bandwidth", "latency", "packet", "TCP", "UDP", "5G", "antenna", "signal",
        "communication", "vehicular", "edge computing", "fog computing", "intrusion detection",
        "network intrusion", "interconnect", "NoC", "network-on-chip"
    ],
    "Security and Privacy": [
        "security", "privacy", "encryption", "cryptography", "authentication", "firewall",
        "malware", "vulnerability", "attack", "defense", "cyber", "hardware trojan",
        "watermark", "unclonable", "PUF", "side channel", "fault injection",
        "IP protection", "intellectual property", "obfuscation", "safety engineering"
    ],
    "Software Engineering": [
        "software engineering", "software development", "agile", "testing", "debugging",
        "code review", "refactoring", "static analysis", "dynamic analysis", "bug",
        "fault-prone", "test case", "verification", "formal methods", "model checking",
        "specification", "UML", "design pattern", "mobile application", "API",
        "compiler", "programming language", "code generation", "VHDL", "SystemC",
        "instruction set", "simulator", "virtual prototype", "post-silicon"
    ],
    "Robotics": [
        "robot", "robotic", "autonomous vehicle", "drone", "UAV", "motion planning",
        "kinematics", "sensor fusion", "SLAM", "navigation", "manipulation",
        "human-robot", "control system", "actuator", "embedded system", "real-time",
        "neuromorphic", "cognitive unit", "sensorimotor"
    ],
    "Bioinformatics": [
        "bioinformatics", "genomics", "proteomics", "DNA", "gene", "protein",
        "sequence alignment", "phylogenetic", "molecular", "biological network",
        "drug discovery", "medical", "health", "clinical", "diagnosis",
        "neural processing", "brain", "neuroscience", "cognitive science"
    ],
    "Quantum Computing": [
        "quantum", "qubit", "quantum circuit", "quantum gate", "superposition",
        "entanglement", "quantum error", "quantum algorithm", "quantum linguistics",
        "quantum cryptography", "cryo-CMOS", "quantum computing"
    ],
    "High Performance Computing": [
        "high performance", "HPC", "supercomputer", "GPU acceleration", "CUDA",
        "OpenMP", "MPI", "performance optimization", "throughput", "latency",
        "benchmark", "profiling", "power efficiency", "energy efficiency",
        "scalable", "parallel computing", "matrix multiplication", "GEMM",
        "sparse matrix", "graph processing", "breadth-first search"
    ],
}

# Build a flat list of (keyword_lower, topic) for fast lookup
_KEYWORD_TOPIC_LIST = []
for _topic, _kws in TOPIC_KEYWORDS.items():
    for _kw in _kws:
        _KEYWORD_TOPIC_LIST.append((_kw.lower(), _topic))

def detect_topics(title: str) -> list[str]:
    """Return list of unique topics detected in title (can be multiple)."""
    title_lower = title.lower()
    found = set()
    for kw, topic in _KEYWORD_TOPIC_LIST:
        if kw in title_lower:
            found.add(topic)
    return sorted(found)

# paper_id -> list of detected topics (only papers with at least one topic)
PAPER_TOPICS = {}   # paper_id -> [topic1, topic2, ...]


def get_headers(header_file):
    if not os.path.exists(header_file):
        print(f"⚠️ Error: No se encuentra el archivo de cabecera {header_file}")
        return []
    with open(header_file, 'r', encoding='utf-8-sig') as f:
        line = f.readline().strip()
        if not line: return []
        parts = line.split(';')
        headers = [col.split(':')[0] for col in parts if col]
        return headers

# --- 1. ARTICLES (Paper -> Volume -> Journal) ---
def process_articles(data_file, header_file):
    print(f"Processing articles.csv.")
    headers = get_headers(header_file)
    journals_seen = set()
    volumes_seen = set()

    with open(data_file, 'r', encoding='utf-8') as f_in, \
         open(f'{OUTPUT_PATH}/paper_node.csv', 'w', encoding='utf-8', newline='') as f_p, \
         open(f'{OUTPUT_PATH}/author_node.csv', 'w', encoding='utf-8', newline='') as f_a, \
         open(f'{OUTPUT_PATH}/writes_relation.csv', 'w', encoding='utf-8', newline='') as f_w, \
         open(f'{OUTPUT_PATH}/volume_node.csv', 'w', encoding='utf-8', newline='') as f_v, \
         open(f'{OUTPUT_PATH}/journal_node.csv', 'w', encoding='utf-8', newline='') as f_j, \
         open(f'{OUTPUT_PATH}/published_in_relation.csv', 'w', encoding='utf-8', newline='') as f_pub, \
         open(f'{OUTPUT_PATH}/belongs_to_relation.csv', 'w', encoding='utf-8', newline='') as f_bel:
        
        reader = csv.DictReader(f_in, delimiter=';', fieldnames=headers)
        w_paper  = csv.writer(f_p,   delimiter=';')
        w_author = csv.writer(f_a,   delimiter=';')
        w_writes = csv.writer(f_w,   delimiter=';')
        w_volume = csv.writer(f_v,   delimiter=';')
        w_journal= csv.writer(f_j,   delimiter=';')
        w_pub    = csv.writer(f_pub, delimiter=';')
        w_bel    = csv.writer(f_bel, delimiter=';')

        w_paper.writerow(['paper_id', 'title', 'pages', 'doi', 'abstract', 'year'])
        w_author.writerow(['name'])
        w_writes.writerow(['author_id', 'paper_id', 'role'])
        w_volume.writerow(['volume_id', 'number'])
        w_journal.writerow(['journal_name'])
        w_pub.writerow(['paper_id', 'container_id'])
        w_bel.writerow(['child_id', 'parent_id'])

        count = 0
        for row in reader:
            p_id   = row.get('id', '').strip()
            p_title= row.get('title', '').strip()
            j_name = row.get('journal', '').strip()

            if not p_id or not p_title or p_title in TITLES_SEEN or not j_name \
               or j_name.lower() == "unknown journal" or not row.get('author', '').strip():
                continue
            if count >= LIMIT: break

            # Detect topics first — skip paper if no topic found
            topics = detect_topics(p_title)
            if not topics:
                continue

            TITLES_SEEN.add(p_title)
            ALL_PAPER_IDS.append(p_id)
            PAPER_TOPICS[p_id] = topics

            abstract = f"This journal article explores {p_title} in depth."
            doi    = row.get('ee', '').split('|')[0] if row.get('ee') else ''
            p_year = row.get('year', '').strip()

            w_paper.writerow([p_id, p_title, row.get('pages', ''), doi, abstract, p_year])

            # Track year and journal for citation strategy
            try:
                PAPER_YEAR[p_id]    = int(p_year)
                PAPER_JOURNAL[p_id] = j_name
            except ValueError:
                pass

            v_num = row.get('volume', 'N/A').strip()
            v_id  = f"vol_{j_name}_{v_num}".replace(" ", "_")

            if j_name not in journals_seen:
                w_journal.writerow([j_name]); journals_seen.add(j_name)
            if v_id not in volumes_seen:
                w_volume.writerow([v_id, v_num]); volumes_seen.add(v_id)
                w_bel.writerow([v_id, j_name])

            w_pub.writerow([p_id, v_id])

            authors = [a.strip() for a in row.get('author', '').split('|') if a.strip()]
            for i, name in enumerate(authors):
                if name not in GLOBAL_AUTHORS:
                    w_author.writerow([name]); GLOBAL_AUTHORS.add(name)
                w_writes.writerow([name, p_id, 'Main Author' if i == 0 else 'Co-author'])

            count += 1

# --- 2. INPROCEEDINGS ---
def process_inproceedings(data_file, header_file):
    print(f"Processing Inproceedings.csv.")
    headers = get_headers(header_file)

    with open(data_file, 'r', encoding='utf-8') as f_in, \
         open(f'{OUTPUT_PATH}/paper_node.csv', 'a', encoding='utf-8', newline='') as f_p, \
         open(f'{OUTPUT_PATH}/author_node.csv', 'a', encoding='utf-8', newline='') as f_a, \
         open(f'{OUTPUT_PATH}/writes_relation.csv', 'a', encoding='utf-8', newline='') as f_w, \
         open(f'{OUTPUT_PATH}/published_in_relation.csv', 'a', encoding='utf-8', newline='') as f_pub:
        
        reader   = csv.DictReader(f_in, delimiter=';', fieldnames=headers)
        w_paper  = csv.writer(f_p,   delimiter=';')
        w_author = csv.writer(f_a,   delimiter=';')
        w_writes = csv.writer(f_w,   delimiter=';')
        w_pub    = csv.writer(f_pub, delimiter=';')

        count = 0
        for row in reader:
            p_id   = row.get('id', '').strip()
            p_title= row.get('title', '').strip()

            if not p_id or not p_title or p_title in TITLES_SEEN or not row.get('author', '').strip():
                continue
            if count >= LIMIT: break

            # Detect topics first — skip paper if no topic found
            topics = detect_topics(p_title)
            if not topics:
                continue

            TITLES_SEEN.add(p_title)
            ALL_PAPER_IDS.append(p_id)
            PAPER_TOPICS[p_id] = topics

            abstract = f"Conference paper discussing: {p_title}."
            doi    = row.get('ee', '').split('|')[0] if row.get('ee') else ''
            p_year = row.get('year', '').strip()

            w_paper.writerow([p_id, p_title, row.get('pages', ''), doi, abstract, p_year])

            try:
                PAPER_YEAR[p_id] = int(p_year)
            except ValueError:
                pass

            edit_id = row.get('crossref', '').strip()
            if edit_id:
                w_pub.writerow([p_id, edit_id])

            authors = [a.strip() for a in row.get('author', '').split('|') if a.strip()]
            for i, name in enumerate(authors):
                if name not in GLOBAL_AUTHORS:
                    w_author.writerow([name]); GLOBAL_AUTHORS.add(name)
                w_writes.writerow([name, p_id, 'Main Author' if i == 0 else 'Co-author'])

            count += 1

# --- 3. PROCEEDINGS ---
def process_proceedings(data_file, header_file):
    print(f"Processing Proceedings.csv.")
    headers = get_headers(header_file)
    confs_seen = set()

    with open(data_file, 'r', encoding='utf-8') as f_in, \
         open(f'{OUTPUT_PATH}/conference_node.csv', 'w', encoding='utf-8', newline='') as f_c, \
         open(f'{OUTPUT_PATH}/edition_node.csv', 'w', encoding='utf-8', newline='') as f_e, \
         open(f'{OUTPUT_PATH}/belongs_to_relation.csv', 'a', encoding='utf-8', newline='') as f_bel:
        
        reader = csv.DictReader(f_in, delimiter=';', fieldnames=headers)
        w_conf = csv.writer(f_c,   delimiter=';')
        w_edit = csv.writer(f_e,   delimiter=';')
        w_bel  = csv.writer(f_bel, delimiter=';')

        w_conf.writerow(['conf_name', 'type'])
        w_edit.writerow(['edition_id', 'title', 'edition_number', 'city'])

        count = 0
        for row in reader:
            conf_name = row.get('booktitle', '').strip()
            edit_id   = row.get('key', '').strip()

            if not conf_name or not edit_id or conf_name.lower() == "unknown":
                continue
            if count >= LIMIT: break

            if conf_name not in confs_seen:
                conf_type = 'Conference' if random.random() < 0.7 else 'Workshop'
                w_conf.writerow([conf_name, conf_type])
                confs_seen.add(conf_name)

            w_edit.writerow([edit_id, row.get('title', ''), row.get('volume', 'N/A'), random.choice(CITIES)])
            w_bel.writerow([edit_id, conf_name])

            count += 1

# --- 4. EXTRA DATA ---
def generate_extra_data():
    print(f"Generating Reviewers and Strategic Citations for Impact Factor.")
    authors_list = sorted(list(GLOBAL_AUTHORS))

    # Build paper_authors map from writes_relation.csv
    paper_authors = {}
    if os.path.exists(f'{OUTPUT_PATH}/writes_relation.csv'):
        with open(f'{OUTPUT_PATH}/writes_relation.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                p_id = row['paper_id']
                if p_id not in paper_authors:
                    paper_authors[p_id] = set()
                paper_authors[p_id].add(row['author_id'])

    # --- REVIEWERS ---
    with open(f'{OUTPUT_PATH}/reviews_relation.csv', 'w', encoding='utf-8', newline='') as f_rev:
        w_rev = csv.writer(f_rev, delimiter=';')
        w_rev.writerow(['author_name', 'paper_id'])

        for p_id in ALL_PAPER_IDS:
            current_authors = paper_authors.get(p_id, set())
            possible_reviewers = [a for a in authors_list if a not in current_authors]
            if len(possible_reviewers) >= 3:
                for r in random.sample(possible_reviewers, 3):
                    w_rev.writerow([r, p_id])

    # --- STRATEGIC CITATIONS FOR IMPACT FACTOR ---
    #
    # Strategy: assign each journal a tier that determines how many
    # within-window (year+1, year+2) citations its papers receive.
    #
    #   Tier A (top,    ~15% of journals) → 15–40 citations per paper
    #   Tier B (mid,    ~35% of journals) →  5–14 citations per paper
    #   Tier C (low,    ~50% of journals) →  0–4  citations per paper
    #
    # Citations from conference papers are purely random (no IF calculation).
    # The citation year is drawn uniformly from [pub_year+1, pub_year+2]
    # so every citation falls inside the classic 2-year IF window.

    # Collect unique journals and assign tiers
    unique_journals = list(set(PAPER_JOURNAL.values()))
    random.shuffle(unique_journals)
    n = len(unique_journals)
    cutA = max(1, int(n * 0.15))
    cutB = max(cutA + 1, int(n * 0.50))

    journal_tier = {}
    for i, j in enumerate(unique_journals):
        if i < cutA:
            journal_tier[j] = 'A'
        elif i < cutB:
            journal_tier[j] = 'B'
        else:
            journal_tier[j] = 'C'

    print(f"Journal tiers — A: {cutA}, B: {cutB - cutA}, C: {n - cutB}")

    # Papers that have a valid year and belong to a journal (for citing others)
    citable_papers = [p for p in ALL_PAPER_IDS if p in PAPER_YEAR]

    with open(f'{OUTPUT_PATH}/cites_relation.csv', 'w', encoding='utf-8', newline='') as f_cite:
        w_cite = csv.writer(f_cite, delimiter=';')
        w_cite.writerow(['paper_id_source', 'paper_id_target'])

        for p_id in ALL_PAPER_IDS:
            pub_year = PAPER_YEAR.get(p_id)
            j_name   = PAPER_JOURNAL.get(p_id)

            if pub_year and j_name:
                # Journal paper: number of citations depends on tier
                tier = journal_tier.get(j_name, 'C')
                if tier == 'A':
                    num_cites = random.randint(15, 40)
                elif tier == 'B':
                    num_cites = random.randint(5, 14)
                else:
                    num_cites = random.randint(0, 4)
            else:
                # Conference paper: small random citations, no IF strategy needed
                num_cites = random.randint(0, 5)

            if num_cites == 0 or len(citable_papers) <= 1:
                continue

            # Choose papers that cite this one (source papers)
            sources = random.sample(
                [p for p in citable_papers if p != p_id],
                min(num_cites, len(citable_papers) - 1)
            )

            for src in sources:
                w_cite.writerow([src, p_id])
                # Accumulate citation count for the target paper
                CITATION_COUNT[p_id] = CITATION_COUNT.get(p_id, 0) + 1

def update_paper_citation_count():
    """
    Re-writes paper_node.csv adding citation_count column,
    and writes paper_citation_count.csv as a standalone update file.
    """
    print(f"Updating paper_node.csv with citation_count...")

    # Read existing paper_node.csv
    paper_rows = []
    with open(f'{OUTPUT_PATH}/paper_node.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            paper_rows.append(row)

    # Rewrite with citation_count column
    with open(f'{OUTPUT_PATH}/paper_node.csv', 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f, delimiter=';')
        w.writerow(['paper_id', 'title', 'pages', 'doi', 'abstract', 'year', 'citation_count'])
        for row in paper_rows:
            w.writerow([
                row['paper_id'], row['title'], row['pages'],
                row['doi'], row['abstract'], row['year'],
                CITATION_COUNT.get(row['paper_id'], 0)
            ])

    print(f" Citation_count added to {len(paper_rows)} papers")

def generate_topic_data():
    """
    Write topic_node.csv and focused_on_relation.csv.
    Only papers with at least one detected topic are included.
    """
    print(f"Generating Keyword nodes and FOCUSED_ON relations.")

    # Collect unique topics
    all_topics = set()
    for topics in PAPER_TOPICS.values():
        all_topics.update(topics)

    with open(f'{OUTPUT_PATH}/topic_node.csv', 'w', encoding='utf-8', newline='') as f_t,          open(f'{OUTPUT_PATH}/focused_on_relation.csv', 'w', encoding='utf-8', newline='') as f_fo:

        w_t  = csv.writer(f_t,  delimiter=';')
        w_fo = csv.writer(f_fo, delimiter=';')

        w_t.writerow(['topic_name'])
        w_fo.writerow(['paper_id', 'topic_name'])

        for topic in sorted(all_topics):
            w_t.writerow([topic])

        for p_id, topics in PAPER_TOPICS.items():
            for topic in topics:
                w_fo.writerow([p_id, topic])

    papers_with_topics = len(PAPER_TOPICS)
    total_relations    = sum(len(t) for t in PAPER_TOPICS.values())
    print(f"  {len(all_topics)} topics, {papers_with_topics} papers matched, {total_relations} FOCUSED_ON relations")

if __name__ == "__main__":
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    random.seed(42)
    process_articles(
        os.path.join(INPUT_PATH, 'output_article.csv'),
        os.path.join(INPUT_PATH, 'output_article_header.csv')
    )
    process_inproceedings(
        os.path.join(INPUT_PATH, 'output_inproceedings.csv'),
        os.path.join(INPUT_PATH, 'output_inproceedings_header.csv')
    )
    process_proceedings(
        os.path.join(INPUT_PATH, 'output_proceedings.csv'),
        os.path.join(INPUT_PATH, 'output_proceedings_header.csv')
    )
    generate_extra_data()
    update_paper_citation_count()
    generate_topic_data()
    print(f"\n Process completed. Results completely consistent.")
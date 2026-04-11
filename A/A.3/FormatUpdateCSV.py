#!/usr/bin/env python3
"""
FormatUpdateCSV.py — Generate CSV files for the A.3 schema evolution.

Reads existing nodes from A.2/nodes_and_relations/ and produces:
  update_data/institution_node.csv       — Type_of_institution nodes (University / Private)
  update_data/affiliated_with_relation.csv — Author -> Type_of_institution (50/50 random)
  update_data/reviews_approved_relation.csv — REVIEWS edges with is_approved (80% Yes / 20% No)
"""
import csv
import os
import random

RANDOM_SEED  = 42
INPUT_PATH   = os.path.join("..", "A.2", "nodes_and_relations")
OUTPUT_PATH  = "update_data"

def read_csv(filename):
    path = os.path.join(INPUT_PATH, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


def generate_institution_nodes():
    print("Generating institution_node.csv...")
    with open(f"{OUTPUT_PATH}/institution_node.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["institution_name"])
        w.writerow(["University"])
        w.writerow(["Private"])
    print("  2 institution nodes written.")


def generate_affiliated_with(authors):
    print("Generating affiliated_with_relation.csv (50/50 split)...")
    with open(f"{OUTPUT_PATH}/affiliated_with_relation.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["author_name", "institution_name"])
        for row in authors:
            institution = "University" if random.random() < 0.5 else "Private"
            w.writerow([row["name"], institution])
    print(f"  {len(authors)} AFFILIATED_WITH rows written.")


def generate_reviews_approved(reviews):
    print("Generating reviews_approved_relation.csv (80% Yes / 20% No)...")
    with open(f"{OUTPUT_PATH}/reviews_approved_relation.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["author_name", "paper_id", "is_approved"])
        for row in reviews:
            is_approved = "Yes" if random.random() < 0.8 else "No"
            w.writerow([row["author_name"], row["paper_id"], is_approved])
    print(f"  {len(reviews)} REVIEWS rows written.")


if __name__ == "__main__":
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    random.seed(RANDOM_SEED)

    authors = read_csv("author_node.csv")
    reviews = read_csv("reviews_relation.csv")

    generate_institution_nodes()
    generate_affiliated_with(authors)
    generate_reviews_approved(reviews)

    print(f"\nDone. CSVs written to {OUTPUT_PATH}/")

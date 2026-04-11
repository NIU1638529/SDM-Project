#!/usr/bin/env python3
"""
FormatUpdateCSV.py — Generate CSV files for the A.3 schema evolution.

Reads existing nodes from A.2/nodes_and_relations/ and produces:
  update_data/institution_node.csv        — Type_of_institution nodes (University / Private)
  update_data/affiliated_with_relation.csv — Author -> Type_of_institution (50/50 random)
  update_data/reviews_approved_relation.csv — REVIEWS edges with is_approved, including:
      · Base reviewers (3 per paper, from A.2)          → is_approved: 80% Yes / 20% No
      · Extra reviewers for 20% of workshops (min 1)    → +2 per paper, same probability
      · Extra reviewers for 20% of journals  (min 1)    → +2 per paper, same probability
      Reviewers cannot coincide with any author of the paper they review,
      nor with reviewers already assigned to that paper.
"""
import csv
import os
import random

RANDOM_SEED          = 42
REVIEWS_APPROVAL_PROB = 0.8   # P(is_approved = "Yes") — 80% Yes, 20% No
INPUT_PATH           = os.path.join("..", "A.2", "nodes_and_relations")
OUTPUT_PATH          = "update_data"


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


def generate_reviews_approved(reviews, paper_authors, paper_reviewers, authors_list):
    """
    Writes reviews_approved_relation.csv with three groups of rows:

    1. Base reviewers (already in reviews_relation.csv from A.2): is_approved assigned.
    2. Extra reviewers for a random 20% of workshops (min 1): +2 per paper.
    3. Extra reviewers for a random 20% of journals  (min 1): +2 per paper.

    In all cases: reviewer cannot be an author or an existing reviewer of that paper.
    is_approved probability: {prob}% Yes / {rest}% No.
    """.format(prob=int(REVIEWS_APPROVAL_PROB * 100), rest=int((1 - REVIEWS_APPROVAL_PROB) * 100))

    print(f"Generating reviews_approved_relation.csv "
          f"(is_approved: {int(REVIEWS_APPROVAL_PROB*100)}% Yes / "
          f"{int((1-REVIEWS_APPROVAL_PROB)*100)}% No)...")

    with open(f"{OUTPUT_PATH}/reviews_approved_relation.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["author_name", "paper_id", "is_approved"])

        # --- 1. Base reviewers from A.2 ---
        for row in reviews:
            is_approved = "Yes" if random.random() < REVIEWS_APPROVAL_PROB else "No"
            w.writerow([row["author_name"], row["paper_id"], is_approved])
        print(f"  Base reviewers written: {len(reviews)}")

        # --- 2. Extra reviewers for workshops and journals ---
        extra_rows = _generate_extra_reviewer_rows(
            paper_authors, paper_reviewers, authors_list
        )
        for row in extra_rows:
            w.writerow(row)

    print(f"  Extra reviewer assignments added: {len(extra_rows)}")
    print(f"  Total REVIEWS rows: {len(reviews) + len(extra_rows)}")


def _generate_extra_reviewer_rows(paper_authors, paper_reviewers, authors_list):
    """
    Selects 20% of workshops (min 1) and 20% of journals (min 1).
    Returns list of [author_name, paper_id, is_approved] rows for the extra assignments.
    """
    conferences  = read_csv("conference_node.csv")
    journals     = read_csv("journal_node.csv")
    belongs_to   = read_csv("belongs_to_relation.csv")
    published_in = read_csv("published_in_relation.csv")

    # Build lookup sets to tell apart Edition->Conference from Volume->Journal
    conf_names    = {row["conf_name"]    for row in conferences}
    journal_names = {row["journal_name"] for row in journals}

    edition_to_conf   = {}
    volume_to_journal = {}
    for row in belongs_to:
        if row["parent_id"] in conf_names:
            edition_to_conf[row["child_id"]] = row["parent_id"]
        elif row["parent_id"] in journal_names:
            volume_to_journal[row["child_id"]] = row["parent_id"]

    # Build conference -> papers and journal -> papers maps
    conf_papers    = {}
    journal_papers = {}
    for row in published_in:
        container = row["container_id"]
        paper     = row["paper_id"]
        if container in edition_to_conf:
            conf_papers.setdefault(edition_to_conf[container], set()).add(paper)
        elif container in volume_to_journal:
            journal_papers.setdefault(volume_to_journal[container], set()).add(paper)

    # Select 20% of workshops (min 1)
    workshops   = [r["conf_name"] for r in conferences if r["type"] == "Workshop"]
    n_workshops = max(1, int(0.2 * len(workshops)))
    selected_workshops = random.sample(workshops, min(n_workshops, len(workshops)))

    # Select 20% of journals (min 1)
    all_journals = [r["journal_name"] for r in journals]
    n_journals   = max(1, int(0.2 * len(all_journals)))
    selected_journals = random.sample(all_journals, min(n_journals, len(all_journals)))

    print(f"  Workshops selected: {len(selected_workshops)} / {len(workshops)} "
          f"(~20%) → +2 extra reviewers per paper")
    print(f"  Journals selected:  {len(selected_journals)} / {len(all_journals)} "
          f"(~20%) → +2 extra reviewers per paper")

    extra_rows = []

    def assign_extra(paper_id, n_extra):
        """Pick n_extra reviewers for paper_id, excluding authors and existing reviewers."""
        excluded = (paper_authors.get(paper_id, set())
                    | paper_reviewers.get(paper_id, set()))
        pool = [a for a in authors_list if a not in excluded]
        for reviewer in random.sample(pool, min(n_extra, len(pool))):
            is_approved = "Yes" if random.random() < REVIEWS_APPROVAL_PROB else "No"
            extra_rows.append([reviewer, paper_id, is_approved])
            # Track to avoid duplicates if the same paper appears in both selections
            paper_reviewers.setdefault(paper_id, set()).add(reviewer)

    for conf in selected_workshops:
        for p_id in conf_papers.get(conf, set()):
            assign_extra(p_id, 2)

    for journal in selected_journals:
        for p_id in journal_papers.get(journal, set()):
            assign_extra(p_id, 2)

    return extra_rows


if __name__ == "__main__":
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    random.seed(RANDOM_SEED)

    authors = read_csv("author_node.csv")
    reviews = read_csv("reviews_relation.csv")
    writes  = read_csv("writes_relation.csv")

    # paper_id -> set of author names (to exclude from reviewer pool)
    paper_authors = {}
    for row in writes:
        paper_authors.setdefault(row["paper_id"], set()).add(row["author_id"])

    # paper_id -> set of already-assigned reviewer names (to avoid duplicates)
    paper_reviewers = {}
    for row in reviews:
        paper_reviewers.setdefault(row["paper_id"], set()).add(row["author_name"])

    authors_list = sorted(row["name"] for row in authors)

    generate_institution_nodes()
    generate_affiliated_with(authors)
    generate_reviews_approved(reviews, paper_authors, paper_reviewers, authors_list)

    print(f"\nDone. CSVs written to {OUTPUT_PATH}/")

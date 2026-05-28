"""
Ingest Synexis vertical pitch deck metadata as Tier 2 (shareable) chunks into Pinecone.

Each vertical deck gets one summary vector. The text is rich enough to score well on
"overview", "intro", or "pitch" queries for that vertical. The share_url in the metadata
flows through _format_context() → [SHAREABLE DOCUMENT LINK] annotation → agent cites it
as a normal [N] source, which shows up in the Sources accordion with a View link.

This is the shareable counterpart to the per-slide Tier 2 chunks already in Pinecone
from the Marketing Approved Collateral SharePoint sync. Those slide chunks are too
granular to win against one-pagers on broad overview queries — this summary vector
ensures the deck surfaces reliably for vertical overview requests.

Usage (from repo root):
    python3 -m pipeline.ingest_vertical_decks            # dry run
    python3 -m pipeline.ingest_vertical_decks --confirm  # upsert to Pinecone
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import voyageai
from dotenv import load_dotenv
from pinecone import Pinecone

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Vertical deck catalog — (slug, title, share_url, description)
# ---------------------------------------------------------------------------
VERTICAL_DECKS = [
    (
        "animal-health",
        "Intro to Synexis - Animal Health",
        "Marketing Approved Collateral/Intro to Synexis _Animal Health_0526.pptx",
        "https://243636346.fs1.hubspotusercontent-na2.net/hubfs/243636346/Rep%20Agent%20Content/Intro%20to%20Synexis%20_Animal%20Health_0526.pptx",
        (
            "Overview and introduction to Synexis DHP® technology for the Animal Health vertical. "
            "Covers the value proposition for veterinary clinics, companion animal hospitals, "
            "equine facilities, and livestock settings. Topics include: what Synexis technology "
            "does, how DHP® provides continuous touchless pathogen control in occupied animal "
            "care spaces, key deployment environments (exam rooms, surgical suites, recovery "
            "areas, kennels, stables, critical care units), efficacy data relevant to animal "
            "health, device lineup and placement guidance, and rep talking points for Animal "
            "Health prospect conversations. Use this deck as the primary shareable resource "
            "for Animal Health vertical introductions and overview conversations."
        ),
    ),
    (
        "healthcare",
        "Intro to Synexis - Healthcare",
        "Marketing Approved Collateral/Intro to Synexis _Healthcare_0526.pptx",
        "https://243636346.fs1.hubspotusercontent-na2.net/hubfs/243636346/Rep%20Agent%20Content/Intro%20to%20Synexis%20_Healthcare_0526.pptx",
        (
            "Overview and introduction to Synexis DHP® technology for the Healthcare vertical. "
            "Covers the value proposition for hospitals, long-term care facilities, outpatient "
            "clinics, surgical suites, ICUs, NICUs, and other clinical environments. Topics "
            "include: what Synexis technology does, how DHP® provides continuous touchless "
            "pathogen control in occupied clinical spaces, HAI reduction framing, key deployment "
            "areas (patient rooms, nursing stations, EVS areas, pharmacies, waiting rooms), "
            "efficacy data relevant to healthcare pathogens, device lineup, stakeholder value "
            "drivers (EVS, infection control, administration), and rep talking points for "
            "Healthcare prospect conversations. Use this deck as the primary shareable resource "
            "for Healthcare vertical introductions and overview conversations."
        ),
    ),
    (
        "poultry",
        "Intro to Synexis - Poultry",
        "Marketing Approved Collateral/Intro to Synexis _Poultry_0526.pptx",
        "https://243636346.fs1.hubspotusercontent-na2.net/hubfs/243636346/Rep%20Agent%20Content/Intro%20to%20Synexis%20_Poultry_0526.pptx",
        (
            "Overview and introduction to Synexis DHP® technology for the Poultry vertical. "
            "Relevant for Poultry and Food Safety conversations — poultry production is a core "
            "food safety context where pathogen control directly impacts product safety and "
            "compliance. Covers the value proposition for hatcheries, broiler houses, egg "
            "coolers, egg collection and packing areas, breeder houses, feed storage, and "
            "employee/common spaces. Topics include: what Synexis technology does, how DHP® "
            "provides continuous touchless pathogen control in occupied poultry production "
            "environments, food safety efficacy data for Salmonella, Campylobacter, and other "
            "poultry and food safety pathogens, flock health and biosecurity framing, food "
            "safety compliance support, device lineup and placement guidance, and rep talking "
            "points for Poultry and Food Safety prospect conversations. Use this deck alongside "
            "the Food Processing deck for comprehensive Food Safety vertical coverage."
        ),
    ),
    (
        "food-processing",
        "Intro to Synexis - Food Processing / Food Safety",
        "Marketing Approved Collateral/Intro to Synexis_Food Processing_0526.pptx",
        "https://243636346.fs1.hubspotusercontent-na2.net/hubfs/243636346/Rep%20Agent%20Content/Intro%20to%20Synexis_Food%20Processing_0526.pptx",
        (
            "Overview and introduction to Synexis DHP® technology for the Food Processing and "
            "Food Safety vertical. This deck is the primary shareable resource for both Food "
            "Processing and Food Safety vertical conversations. Covers the value proposition for "
            "food processing facilities, food safety programs, packaging areas, cold storage, "
            "dry storage, and employee/common spaces in food manufacturing. Topics include: what "
            "Synexis technology does, how DHP® provides continuous touchless pathogen control in "
            "occupied food processing and food safety environments, efficacy data for Listeria, "
            "Salmonella, E. coli, and other food safety pathogens, food safety compliance and "
            "HACCP framing, device lineup and placement guidance, and rep talking points for "
            "Food Processing and Food Safety prospect conversations. Use this deck as the primary "
            "shareable resource for Food Processing and Food Safety vertical introductions and "
            "overview conversations."
        ),
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk_text(title: str, description: str, share_url: str) -> str:
    return (
        f"{title}\n\n"
        f"{description}\n\n"
        f"Share this deck with your prospect: {share_url}"
    )


def _build_vector(slug: str, title: str, file_path: str, share_url: str, description: str, embedding: list) -> dict:
    text = _chunk_text(title, description, share_url)
    return {
        "id": f"vertical-deck-{slug}",
        "values": embedding,
        "metadata": {
            "source":             title,
            "file_path":          file_path,
            "share_url":          share_url,
            "doc_id":             f"vertical-deck-{slug}",
            "chunk_index":        0,
            "source_category":    "Industry Specific Decks",
            "content_type":       "Industry Specific Decks",
            "intake_mode":        "manual-ingest",
            "tier":               2,
            "surface_citations":  True,
            "governance_status":  "approved",
            "page_or_slide":      1,
            "has_efficacy_claim": False,
            "extension":          "pptx",
            "extractor_used":     "manual",
            "token_count":        len(text.split()),
            "text":               text,
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Ingest vertical pitch deck metadata as Tier 2 Pinecone chunks."
    )
    ap.add_argument(
        "--confirm",
        action="store_true",
        help="Actually upsert to Pinecone (default: dry run).",
    )
    args = ap.parse_args(argv)

    voyage_key   = os.getenv("VOYAGE_API_KEY")
    pinecone_key = os.getenv("PINECONE_API_KEY")
    index_name   = os.getenv("PINECONE_INDEX_NAME", "sra")

    if not voyage_key:
        sys.exit("VOYAGE_API_KEY missing from .env")
    if args.confirm and not pinecone_key:
        sys.exit("PINECONE_API_KEY missing from .env")

    voyage = voyageai.Client(api_key=voyage_key)

    texts = [_chunk_text(title, desc, share_url) for _, title, _fp, share_url, desc in VERTICAL_DECKS]

    print(f"[vertical_decks] Embedding {len(texts)} deck summary chunks via Voyage …")
    result     = voyage.embed(texts, model="voyage-3", input_type="document")
    embeddings = result.embeddings

    vectors = [
        _build_vector(slug, title, file_path, share_url, desc, emb)
        for (slug, title, file_path, share_url, desc), emb in zip(VERTICAL_DECKS, embeddings)
    ]

    print()
    for v in vectors:
        marker = "[DRY RUN]" if not args.confirm else "[UPSERT] "
        print(f"  {marker}  {v['id']}")
        print(f"             {v['metadata']['source']}")
        print(f"             file_path={v['metadata']['file_path']}")
        print(f"             tier=2  surface_citations=True")
        print(f"             share_url={v['metadata']['share_url']}")
        print()

    if args.confirm:
        pc    = Pinecone(api_key=pinecone_key)
        index = pc.Index(index_name)
        index.upsert(vectors=vectors, namespace="")
        print(f"[vertical_decks] ✓ Upserted {len(vectors)} Tier 2 vertical deck vectors to index '{index_name}'.")
    else:
        print(
            f"[vertical_decks] Dry run complete — {len(vectors)} vectors would be upserted. "
            "Pass --confirm to write."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())

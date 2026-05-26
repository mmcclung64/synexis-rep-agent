"""
Ingest Synexis instructional video metadata as Tier 2 (shareable) chunks into Pinecone.

Each video gets one vector. The text encodes the video title and a short description of
the content so the Rep Agent can retrieve and surface the link when a rep asks about a
specific device or procedure.

These are the SHAREABLE counterpart to the Tier 3 transcript chunks that live in the
SharePoint "Training Video Scripts" folder. Transcripts shape agent answers (Tier 3);
these vectors are what the agent actually surfaces and cites to the rep (Tier 2).

All 7 public videos from synexis.com/instructional-videos/ are included.
(Video 1148206547 / Module 1D is private/deleted and excluded — matches the *1D* exclude
pattern already in watched_folders.json for Marketing Approved Collateral.)

Usage (from repo root):
    python3 -m pipeline.ingest_training_videos            # dry run — shows what would be upserted
    python3 -m pipeline.ingest_training_videos --confirm  # actually upsert to Pinecone
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

# Canonical shareable URL — this is what gets cited to the rep
PAGE_URL = "https://synexis.com/instructional-videos/"

# ---------------------------------------------------------------------------
# Video catalog
# (title, vimeo_id, description)
# Order matches approximate module sequence on the page.
# ---------------------------------------------------------------------------
TRAINING_VIDEOS = [
    (
        "Synexis Sphere Instructional Guide",
        "1178952643",
        (
            "Step-by-step instructional video covering setup, placement, and operation "
            "of the Synexis Sphere DHP® device. Covers device features, indicator lights, "
            "and general use guidance for reps and end users."
        ),
    ),
    (
        "Synexis Sentry XL Tutorial",
        "1178952586",
        (
            "Tutorial video for the Synexis Sentry XL DHP® device. Covers features, "
            "operation, and guidance for deploying the Sentry XL in commercial and "
            "institutional environments."
        ),
    ),
    (
        "Synexis Sentry XL DHP Animation",
        "1154518269",
        (
            "Animated explainer video showing how the Synexis Sentry XL generates "
            "dry hydrogen peroxide (DHP®) for continuous, touchless pathogen control. "
            "Useful for customer education on how the technology works."
        ),
    ),
    (
        "Synexis Sentry XL Instructional Mounting Video",
        "1178953442",
        (
            "Mounting and installation guide for the Synexis Sentry XL DHP® device. "
            "Covers physical mounting, positioning, and installation steps."
        ),
    ),
    (
        "Synexis Blade and Bulb Replacement Guide",
        "1178953614",
        (
            "Step-by-step video guide for replacing the catalyst blade and UV-A bulb "
            "in Synexis DHP® devices. Covers maintenance intervals and the replacement procedure."
        ),
    ),
    (
        "Synexis Sphere OUS Instructional Video",
        "1178952730",
        (
            "Outside-U.S. (OUS) instructional video for the Synexis Sphere DHP® device. "
            "Covers international deployment and operational guidance for markets outside "
            "the United States."
        ),
    ),
    (
        "Synexis Sphere Mounting Video",
        "1178952692",
        (
            "Mounting and installation guide for the Synexis Sphere DHP® device. "
            "Covers physical mounting, wall/ceiling positioning, and installation steps."
        ),
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk_text(title: str, description: str) -> str:
    return (
        f"{title}\n\n"
        f"{description}\n\n"
        f"Watch this video at: {PAGE_URL}"
    )


def _build_vector(title: str, vimeo_id: str, description: str, embedding: list) -> dict:
    text = _chunk_text(title, description)
    return {
        "id": f"training-video-{vimeo_id}",
        "values": embedding,
        "metadata": {
            "source":             title,
            "file_path":          PAGE_URL,
            "doc_id":             f"training-video-{vimeo_id}",
            "chunk_index":        0,
            "source_category":    "Training Videos",
            "content_type":       "Training Video",
            "intake_mode":        "manual-ingest",
            "tier":               2,
            "surface_citations":  True,
            "governance_status":  "approved",
            "page_or_slide":      "video",
            "has_efficacy_claim": False,
            "extension":          "mp4",
            "extractor_used":     "manual",
            "token_count":        len(text.split()),
            "vimeo_id":           vimeo_id,
            "text":               text,
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Ingest training video metadata as Tier 2 Pinecone chunks."
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

    # Embed
    voyage = voyageai.Client(api_key=voyage_key)
    texts  = [_chunk_text(t, d) for t, _, d in TRAINING_VIDEOS]

    print(f"[training_videos] Embedding {len(texts)} video chunks via Voyage …")
    result     = voyage.embed(texts, model="voyage-3", input_type="document")
    embeddings = result.embeddings

    vectors = [
        _build_vector(title, vimeo_id, desc, emb)
        for (title, vimeo_id, desc), emb in zip(TRAINING_VIDEOS, embeddings)
    ]

    # Preview
    print()
    for v in vectors:
        marker = "[DRY RUN]" if not args.confirm else "[UPSERT] "
        print(f"  {marker}  {v['id']}")
        print(f"             {v['metadata']['source']}")
        print(f"             tier=2  surface_citations=True  file_path={PAGE_URL}")
        print()

    if args.confirm:
        pc    = Pinecone(api_key=pinecone_key)
        index = pc.Index(index_name)
        index.upsert(vectors=vectors, namespace="")
        print(f"[training_videos] ✓ Upserted {len(vectors)} Tier 2 video vectors to index '{index_name}'.")
    else:
        print(
            f"[training_videos] Dry run complete — {len(vectors)} vectors would be upserted. "
            "Pass --confirm to write."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())

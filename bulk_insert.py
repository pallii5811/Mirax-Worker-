import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

try:
    from supabase import create_client  # type: ignore
except Exception:
    create_client = None


SUPABASE_URL = "https://rtjmnjromqpsfqsgyfvp.supabase.co"
_SUPABASE_PUBLISHABLE_FALLBACK = "sb_publishable_oqwwYsG10z7HvPrJOifF-w_J7ARllCp"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_supabase_key() -> str:
    # Load .env from repo root (local convenience). Optional dependency.
    try:
        if load_dotenv is not None:
            load_dotenv(override=False)
    except Exception:
        pass

    k = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if k:
        return k
    return _SUPABASE_PUBLISHABLE_FALLBACK


DEFAULT_USER_ID = "f7b9229f-9bcf-48f7-a91c-5c4b38a86608"

DEFAULT_CATEGORIES = [
    "Cliniche Private",
    "Studi Dentistici",
    "Avvocati Penalisti",
    "Commercialisti",
    "Hotel 4 stelle",
    "Centri Estetici",
    "Officine Meccaniche",
    "Palestre",
    "Ristoranti Gourmet",
    "Agenzie Immobiliari",
    "agenzie spettacolo",
    "agenzie web",
    "ristoranti",
    "osterie",
    "paninoteche",
    "pizzerie",
    "barbieri",
    "parrucchieri",
]

DEFAULT_CITIES = [
    "Milano",
    "Roma",
    "Napoli",
    "Torino",
    "Palermo",
    "Genova",
    "Bologna",
    "Firenze",
    "Bari",
    "Catania",
    "Venezia",
    "Verona",
    "Messina",
    "Padova",
    "Trieste",
    "Brescia",
    "Parma",
    "Prato",
    "Modena",
    "Reggio Calabria",
]


def _chunks(items: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    if n <= 0:
        return [items]
    return [items[i : i + n] for i in range(0, len(items), n)]


def main() -> None:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--user-id", type=str, default=DEFAULT_USER_ID)
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=200,
        help="Numero massimo di job da inserire (default 200).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Numero righe per insert (default 100).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Esegue davvero l'insert. Senza --yes stampa solo il conteggio.",
    )

    args = parser.parse_args()

    if create_client is None:
        raise SystemExit("ERROR: supabase-py non installato. Esegui: pip install supabase")

    user_id = (args.user_id or "").strip()
    if not user_id:
        raise SystemExit("ERROR: --user-id è obbligatorio")

    cities = DEFAULT_CITIES
    categories = DEFAULT_CATEGORIES

    # Cross product
    now_iso = _utc_now_iso()
    payloads: List[Dict[str, Any]] = []
    for cat in categories:
        for city in cities:
            payloads.append(
                {
                    "user_id": user_id,
                    "status": "pending",
                    "category": cat,
                    "location": city,
                    "results": None,
                    "created_at": now_iso,
                }
            )

    total = len(payloads)
    max_jobs = int(args.max_jobs or 0)
    if max_jobs > 0 and max_jobs < total:
        payloads = payloads[:max_jobs]

    print(f"[bulk_insert] categories: {len(categories)}")
    print(f"[bulk_insert] cities: {len(cities)}")
    print(f"[bulk_insert] cross-product: {total}")
    print(f"[bulk_insert] will insert: {len(payloads)}")

    if not args.yes:
        print("[bulk_insert] Dry-run. Passa --yes per inserire su Supabase.")
        return

    supabase_key = _get_supabase_key()
    if supabase_key == _SUPABASE_PUBLISHABLE_FALLBACK:
        print(
            "[bulk_insert] WARNING: stai usando la publishable key come fallback. "
            "Se hai RLS attiva, setta SUPABASE_SERVICE_ROLE_KEY in .env."
        )

    supabase = create_client(SUPABASE_URL, supabase_key)

    chunk_size = int(args.chunk_size or 100)
    batches = _chunks(payloads, chunk_size)
    print(f"[bulk_insert] inserting batches: {len(batches)} (chunk_size={chunk_size})")

    inserted = 0
    for bi, batch in enumerate(batches, start=1):
        resp = supabase.table("searches").insert(batch).execute()
        data = getattr(resp, "data", None)
        if isinstance(data, list):
            inserted += len(data)
        else:
            inserted += len(batch)
        print(f"[bulk_insert] batch {bi}/{len(batches)} ok -> inserted ~{inserted}")

    print(f"[bulk_insert] DONE. Inserted: {inserted}")


if __name__ == "__main__":
    main()

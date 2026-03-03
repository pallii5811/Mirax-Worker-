import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

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

# Public datasets of Italian municipalities (comuni)
COMUNI_DATASET_URLS: Sequence[str] = (
    "https://raw.githubusercontent.com/italia/comuni-json/master/comuni.json",
    "https://raw.githubusercontent.com/matteocontrini/comuni-json/master/comuni.json",
)


DEFAULT_USER_ID = "f7b9229f-9bcf-48f7-a91c-5c4b38a86608"


CATEGORIES: List[str] = [
    "Avvocati",
    "Commercialisti",
    "Notai",
    "Consulenti del lavoro",
    "Agenzie immobiliari",
    "Costruttori edili",
    "Imprese di pulizie",
    "Imprese di traslochi",
    "Idraulici",
    "Elettricisti",
    "Falegnami",
    "Serramentisti",
    "Fabbri",
    "Giardinieri",
    "Disinfestazioni",
    "Climatizzatori",
    "Caldaie e manutenzione",
    "Fotovoltaico",
    "Installatori impianti",
    "Autofficine",
    "Carrozzerie",
    "Gommisti",
    "Elettrauto",
    "Concessionarie auto",
    "Noleggio auto",
    "Lavaggi auto",
    "Dentisti",
    "Ortodonzia",
    "Cliniche private",
    "Poliambulatori",
    "Fisioterapia",
    "Oculisti",
    "Dermatologi",
    "Psicologi",
    "Nutrizionisti",
    "Farmacie",
    "Veterinari",
    "Palestre",
    "Personal trainer",
    "Piscine",
    "Centri estetici",
    "Parrucchieri",
    "Barbieri",
    "SPA e benessere",
    "Ristoranti",
    "Pizzerie",
    "Bar",
    "Pasticcerie",
    "Gelaterie",
    "Hotel",
    "B&B",
    "Agriturismi",
    "Agenzie viaggi",
    "Studi di architettura",
    "Ingegneri",
    "Geometri",
    "Assicurazioni",
    "Banche e consulenza finanziaria",
    "Agenzie marketing",
    "Agenzie web",
    "Tipografie",
    "Negozi abbigliamento",
    "Ottici",
    "Telefonia",
    "Informatica e assistenza PC",
    "Arredamento",
    "Ferramenta",
    "Supermercati",
    "Panifici",
    "Macellerie",
    "Pescherie",
    "Scuole guida",
    "Autoscuole",
    "Scuole di lingua",
    "Asili nido",
    "Scuole private",
    "Assistenza anziani",
    "Badanti",
    "Case di riposo",
    "Studi veterinari",
    "Negozi animali",
    "Logistica e spedizioni",
    "Corrieri",
    "Magazzini",
    "Aziende manifatturiere",
    "E-commerce",
    "Negozi di elettronica",
    "Centri assistenza elettrodomestici",
    "Carpenteria metallica",
    "Servizi informatici",
    "Software house",
    "Coworking",
    "Studi fotografici",
    "Wedding planner",
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_supabase_key() -> str:
    try:
        if load_dotenv is not None:
            load_dotenv(override=False)
    except Exception:
        pass

    k = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if k:
        return k
    return _SUPABASE_PUBLISHABLE_FALLBACK


def _chunks(items: List[Dict[str, Any]], n: int) -> Iterable[List[Dict[str, Any]]]:
    if n <= 0:
        yield items
        return
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _download_json(url: str, timeout_s: float = 25.0) -> Any:
    import urllib.request

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as r:
        raw = r.read()
    return json.loads(raw.decode("utf-8"))


def _extract_comuni_names(dataset: Any) -> List[str]:
    # Supports multiple common schemas.
    # Expected output: list of strings (municipality names)
    out: List[str] = []

    if isinstance(dataset, list):
        for row in dataset:
            if isinstance(row, str):
                name = row.strip()
                if name:
                    out.append(name)
                continue
            if not isinstance(row, dict):
                continue

            # Common keys
            for k in ("nome", "name", "denominazione", "comune"):
                v = row.get(k)
                if isinstance(v, str) and v.strip():
                    out.append(v.strip())
                    break
    elif isinstance(dataset, dict):
        # Some datasets wrap the list
        for k in ("comuni", "data", "items", "results"):
            if k in dataset:
                return _extract_comuni_names(dataset.get(k))

    # de-dup while preserving order
    seen: Set[str] = set()
    uniq: List[str] = []
    for n in out:
        key = n.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(n)
    return uniq


def load_comuni_names() -> List[str]:
    last_err: Optional[str] = None
    for url in COMUNI_DATASET_URLS:
        try:
            data = _download_json(url)
            names = _extract_comuni_names(data)
            if names:
                return names
        except Exception as e:
            last_err = f"{url}: {e}"
            continue

    # Fallback: minimal list (keeps script runnable even if network is blocked)
    if last_err:
        print(f"[generate_national_queue] WARNING: comuni download failed ({last_err}). Using fallback list.")
    return [
        "Roma",
        "Milano",
        "Napoli",
        "Torino",
        "Palermo",
        "Genova",
        "Bologna",
        "Firenze",
        "Bari",
        "Catania",
    ]


def _normalize_city(s: str) -> str:
    return " ".join((s or "").strip().split())


def _existing_pairs_for_city(
    supabase: Any,
    city: str,
    statuses: Sequence[str],
) -> Set[Tuple[str, str]]:
    # Returns a set of (city_lower, category_lower)
    # Query is scoped by city to keep it cheap.
    resp = (
        supabase.table("searches")
        .select("category,location,status")
        .eq("location", city)
        .in_("status", list(statuses))
        .execute()
    )

    rows = getattr(resp, "data", None) or []
    out: Set[Tuple[str, str]] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        c = (r.get("category") or "").strip()
        loc = (r.get("location") or "").strip()
        if c and loc:
            out.add((loc.lower(), c.lower()))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--user-id", type=str, default=DEFAULT_USER_ID)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Non inserisce nulla, stampa solo quante righe inserirebbe.",
    )
    parser.add_argument(
        "--max-cities",
        type=int,
        default=0,
        help="Limita il numero di comuni (debug). 0 = tutti.",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Limita il numero totale di job da inserire (debug). 0 = illimitato.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Pausa tra i batch (ms). Utile per rate-limit.",
    )
    args = parser.parse_args()

    if create_client is None:
        raise SystemExit("ERROR: supabase-py non installato. Esegui: pip install supabase")

    user_id = (args.user_id or "").strip()
    if not user_id:
        raise SystemExit("ERROR: --user-id è obbligatorio")

    supabase_key = _get_supabase_key()
    if supabase_key == _SUPABASE_PUBLISHABLE_FALLBACK:
        print(
            "[generate_national_queue] WARNING: stai usando la publishable key come fallback. "
            "Se hai RLS attiva, setta SUPABASE_SERVICE_ROLE_KEY in .env."
        )

    supabase = create_client(SUPABASE_URL, supabase_key)

    comuni = [_normalize_city(x) for x in load_comuni_names()]
    comuni = [c for c in comuni if c]

    if int(args.max_cities or 0) > 0:
        comuni = comuni[: int(args.max_cities)]

    categories = [c.strip() for c in CATEGORIES if c.strip()]

    print(f"[generate_national_queue] comuni: {len(comuni)}")
    print(f"[generate_national_queue] categories: {len(categories)}")
    print(f"[generate_national_queue] cross-product: {len(comuni) * len(categories)}")

    statuses_to_skip = ("pending", "completed")
    now_iso = _utc_now_iso()

    total_to_insert = 0
    inserted = 0
    buffer: List[Dict[str, Any]] = []

    # Iterate by city to make dedup queries cheap.
    for ci, city in enumerate(comuni, start=1):
        existing = set()
        try:
            existing = _existing_pairs_for_city(supabase, city, statuses=statuses_to_skip)
        except Exception as e:
            print(f"[generate_national_queue] WARNING: failed dedup query for city='{city}': {e}")
            existing = set()

        for cat in categories:
            if (city.lower(), cat.lower()) in existing:
                continue

            buffer.append(
                {
                    "user_id": user_id,
                    "status": "pending",
                    "category": cat,
                    "location": city,
                    "results": None,
                    "created_at": now_iso,
                }
            )
            total_to_insert += 1

            max_jobs = int(args.max_jobs or 0)
            if max_jobs > 0 and total_to_insert >= max_jobs:
                break

            if len(buffer) >= int(args.chunk_size or 1000):
                if args.dry_run:
                    inserted += len(buffer)
                    buffer.clear()
                else:
                    resp = supabase.table("searches").insert(buffer).execute()
                    data = getattr(resp, "data", None)
                    inserted += len(data) if isinstance(data, list) else len(buffer)
                    buffer.clear()

                if int(args.sleep_ms or 0) > 0:
                    time.sleep(int(args.sleep_ms) / 1000.0)

        if max_jobs > 0 and total_to_insert >= max_jobs:
            break

        if ci % 50 == 0:
            print(
                f"[generate_national_queue] progress: {ci}/{len(comuni)} cities | "
                f"planned={total_to_insert} | inserted~{inserted}"
            )

    # Flush remainder
    if buffer:
        if args.dry_run:
            inserted += len(buffer)
        else:
            resp = supabase.table("searches").insert(buffer).execute()
            data = getattr(resp, "data", None)
            inserted += len(data) if isinstance(data, list) else len(buffer)

    print(f"[generate_national_queue] DONE. planned={total_to_insert} inserted~{inserted}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[generate_national_queue] Interrotto.")
        sys.exit(130)

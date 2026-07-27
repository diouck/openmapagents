"""
embedder.py — Indexation multi-vector dans PostgreSQL pgvector
Strategie : un embedding par tool (description+triggers) + un par exemple de query
=> ~60 vecteurs pour 15 tools => couverture semantique maximale

Usage :
    python rag/embedder.py --reset --test

Installation :
    pip install fastembed psycopg2-binary python-dotenv
"""

import os, sys, json, time, argparse, logging, pathlib
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("embedder")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB",       "openmap")
PG_USER     = os.getenv("PG_USER",     "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# Modeles fastembed multilingues supportes :
#   "sentence-transformers/paraphrase-multilingual-mpnet-base-v2" -> 768 dims (defaut)
#   "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2" -> 384 dims, rapide
#   "intfloat/multilingual-e5-large"                              -> 1024 dims, precis
EMBED_MODEL = os.getenv(
    "EMBED_MODEL",
    "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
)
EMBED_DIM  = int(os.getenv("EMBED_DIM", "768"))
TABLE_NAME = "rag_tools"

# ─── CONNEXION ────────────────────────────────────────────────────────────────
def get_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )

# ─── SCHEMA MULTI-VECTOR ──────────────────────────────────────────────────────
SCHEMA_SQL = f"""
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id          TEXT PRIMARY KEY,
    tool_id     TEXT NOT NULL,
    domain      TEXT NOT NULL,
    title       TEXT NOT NULL,
    description TEXT NOT NULL,
    triggers    TEXT[] NOT NULL DEFAULT '{{}}',
    examples    JSONB NOT NULL DEFAULT '[]',
    schema      JSONB NOT NULL DEFAULT '{{}}',
    endpoint    TEXT NOT NULL,
    embed_text  TEXT NOT NULL,
    embed_type  TEXT DEFAULT 'main',
    embedding   vector({EMBED_DIM}),
    indexed_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS {TABLE_NAME}_embedding_idx
    ON {TABLE_NAME}
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS {TABLE_NAME}_tool_id_idx
    ON {TABLE_NAME} (tool_id);

CREATE INDEX IF NOT EXISTS {TABLE_NAME}_domain_idx
    ON {TABLE_NAME} (domain);
"""

def setup_db(conn, reset=False):
    with conn.cursor() as cur:
        if reset:
            log.info(f"DROP TABLE {TABLE_NAME}...")
            cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
        cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("Schema multi-vector pret")

# ─── CONSTRUCTION DES VECTEURS ────────────────────────────────────────────────
def build_rows(tool: dict) -> list[dict]:
    """
    Construit toutes les lignes a indexer pour un tool :
      - 1 ligne 'main'    : titre x3 + description + triggers
      - N lignes 'example': une par query d exemple
    """
    rows = []
    tid = tool["id"]

    # ── Ligne principale : dense, courte ──────────────────────────────────────
    title    = tool["title"]
    triggers = " | ".join(tool["triggers"][:15])
    # Description : garder seulement les 2 premieres phrases
    desc_sentences = tool["description"].replace(". ", ".\n").split("\n")
    desc_short = " ".join(desc_sentences[:2])

    main_text = f"{title}. {title}. {title}. {desc_short} {triggers}"
    rows.append({
        "id":         tid,
        "tool_id":    tid,
        "embed_type": "main",
        "embed_text": main_text,
    })

    # ── Lignes exemples : une par query ──────────────────────────────────────
    for i, ex in enumerate(tool.get("examples", [])):
        q = ex.get("query", "").strip()
        if not q:
            continue
        rows.append({
            "id":         f"{tid}__ex{i}",
            "tool_id":    tid,
            "embed_type": "example",
            "embed_text": q,
        })

    return rows

# ─── MODELE FASTEMBED ─────────────────────────────────────────────────────────
_model = None

def get_model():
    global _model
    if _model is None:
        try:
            from fastembed import TextEmbedding
        except ImportError:
            log.error("fastembed non installe. Installer : pip install fastembed")
            sys.exit(1)
        model_name = EMBED_MODEL
        log.info(f"Chargement modele fastembed : {model_name} ...")
        t0 = time.time()
        _model = TextEmbedding(model_name=model_name)
        log.info(f"Modele charge en {time.time()-t0:.1f}s")
    return _model

def embed_texts(texts: list[str]) -> list[list[float]]:
    model = get_model()
    return [list(map(float, e)) for e in model.embed(texts)]

def embed_query(query: str) -> list[float]:
    return embed_texts([query])[0]

# ─── INDEXATION ───────────────────────────────────────────────────────────────
def index_tools(conn, tools: list[dict]):
    # Construire toutes les lignes
    all_rows = []
    for tool in tools:
        all_rows.extend(build_rows(tool))

    log.info(f"Indexation : {len(tools)} tools → {len(all_rows)} vecteurs")

    # Calculer tous les embeddings en batch
    texts = [r["embed_text"] for r in all_rows]
    log.info(f"Calcul embeddings batch ({len(texts)} textes)...")
    t0 = time.time()
    embeddings = embed_texts(texts)
    log.info(f"Embeddings calcules en {time.time()-t0:.1f}s — dim={len(embeddings[0])}")

    upsert_sql = f"""
    INSERT INTO {TABLE_NAME}
        (id, tool_id, domain, title, description, triggers,
         examples, schema, endpoint, embed_text, embed_type, embedding)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector)
    ON CONFLICT (id) DO UPDATE SET
        tool_id    = EXCLUDED.tool_id,
        embed_text = EXCLUDED.embed_text,
        embed_type = EXCLUDED.embed_type,
        embedding  = EXCLUDED.embedding,
        indexed_at = NOW();
    """

    # Mapping tool_id → tool pour les metadonnees
    tool_map = {t["id"]: t for t in tools}

    with conn.cursor() as cur:
        for row, emb in zip(all_rows, embeddings):
            tid  = row["tool_id"]
            tool = tool_map[tid]
            vec  = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
            cur.execute(upsert_sql, (
                row["id"], tid,
                tool["domain"], tool["title"], tool["description"],
                tool["triggers"],
                json.dumps(tool["examples"]),
                json.dumps(tool["schema"]),
                tool["endpoint"],
                row["embed_text"],
                row["embed_type"],
                vec,
            ))

    conn.commit()
    log.info(f"{len(all_rows)} vecteurs indexes dans pgvector")

    # Afficher repartition
    by_type = {}
    for r in all_rows:
        by_type[r["embed_type"]] = by_type.get(r["embed_type"], 0) + 1
    for t, c in by_type.items():
        log.info(f"  {t}: {c} vecteurs")

# ─── VERIFICATION ─────────────────────────────────────────────────────────────
def check_index(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(DISTINCT tool_id) FROM {TABLE_NAME}")
        tools = cur.fetchone()[0]
        cur.execute(f"SELECT domain, COUNT(DISTINCT tool_id) FROM {TABLE_NAME} GROUP BY domain ORDER BY domain")
        domains = cur.fetchall()
        cur.execute(f"SELECT MAX(indexed_at) FROM {TABLE_NAME}")
        last = cur.fetchone()[0]

    print(f"\n{'='*50}")
    print(f"Table    : {TABLE_NAME}")
    print(f"Vecteurs : {total} ({tools} tools distincts)")
    print(f"Modele   : {EMBED_MODEL} ({EMBED_DIM} dims)")
    print(f"Indexe   : {last}")
    print(f"\nPar domaine :")
    for domain, count in domains:
        print(f"  {domain}: {count} tools")
    print(f"{'='*50}\n")

# ─── TEST RETRIEVAL ───────────────────────────────────────────────────────────
def test_retrieval(conn):
    TEST_QUERIES = [
        ("restaurants a Nantes",             "query_overture_places"),
        ("Nantes restaurants",               "query_overture_places"),
        ("NDVI Dakar Sentinel-2",            "gee_imagery"),
        ("satellite imagery temperature",    "gee_imagery"),
        ("itineraire velo gare chateau",     "compute_route"),
        ("zone accessible 15 min voiture",   "compute_isochrone"),
        ("ma base PostGIS parcelles",        "db_query"),
        ("buffer 500m autour des commerces", "spatial_analysis"),
        ("occupation du sol worldcover",     "gee_imagery"),
        ("communes Loire-Atlantique",        "query_overture_divisions"),
    ]

    print(f"\n{'='*60}")
    print("TEST RETRIEVAL MULTI-VECTOR")
    print(f"{'='*60}")

    ok = 0
    for query, expected in TEST_QUERIES:
        vec = embed_query(query)
        vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"

        # Requete : DISTINCT sur tool_id, prendre le meilleur score par tool
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT ON (tool_id)
                    tool_id,
                    1 - (embedding <=> %s::vector) AS score,
                    embed_type
                FROM {TABLE_NAME}
                ORDER BY tool_id, embedding <=> %s::vector
            """, (vec_str, vec_str))
            raw = cur.fetchall()

        # Retrier par score desc
        results = sorted(raw, key=lambda x: -x[1])[:3]
        top_id    = results[0][0] if results else "?"
        top_score = results[0][1] if results else 0
        top_type  = results[0][2] if results else "?"
        top3      = [(r[0], round(r[1], 3)) for r in results]
        match     = "OK" if top_id == expected else "!!"

        print(f"[{match}] '{query}'")
        print(f"     top1={top_id}({top_score:.3f},{top_type}) | top3={top3}")
        if top_id == expected:
            ok += 1

    print(f"\nScore : {ok}/{len(TEST_QUERIES)} ({100*ok//len(TEST_QUERIES)}%)")
    print(f"{'='*60}\n")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--test",  action="store_true")
    args = parser.parse_args()

    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
    from rag.tool_registry import TOOL_REGISTRY

    log.info(f"Connexion PostgreSQL {PG_HOST}:{PG_PORT}/{PG_DB}...")
    conn = get_conn()
    log.info("Connecte")

    if args.check:
        check_index(conn)
        conn.close()
        return

    setup_db(conn, reset=args.reset)
    index_tools(conn, TOOL_REGISTRY)
    check_index(conn)
    if args.test:
        test_retrieval(conn)

    conn.close()
    log.info("Termine")

if __name__ == "__main__":
    main()

# This program loads these differents csv and prints the unique model names found in each file, with the amount of times each appeat

FILES = [
    "../AMAZON/amazon_offers.csv",
    "../CARREFOUR/scraping_carrefour.csv",
    "../CDISCOUNT/scraping_cdiscount.csv",
    "../LECLERC/scraping_leclerc.csv",
    "../RAKUTEN/Rakuten_data.csv"
]

# --- added code below ---
import argparse
import sys
from pathlib import Path
import pandas as pd
import re

CANDIDATE_MODEL_COLUMNS = ["descriptsmartphone", "Product Name"]
LIENS_XLSX = "../lien.xlsx"

def resolve_path(p: str) -> Path:
    return (Path(__file__).parent / p).resolve()

def normalize_colname(c: str) -> str:
    return c.lower().replace(" ", "").replace("_", "")

def guess_model_column(columns):
    # Prefer explicit known columns first (exact/normalized), else any containing 'model'
    lowered_map = {c.lower(): c for c in columns}
    normalized_map = {normalize_colname(c): c for c in columns}
    for target in CANDIDATE_MODEL_COLUMNS:
        # direct case-insensitive
        key = target.lower()
        if key in lowered_map:
            return lowered_map[key]
        # normalized (remove spaces/underscores)
        nkey = normalize_colname(target)
        if nkey in normalized_map:
            return normalized_map[nkey]
    for c in columns:
        if "model" in c.lower():
            return c
    return None

# --- new resilient CSV reader ---
def read_csv_resilient(path: Path):
    attempts = [
        ({}, "default"),
        ({"sep": ";"}, "semicolon"),
        ({"on_bad_lines": "skip"}, "skip_bad_lines"),
        ({"sep": ";", "on_bad_lines": "skip"}, "semicolon_skip"),
        ({"engine": "python", "sep": ";"}, "python_semicolon"),
        ({"engine": "python", "sep": ";", "on_bad_lines": "skip"}, "python_semicolon_skip"),
    ]
    last_err = None
    for params, label in attempts:
        p = dict(params)
        try:
            df = pd.read_csv(path, **p)
            # Heuristic: if we still have a single column that appears to contain semicolon-separated data, retry with other strategies
            if len(df.columns) == 1:
                only_col = df.columns[0]
                sample = df[only_col].astype(str)
                semicolon_in_header = ";" in only_col
                semicolon_ratio = sample.head(200).str.contains(";").mean() if not sample.empty else 0
                if semicolon_in_header or semicolon_ratio > 0.2:
                    # If this attempt did NOT explicitly set a separator, skip returning and try next
                    if "sep" not in p and "delimiter" not in p:
                        print(f"[DEBUG] Strategy '{label}' produced single semicolon-packed column -> retrying with explicit sep")
                        continue
            print(f"[INFO] Read {path.name} using strategy '{label}' rows={len(df)} cols={len(df.columns)}")
            return df
        except TypeError as te:
            if "on_bad_lines" in p:
                p.pop("on_bad_lines", None)
                try:
                    df = pd.read_csv(path, **p)
                    print(f"[INFO] Read {path.name} using strategy '{label}' (fallback without on_bad_lines) rows={len(df)} cols={len(df.columns)}")
                    return df
                except Exception as e2:
                    last_err = e2
                    continue
            last_err = te
        except Exception as e:
            last_err = e
            continue
    raise last_err

# --- new helper to expand single-column semicolon CSVs ---
def maybe_expand_semicolon(df: pd.DataFrame) -> pd.DataFrame:
    if len(df.columns) != 1:
        return df
    only_col = df.columns[0]
    # Heuristics: header contains semicolons OR majority of rows contain semicolons
    sample_series = df[only_col].astype(str)
    if (';' in only_col) or (sample_series.str.contains(';').mean() > 0.3):
        expanded = sample_series.str.split(';', expand=True)
        if expanded.empty:
            return df
        # First row assumed to be header
        header = [h.strip() if isinstance(h, str) else h for h in expanded.iloc[0].tolist()]
        expanded = expanded.iloc[1:].reset_index(drop=True)
        # Guard against duplicate / empty header names
        clean_header = []
        counts = {}
        for h in header:
            h = h or "col"
            base = h
            if h in counts:
                counts[h] += 1
                h = f"{base}_{counts[base]}"
            else:
                counts[h] = 0
            clean_header.append(h)
        expanded.columns = clean_header
        print(f"[INFO] Expanded single-column data into {len(expanded.columns)} columns via semicolon split")
        return expanded
    return df
# --- end new helper ---

def load_id_mapping():
    path = resolve_path(LIENS_XLSX)
    if not path.exists():
        print(f"[WARN] Mapping file not found: {path}")
        return None
    try:
        df_map = pd.read_excel(path, sheet_name="RAKUTEN")
    except Exception as e:
        print(f"[WARN] Failed reading mapping excel {path}: {e}")
        return None
    needed_cols = {"idsmartphone", "Phone"}
    if not needed_cols.issubset(set(df_map.columns)):
        print(f"[WARN] Mapping sheet missing required columns {needed_cols}")
        return None
    mapping = (
        df_map[["idsmartphone", "Phone"]]
        .dropna(subset=["idsmartphone", "Phone"])
        .set_index("idsmartphone")["Phone"]
        .to_dict()
    )
    if not mapping:
        print("[WARN] Empty idsmartphone -> Phone mapping")
    return mapping

def count_models(df: pd.DataFrame, col: str):
    series = df[col].astype(str).str.strip()
    series = series[series.ne("").values]
    vc = series.value_counts(dropna=True)
    return vc

def extract_exact_model(raw: str):
    """
    From a full product name, detect brand phrase then extract:
      first token after brand, optionally plus a suffix:
        - if next token in (plus, ultra, pro)
        - or next two tokens are ('pro','max')
    Returns normalized model string or None if pattern not found.
    """
    if not raw or not isinstance(raw, str):
        return None
    # Normalize separators -> spaces
    cleaned = re.sub(r"[^\w+]", " ", raw)
    # Tokenize preserving original (for case) while also making lowercase map
    orig_tokens = [t for t in cleaned.split() if t]
    if not orig_tokens:
        return None
    lower_tokens = [t.lower() for t in orig_tokens]

    for brand_tokens in BRAND_PATTERNS:
        blen = len(brand_tokens)
        # slide window
        for i in range(0, len(lower_tokens) - blen + 1):
            if lower_tokens[i:i+blen] == brand_tokens:
                after_idx = i + blen
                if after_idx >= len(orig_tokens):
                    return None
                # Base model token
                base = orig_tokens[after_idx]
                model_parts = [base]
                # Check multi-word suffix first
                if after_idx + 2 < len(orig_tokens):
                    t1 = lower_tokens[after_idx + 1]
                    t2 = lower_tokens[after_idx + 2]
                    if (t1, t2) in SUFFIX_MULTI:
                        model_parts.extend([orig_tokens[after_idx + 1], orig_tokens[after_idx + 2]])
                        return " ".join(model_parts)
                # Check single-word suffix
                if after_idx + 1 < len(orig_tokens):
                    t1 = lower_tokens[after_idx + 1]
                    if t1 in SUFFIX_SINGLE:
                        model_parts.append(orig_tokens[after_idx + 1])
                return " ".join(model_parts)
    return None

# --- new normalization helper ---
def normalize_exact_model(model: str) -> str:
    if not model:
        return model
    tokens = [t for t in model.strip().split() if t]
    if not tokens:
        return model
    base = tokens[0]

    plus_in_base = False
    if base.endswith("+"):
        plus_in_base = True
        base = base[:-1]  # strip trailing '+'

    # Uppercase leading letters before first digit (e.g., s23 -> S23, s24 -> S24)
    m = re.match(r'^([a-zA-Z]+)(\d.*)?$', base)
    if m:
        letters, rest = m.group(1), m.group(2) or ""
        base = letters.upper() + rest

    suffix_tokens = []
    for tok in tokens[1:]:
        low = tok.lower()
        if low == "pro":
            suffix_tokens.append("Pro")
        elif low == "max":
            suffix_tokens.append("Max")
        elif low == "plus":
            suffix_tokens.append("Plus")
        elif low == "ultra":
            suffix_tokens.append("Ultra")
        else:
            suffix_tokens.append(tok.capitalize())

    # If original base ended with '+', ensure 'Plus' suffix present
    if plus_in_base and "Plus" not in suffix_tokens:
        suffix_tokens.insert(0, "Plus")  # keep Plus first among suffixes

    normalized = " ".join([base] + suffix_tokens) if suffix_tokens else base
    return normalized
# --- end normalization helper ---

# --- existing global storage etc. remains below ---
EXTRACTED_EXACT_MODELS = set()

BRAND_PATTERNS = [
    ["apple", "iphone"],
    ["apple"],
    ["iphone"],
    ["samsung", "galaxy"],
    ["samsung"],  # added to handle cases like "Smartphone Samsung ..."
]

SUFFIX_SINGLE = {"plus", "ultra", "pro"}
SUFFIX_MULTI = [("pro", "max")]

# --- NEW: canonical phone parsing configuration ---
VALID_APPLE_MODELS = {"14", "15", "16", "16e"}
VALID_SAMSUNG_MODELS = {"s23", "s24", "s25"}
APPLE_VARIANTS = {
    "plus": "Plus",
    "pro": "Pro",
    "pro max": "Pro Max",
    "promax": "Pro Max",
}
SAMSUNG_VARIANTS = {
    "plus": "Plus",
    "ultra": "Ultra",
}
STORAGE_PATTERN = re.compile(
    r"(?i)\b(128|256|512|1024)(?:(?:\s*|[-_]?)(?:go|gb|g))?\b|\b(1)\s*(?:tb|t[o0])\b"
)
BRAND_PATTERNS_PARSE = [
    ("Apple", re.compile(r"(?i)\b(apple\s+iphone|iphone|apple|iph)\b")),
    ("Samsung", re.compile(r"(?i)\b(smartphone\s+samsung|samsung\s+galaxy|galaxy|samsung|gal)\b")),  # added smartphone samsung
]

PARSED_PHONE_MODELS = {}  # id -> {'brand':..., 'family':..., 'variant':..., 'storage':..., 'count': n}

def normalize_storage(raw: str) -> str:
    if not raw:
        return None
    r = raw.lower().replace(" ", "")
    if r in {"1tb", "1to", "1t0"}:
        return "1024"
    # strip trailing french/english unit markers
    r = re.sub(r'(go|gb|g)$', '', r)
    return r

def parse_phone_model(text: str, storage: str = None):
    """
    Parse a phone description into structured components.
    If storage is provided explicitly, it overrides detection (and must be one of allowed values).
    Returns dict or None.
    """
    if not text or not isinstance(text, str):
        return None
    lowered = text.lower()

    brand = None
    for bname, pattern in BRAND_PATTERNS_PARSE:
        if pattern.search(lowered):
            brand = bname
            break
    if not brand:
        return None

    # Storage handling
    if storage is None:
        storage_match = STORAGE_PATTERN.search(lowered)
        if not storage_match:
            return None
        # Prefer first capturing group (128/256/512/1024) else second (the '1' for 1TB/1To)
        captured = storage_match.group(1) or storage_match.group(2)
        storage = normalize_storage(captured)
        if storage == "1":  # normalize 1TB/1To
            storage = "1024"
    else:
        storage = normalize_storage(storage)
    if storage not in {"128", "256", "512", "1024"}:
        return None  # enforce allowed set

    # Model + variant
    family = None
    variant = ""
    tokens = re.findall(r"[a-z0-9]+", lowered)

    def consume_variant(after_tokens, variant_map):
        if not after_tokens:
            return ""
        if len(after_tokens) >= 2 and f"{after_tokens[0]} {after_tokens[1]}" in variant_map:
            return variant_map[f"{after_tokens[0]} {after_tokens[1]}"]
        if after_tokens[0] in variant_map:
            return variant_map[after_tokens[0]]
        return ""

    for i, tok in enumerate(tokens):
        if brand == "Apple":
            if tok in VALID_APPLE_MODELS:
                family = tok
                variant = consume_variant(tokens[i+1:i+3], APPLE_VARIANTS)
                break
        else:  # Samsung
            if tok in VALID_SAMSUNG_MODELS:
                family = tok.upper()
                variant = consume_variant(tokens[i+1:i+3], SAMSUNG_VARIANTS)
                break
    if not family:
        return None

    if brand == "Apple":
        canon_family = family if family != "16e" else "16e"
    else:
        canon_family = family.upper()

    return {
        "brand": brand,
        "family": canon_family if brand == "Samsung" or canon_family == "16e" else canon_family,
        "variant": variant,
        "storage": storage,
    }

def generate_phone_id(brand: str, family: str, variant: str, storage: str) -> str:
    brand_code = "A" if brand == "Apple" else "S"
    fam_code = family.upper()
    var_code = variant.upper().replace(" ", "") if variant else ""
    parts = [brand_code, fam_code]
    if var_code:
        parts.append(var_code)
    parts.append(storage)
    return "-".join(parts)

def record_parsed_model(parsed):
    pid = generate_phone_id(parsed["brand"], parsed["family"], parsed["variant"], parsed["storage"])
    entry = PARSED_PHONE_MODELS.get(pid)
    if entry:
        entry["count"] += 1
    else:
        PARSED_PHONE_MODELS[pid] = {**parsed, "id": pid, "count": 1}
# --- END NEW ---

def process_file(csv_path: str, override_col: str = None):
    path = resolve_path(csv_path)
    if not path.exists():
        print(f"[WARN] File not found: {path}")
        return False
    try:
        df = read_csv_resilient(path)
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")
        return False
    # --- new expansion step ---
    df = maybe_expand_semicolon(df)
    # --- end new expansion step ---
    if df.empty:
        print(f"[INFO] Empty file: {path}")
        return False

    model_col = override_col if override_col else guess_model_column(df.columns)
    used_mapping = False
    model_series = None

    if model_col and model_col in df.columns:
        model_series = df[model_col].astype(str).str.strip()
    elif "idsmartphone" in df.columns:
        mapping = load_id_mapping()
        if mapping:
            mapped = df["idsmartphone"].map(mapping)
            model_series = mapped.dropna().astype(str).str.strip()
            used_mapping = True
            model_col = "idsmartphone->Phone"
        else:
            print(f"[WARN] Could not build model names from idsmartphone for {path}")
            return False
    else:
        print(f"[WARN] No model column or idsmartphone in {path}. Available columns: {list(df.columns)}")
        return False

    model_series = model_series[model_series.ne("")]
    if model_series.empty:
        print(f"[INFO] No model values for {path}")
        return True

    counts = model_series.value_counts()
    print(f"\n=== {path.name} (column: {model_col}{' (mapped)' if used_mapping else ''}) ===")
    for model, cnt in counts.items():
        print(f"{model}\t{cnt}")
    # --- modified: normalize before storing ---
    unique_models = model_series.dropna().unique()
    for m in unique_models:
        exact = extract_exact_model(m)
        if exact:
            EXTRACTED_EXACT_MODELS.add(normalize_exact_model(exact))
    # --- end modified ---
    # --- NEW: per-row parsing for canonical ID generation (include duplicates for counts) ---
    for raw in model_series:
        parsed = parse_phone_model(raw)
        if parsed:
            record_parsed_model(parsed)
    # --- END NEW ---
    return True

def main(argv=None):
    parser = argparse.ArgumentParser(description="Print unique model names and counts per CSV file.")
    parser.add_argument("--column", "-c", help="Explicit model column name to use for all files.")
    args = parser.parse_args(argv)

    any_success = False
    for f in FILES:
        ok = process_file(f, args.column)
        any_success = any_success or ok

    # --- new final exact model printout ---
    if EXTRACTED_EXACT_MODELS:
        print("\n=== Extracted exact phone models ===")
        for em in sorted(EXTRACTED_EXACT_MODELS, key=lambda s: (s.lower(), s)):
            print(em)
    else:
        print("\n=== No exact phone models extracted ===")
    # --- NEW: final print of parsed canonical IDs ---
    if PARSED_PHONE_MODELS:
        print("\n=== Canonical phone models with IDs (brand/model/variant/storage) ===")
        # Sort by brand, family (numeric if Apple), variant, storage
        def sort_key(item):
            pid, data = item
            brand_order = 0 if data["brand"] == "Apple" else 1
            # numeric part for apple families
            fam = data["family"]
            try:
                fam_num = int(re.sub(r"\D+", "", fam)) if data["brand"] == "Apple" else fam
            except:
                fam_num = fam
            return (brand_order, str(fam_num), data["variant"], int(data["storage"]))
        for pid, data in sorted(PARSED_PHONE_MODELS.items(), key=sort_key):
            variant_part = f" {data['variant']}" if data['variant'] else ""
            print(f"{pid}\t{data['brand']} {data['family']}{variant_part} {data['storage']} ({data['count']})")
    else:
        print("\n=== No canonical phone models with IDs parsed (missing size or patterns) ===")
    # --- END NEW ---

    if not any_success:
        print("No data processed.", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

# Remove old get_model_code (with storage param) and redefine below
def get_model_code(model_text: str):
    """
    Return canonical model code (e.g. 'A-15-PRO-256' or 'S-S24-ULTRA-512') extracted only if
    BOTH model (brand/family) and storage are present inside model_text.
    If storage is not present in the text, returns None (ignored).
    """
    parsed = parse_phone_model(model_text)  # do not supply external storage; must be in text
    if not parsed:
        return None
    return generate_phone_id(parsed["brand"], parsed["family"], parsed["variant"], parsed["storage"])

__all__ = ["get_model_code"]
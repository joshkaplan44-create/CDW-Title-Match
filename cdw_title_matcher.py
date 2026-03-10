"""
CDW Job Title Fuzzy Matcher
============================
Maps raw CDW job titles to a canonical Zoom persona taxonomy
using abbreviation expansion + TF-IDF cosine similarity.

Usage:
    python cdw_title_matcher.py

Inputs:
    - CDW Function Title (1).csv          : raw CDW titles (column 1)
    - Persona Values from Zoom.xlsx       : canonical titles ("Title Mapping" tab)

Output:
    - CDW Function Title - Matched with Hierarchy.csv

Requirements:
    pip install pandas numpy openpyxl
"""

import pandas as pd
import numpy as np
import re
import time
import collections

# ── Configuration ─────────────────────────────────────────────────────────────
THRESHOLD  = 60       # minimum cosine similarity score (0-100) to assign a match
BATCH_SIZE = 5000     # records per vectorization batch

CDW_FILE       = "CDW Function Title (1).csv"
CANONICAL_FILE = "Persona Values from Zoom.xlsx"
CANONICAL_SHEET = "Title Mapping"
OUTPUT_FILE    = "CDW Function Title - Matched with Hierarchy.csv"

# ── Abbreviation expansion dictionary ─────────────────────────────────────────
ABBREVS = {
    r'\bMGR\b': 'Manager',       r'\bMGMT\b': 'Management',
    r'\bDIR\b': 'Director',      r'\bVP\b': 'VP Level Exec',
    r'\bEVP\b': 'Executive VP Level Exec', r'\bSVP\b': 'Senior VP Level Exec',
    r'\bCEO\b': 'Chief Executive Officer', r'\bCOO\b': 'Chief Operations Officer',
    r'\bCFO\b': 'Chief Financial Officer', r'\bCTO\b': 'Chief Technology Officer',
    r'\bCIO\b': 'Chief Information Officer', r'\bCMO\b': 'Chief Marketing Officer',
    r'\bCHRO\b': 'Chief Human Resources Officer', r'\bCSO\b': 'Chief Security Officer',
    r'\bSNR\b': 'Senior',        r'\bSR\b': 'Senior',    r'\bJR\b': 'Junior',
    r'\bASSOC\b': 'Associate',   r'\bASST\b': 'Assistant',
    r'\bADMIN\b': 'Administrator', r'\bEXEC\b': 'Executive',
    r'\bPRES\b': 'President',    r'\bOPS\b': 'Operations',
    r'\bIT\b': 'Information Technology', r'\bIS\b': 'Information Systems',
    r'\bHR\b': 'Human Resources', r'\bPR\b': 'Public Relations',
    r'\bBD\b': 'Business Development', r'\bBIZ\b': 'Business',
    r'\bMKT\b': 'Marketing',     r'\bMKTG\b': 'Marketing',
    r'\bSLS\b': 'Sales',         r'\bACCT\b': 'Accounting',
    r'\bFIN\b': 'Finance',       r'\bENGR\b': 'Engineering', r'\bENG\b': 'Engineering',
    r'\bTECH\b': 'Technology',   r'\bDEV\b': 'Development',
    r'\bPROD\b': 'Product',      r'\bPROJ\b': 'Project',     r'\bPRJ\b': 'Project',
    r'\bPURCH\b': 'Purchasing',  r'\bSUPV\b': 'Supervisor',  r'\bSUP\b': 'Supervisor',
    r'\bCOORD\b': 'Coordinator', r'\bSPEC\b': 'Specialist',
    r'\bREP\b': 'Representative', r'\bSVC\b': 'Service',     r'\bSVCS\b': 'Services',
    r'\bCUST\b': 'Customer',     r'\bCOMM\b': 'Communications',
    r'\bINFO\b': 'Information',  r'\bINFRA\b': 'Infrastructure',
    r'\bNETWK\b': 'Network',     r'\bSYS\b': 'Systems',
    r'\bSEC\b': 'Security',      r'\bDB\b': 'Database',
    r'\bSW\b': 'Software',       r'\bHW\b': 'Hardware',
    r'\bQA\b': 'Quality Assurance', r'\bQC\b': 'Quality Control',
    r'\bR&D\b': 'Research and Development', r'\bBUS\b': 'Business',
    r'\bLOGS\b': 'Logistics',    r'\bMFG\b': 'Manufacturing', r'\bCORP\b': 'Corporate',
    r'\bINTL\b': 'International', r'\bGEN\b': 'General',
    r'\bGOVT\b': 'Government',   r'\bCLIN\b': 'Clinical',
    r'\bMED\b': 'Medical',       r'\bHLTH\b': 'Health',
    r'\bEDUC\b': 'Education',    r'\bUNIV\b': 'University',
    r'\bMNG\b': 'Manager',       r'\bMNGR\b': 'Manager',
    r'\bFAC\b': 'Facilities',    r'\bWHS\b': 'Warehouse',
    r'\bDIST\b': 'Distribution', r'\bDSGN\b': 'Design',      r'\bBRAND\b': 'Brand',
    r'\bPM\b': 'Project Manager', r'\bERP\b': 'Enterprise Resource Planning',
    r'\bCRM\b': 'Customer Relationship Management',
    r'\bBI\b': 'Business Intelligence', r'\bSEO\b': 'Search Engine Optimization',
    r'\bUI\b': 'User Interface',  r'\bUX\b': 'User Experience',
    r'\bPRINC\b': 'Principal',   r'\bCHIEF\b': 'Chief',      r'\bHEAD\b': 'Head',
}

# ── ZI hierarchy keyword rules ─────────────────────────────────────────────────
ZI_RULES = [
    ("Board Members", [
        r'\bboard\b', r'\btrustee\b', r'\bgovernor\b', r'\bchairman\b',
        r'\bchairwoman\b', r'\bchair\b',
    ]),
    ("C-Level", [
        r'\bchief\b', r'\bceo\b', r'\bcoo\b', r'\bcfo\b', r'\bcto\b', r'\bcio\b',
        r'\bcmo\b', r'\bchro\b', r'\bcso\b', r'\bcdo\b', r'\bcpo\b', r'\bciso\b',
        r'\bpresident\b', r'\bowner\b', r'\bfounder\b', r'\bco-founder\b',
        r'\bmanaging partner\b', r'\bmanaging director\b', r'\bprincipal\b',
        r'\bc level\b', r'\bc-level\b', r'\bexecutive director\b',
    ]),
    ("VP-Level", [
        r'\bvice president\b', r'\bvp\b', r'\bsvp\b', r'\bevp\b',
        r'\bsenior vice\b', r'\bavp\b', r'\bvp level\b',
    ]),
    ("Director", [
        r'\bdirector\b', r'\bhead of\b', r'\bglobal head\b',
        r'\bsenior director\b', r'\bassociate director\b',
    ]),
    ("Manager", [
        r'\bmanager\b', r'\bsupervisor\b', r'\bsuperintendent\b',
        r'\bteam lead\b', r'\bteam leader\b', r'\bcoordinator\b',
        r'\bforeman\b', r'\bsection chief\b',
    ]),
]


def expand_abbrevs(text: str) -> str:
    text = str(text).upper()
    for pat, rep in ABBREVS.items():
        text = re.sub(pat, rep, text, flags=re.IGNORECASE)
    return text


def normalize(text: str) -> str:
    text = expand_abbrevs(text).lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return ' '.join(text.split())


def get_features(text: str):
    words = text.split()
    feats = list(words)
    padded = ' ' + text + ' '
    for i in range(len(padded) - 2):
        feats.append(padded[i:i+3])
    return feats


def classify_zi(raw_title: str, canonical_title: str = None) -> str:
    """Assign a ZI hierarchy tier from canonical title or raw title keywords."""
    if canonical_title and canonical_title not in ("Not Enough Information", "", None):
        ct = canonical_title.lower()
        if "board member" in ct:     return "Board Members"
        if "c level exec" in ct:     return "C-Level"
        if "vp level exec" in ct:    return "VP-Level"
        if "director" in ct:         return "Director"
        if "manager" in ct:          return "Manager"
    text = re.sub(r'[^a-z0-9\s]', ' ', str(raw_title).lower())
    for label, patterns in ZI_RULES:
        for pat in patterns:
            if re.search(pat, text):
                return label
    return "Non-Manager"


def run_matching():
    print("=" * 60)
    print("CDW Title Matcher")
    print("=" * 60)

    # Load canonical titles
    print(f"\nLoading canonical titles from {CANONICAL_FILE}...")
    canon_df = pd.read_excel(CANONICAL_FILE, sheet_name=CANONICAL_SHEET)
    canonical_titles = [str(t).strip() for t in canon_df.iloc[:, 0].dropna() if str(t).strip()]
    canonical_norm = [normalize(t) for t in canonical_titles]
    print(f"  {len(canonical_titles)} canonical titles loaded")

    # Load CDW titles
    print(f"Loading CDW titles from {CDW_FILE}...")
    cdw_df = pd.read_csv(CDW_FILE, header=0, dtype=str)
    cdw_col = cdw_df.columns[0]
    cdw_titles = cdw_df[cdw_col].fillna('').astype(str).tolist()
    print(f"  {len(cdw_titles):,} CDW titles loaded")

    # Normalize CDW titles
    print("Normalizing titles...")
    cdw_norm = [normalize(t) for t in cdw_titles]

    # Build vocabulary from canonical titles only
    vocab = set()
    for t in canonical_norm:
        vocab.update(get_features(t))
    vocab = sorted(vocab)
    vocab_idx = {f: i for i, f in enumerate(vocab)}
    V = len(vocab)
    C = len(canonical_titles)
    print(f"  Vocabulary size: {V} features")

    # Vectorize canonical titles
    canon_mat = np.zeros((C, V), dtype=np.float32)
    for i, t in enumerate(canonical_norm):
        for feat in get_features(t):
            if feat in vocab_idx:
                canon_mat[i, vocab_idx[feat]] += 1
    norms = np.linalg.norm(canon_mat, axis=1, keepdims=True)
    norms[norms == 0] = 1
    canon_mat_T = (canon_mat / norms).T.astype(np.float32)

    # Run vectorized matching in batches
    print(f"\nRunning vectorized matching (threshold: {THRESHOLD}%)...")
    matched_titles, match_scores = [], []
    start = time.time()
    n = len(cdw_norm)

    for batch_start in range(0, n, BATCH_SIZE):
        batch = cdw_norm[batch_start: batch_start + BATCH_SIZE]
        bs = len(batch)
        batch_mat = np.zeros((bs, V), dtype=np.float32)
        for i, t in enumerate(batch):
            for feat in get_features(t):
                if feat in vocab_idx:
                    batch_mat[i, vocab_idx[feat]] += 1
        bnorms = np.linalg.norm(batch_mat, axis=1, keepdims=True)
        bnorms[bnorms == 0] = 1
        batch_mat /= bnorms
        sims = batch_mat @ canon_mat_T
        best_idx = np.argmax(sims, axis=1)
        best_scores = sims[np.arange(bs), best_idx] * 100
        for idx, score in zip(best_idx, best_scores):
            s = round(float(score), 1)
            matched_titles.append(canonical_titles[idx] if s >= THRESHOLD else "Not Enough Information")
            match_scores.append(s)
        done = batch_start + bs
        elapsed = time.time() - start
        eta = (n - done) / (done / elapsed) if done > 0 else 0
        print(f"  {done:,}/{n:,} | {elapsed:.0f}s elapsed | ETA {eta:.0f}s")

    # Add ZI hierarchy
    print("\nClassifying ZI hierarchy tiers...")
    zi_tiers = [classify_zi(r, c) for r, c in zip(cdw_titles, matched_titles)]

    # Save output
    out_df = pd.DataFrame({
        'ContactFunctionTitle': cdw_titles,
        'Canonical Title': matched_titles,
        'Match Score': match_scores,
        'ZI Hierarchy': zi_tiers,
    })
    out_df.to_csv(OUTPUT_FILE, index=False)

    # Summary
    matched_count = sum(1 for t in matched_titles if t != "Not Enough Information")
    total_time = time.time() - start
    print(f"\n{'='*60}")
    print(f"Done in {total_time:.1f}s")
    print(f"Total records   : {n:,}")
    print(f"Matched (≥{THRESHOLD}%) : {matched_count:,} ({matched_count/n*100:.1f}%)")
    print(f"Not Enough Info : {n-matched_count:,} ({(n-matched_count)/n*100:.1f}%)")

    buckets = collections.Counter()
    for s in match_scores:
        if s >= 90:   buckets['90-100'] += 1
        elif s >= 80: buckets['80-89']  += 1
        elif s >= 70: buckets['70-79']  += 1
        elif s >= 60: buckets['60-69']  += 1
        elif s >= 50: buckets['50-59']  += 1
        else:         buckets['<50']    += 1
    print("\nScore distribution:")
    for b in ['90-100', '80-89', '70-79', '60-69', '50-59', '<50']:
        print(f"  {b}: {buckets[b]:,}")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    run_matching()

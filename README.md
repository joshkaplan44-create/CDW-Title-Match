# CDW Title Match

Fuzzy matching pipeline that maps 790,226 raw CDW job titles to a canonical Zoom persona taxonomy, with ZoomInfo hierarchy classification.

## Files

| File | Description |
|------|-------------|
| `cdw_title_matcher.py` | Main matching script — run this to regenerate results |
| `CDW Function Title - Matched with Hierarchy.csv` | Full output: 790k records with canonical title, match score, and ZI hierarchy tier |
| `CDW Title Matching Dashboard.html` | Interactive dashboard — open in any browser |
| `CDW Title Matching - Methodology & Results.docx` | Word doc summarizing methodology, results, and guidance |

## How It Works

1. **Abbreviation expansion** — 80+ common business title abbreviations expanded (MGR → Manager, IT → Information Technology, etc.)
2. **Normalization** — lowercase, punctuation removed
3. **TF-IDF cosine similarity** — vectorized matching against 745 canonical titles using character trigrams + word unigrams
4. **Threshold filtering** — scores ≥ 60% assigned a canonical title; below 60% → "Not Enough Information"
5. **ZI hierarchy classification** — each record assigned to Board Members / C-Level / VP-Level / Director / Manager / Non-Manager

## Results Summary

| Metric | Value |
|--------|-------|
| Total records | 790,226 |
| Matched (≥60%) | 267,816 (33.9%) |
| Not Enough Information | 522,410 (66.1%) |
| Processing time | ~9 seconds |

## ZI Hierarchy Distribution

| Tier | Count | % |
|------|-------|---|
| Board Members | 2,551 | 0.3% |
| C-Level | 24,901 | 3.2% |
| VP-Level | 27,503 | 3.5% |
| Director | 84,343 | 10.7% |
| Manager | 187,255 | 23.7% |
| Non-Manager | 463,673 | 58.7% |

## Requirements

```bash
pip install pandas numpy openpyxl
```

## Usage

Place `CDW Function Title (1).csv` and `Persona Values from Zoom.xlsx` in the same directory, then:

```bash
python cdw_title_matcher.py
```

Output is written to `CDW Function Title - Matched with Hierarchy.csv`.

---
*Generated March 2026*

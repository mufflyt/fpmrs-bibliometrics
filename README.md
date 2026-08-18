# URPS Bibliometrics Pipeline

Manuscript-ready longitudinal bibliometric analysis for Urogynecology and
Reconstructive Pelvic Surgery (URPS) literature. Supports PubMed API, Web of
Science, and Scopus data sources.

The subspecialty was renamed from Female Pelvic Medicine and Reconstructive
Surgery (FPMRS) to URPS effective 2024. Use "URPS" in new code, figures, and
prose; "FPMRS" survives only where the historical name is the subject (the
nomenclature-transition analysis and the search query, which must match both
eras).

## Dependencies

Hard dependencies — needed to source and run the core analysis:

```r
install.packages(c(
  "assertthat", "digest", "dplyr", "ggplot2", "jsonlite",
  "purrr", "scales", "stringr", "tibble", "tidyr"
))
```

Optional packages enable specific stages (PubMed/OpenAlex retrieval, spatial
figures, Bayesian models). The full list is declared in `DESCRIPTION` under
`Suggests`; install what a given analysis needs:

```r
# Retrieval + enrichment
install.packages(c("rentrez", "pubmedR", "openalexR", "bibliometrix"))
# Multi-panel figures and PDF text checks
install.packages(c("patchwork", "pdftools"))
```

`DESCRIPTION` exists to declare this dependency surface for reproducible
installs and CI. The project is consumed by sourcing the script, not via
`library()`.

`genderizeR` is archived on CRAN and is therefore not declared in
`DESCRIPTION` — listing it would make the dependency set unresolvable.
`infer_gender_cascade()` guards it and defaults to `use_genderizeR = FALSE`.
Install it from the archive only if you need that fallback:

```r
remotes::install_version("genderizeR", version = "2.1.1")
```

## Usage

```r
source("R/fpmrs_bibliometrics_pipeline.R")

result <- run_fpmrs_bibliometric_pipeline(
  data_source  = "pubmed",
  pubmed_query = get_subspecialty_pubmed_query("fpmrs"),
  output_dir   = "output",
  year_start   = 1975L,
  year_end     = 2024L
)
```

Every figure is written twice: `figure_format` (PDF by default, vector art
for typesetting) plus the companions in `additional_formats` (JPEG by
default, 300 dpi at quality 95 on an opaque white background, for slides,
email, and submission portals that reject PDFs). Supported formats are
`pdf`, `png`, `svg`, `jpeg`, `jpg`, and `tiff`.

```r
# PDF only
run_fpmrs_bibliometric_pipeline(..., additional_formats = character(0))

# PNG and TIFF alongside the PDFs
run_fpmrs_bibliometric_pipeline(..., additional_formats = c("png", "tiff"))
```

See function documentation within the pipeline file for detailed usage of
individual analysis steps.

## Tests

The suite runs entirely offline. Live-API tests are opt-in.

```r
testthat::test_dir("tests/testthat")
```

Synthetic corpora (`generate_synthetic_bibliography()`) stand in for PubMed so
the analysis stages can be exercised without network access. To additionally
run the live API contract tests:

```sh
FPMRS_RUN_NETWORK_TESTS=true Rscript -e 'testthat::test_dir("tests/testthat")'
```

What the suite deliberately covers:

- **Record accounting** — year-range and language exclusions must stay
  separable. A prior bug reported "0 non-English excluded" while 7,120 records
  were in fact removed, because both filters ran in one call and the caller
  derived counts from row totals.
- **Caption integrity** — figures are rendered to real PDFs and the caption
  text is extracted and compared. ggplot2 does not wrap captions, so an
  over-long one is silently clipped at the device edge: the plot object looks
  correct while the published figure is missing words.
- **Abstract prose** — subspecialty naming, journal-style P values, title
  casing of journals and author initials, and conditional sentences that must
  not overclaim (for example, asserting "heavily concentrated" authorship when
  the concentration index is low).
- **Query stability** — the PubMed query defines the corpus, so its hash is
  pinned against a committed baseline.

## Manuscript

`manuscript/urps_bibliometrics_manuscript.Rmd` renders the whole
manuscript — generated abstract, figures, tables, and results prose — from
the analysis object.

```sh
Rscript manuscript/render.R            # HTML
Rscript manuscript/render.R word       # Word .docx
Rscript manuscript/render.R both
Rscript manuscript/render.R both --refresh   # re-run the pipeline first
```

For journal submission:

```sh
Rscript manuscript/render.R journal   # -> output/URPS_bibliometrics_manuscript.docx
```

`manuscript/urps_journal_manuscript.Rmd` is the submission draft formatted for
*Urogynecology*: a structured abstract (Importance / Objectives / Study Design
/ Results / Conclusions) under the 250-word limit, IMRaD sections, numbered
references via `references.bib` + `urogynecology.csl`, and a "Poster bullets"
appendix meant to be lifted onto a poster and deleted before submission. Every
number in the prose is an inline R expression, so the text cannot drift from
the analysis.

Two caveats on the references:

- Clinical citations were pulled **from the analysed corpus itself**, so each
  carries the PMID and DOI of a record the search actually retrieved. Author
  lists and page ranges come from the PubMed export and have not been checked
  against publisher records — verify every entry before submission.
- `urogynecology.csl` is an AMA-style numeric approximation, not the journal's
  official style. To use the canonical AMA style, drop
  `american-medical-association.csl` from the
  [CSL styles repository](https://github.com/citation-style-language/styles)
  into `manuscript/` and point the Rmd's `csl:` field at it.

Without `--refresh` it reuses the cached analysis at
`output/pipeline_result.rds`. That file is gitignored, so a fresh clone runs
the full pipeline (PubMed fetch, ~30 min) on the first render.

Parameters (`params:` in the YAML header, overridable via
`rmarkdown::render(params = ...)`):

| Parameter | Default | Purpose |
| --- | --- | --- |
| `results_rds` | `output/pipeline_result.rds` | Cached analysis object |
| `refresh` | `FALSE` | Re-run the pipeline instead of loading the cache |
| `year_start` / `year_end` | 1975 / 2024 | Analysis window |
| `focal` | `URPS` | Focal subspecialty |
| `top_n` | 10 | Rows in the journal and author tables |

Two things worth knowing:

- **Equity is recomputed at knit time** rather than read from the cached
  object, so a cache written before the ISO-2 income-tier fix cannot carry
  stale income shares into the manuscript.
- **With only one corpus loaded, every rank is 1 by construction.** The
  document prints a banner saying so. The comparative sentences (cohort
  ranking, urology contrast, evidence-quality benchmarking) need the
  comparator subspecialties run through `run_subspecialty_comparison()`.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | What it protects |
| --- | --- |
| `source-check` | The script must source with **hard dependencies only**. Fails if a `Suggests`-only package (brms, sf, tigris…) leaks into top-level code. |
| `lint` | Correctness-focused linters (see `.lintr.R`). Not a style gate. |
| `test` | Full offline suite on R release + oldrel-1, Ubuntu and macOS. Uploads a JUnit report. |
| `figures` | Renders all figures from a synthetic corpus and uploads them as artifacts, so a reviewer can see a PR's visual impact. Fails on clipped captions. |
| `query-stability` | Fails if a subspecialty query changed without its baseline being updated. |

`.github/workflows/network.yml` runs weekly (and on demand) against NCBI and
OpenAlex to catch upstream contract drift. It never gates a pull request — a
rate-limited external service must not block a merge.

### Changing the PubMed query

A query change redefines the corpus and invalidates every published number, so
it is gated deliberately:

```sh
Rscript inst/ci/check_query_stability.R --update   # accept the new query
```

Commit the updated `inst/ci/query-baseline.json` in the same change, and re-run
the pipeline so published outputs match the new corpus.

## Author

Tyler Muffly, MD

## License

MIT

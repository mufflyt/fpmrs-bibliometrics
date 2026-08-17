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

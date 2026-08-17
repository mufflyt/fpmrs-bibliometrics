# Source the pipeline once for the whole test run.
#
# The project is a single sourced script, not an installed package, so
# tests cannot rely on library(fpmrs.bibliometrics). We locate the script
# relative to this helper and source it into the global environment, which
# is where the pipeline itself expects its functions to live.

.pipeline_script <- local({
  candidates <- c(
    "../../R/fpmrs_bibliometrics_pipeline.R",  # tests/testthat/ -> repo root
    "../R/fpmrs_bibliometrics_pipeline.R",
    "R/fpmrs_bibliometrics_pipeline.R"
  )
  hit <- candidates[file.exists(candidates)]
  if (length(hit) == 0L) {
    stop("Cannot locate R/fpmrs_bibliometrics_pipeline.R from ", getwd())
  }
  normalizePath(hit[[1L]])
})

# Sourcing must not emit warnings or messages that would mask real ones,
# but it also must not silently swallow a genuine parse/eval failure.
local({
  ok <- tryCatch({
    suppressWarnings(suppressMessages(
      sys.source(.pipeline_script, envir = globalenv())
    ))
    TRUE
  }, error = function(e) {
    stop("Sourcing the pipeline failed: ", conditionMessage(e), call. = FALSE)
  })
  stopifnot(isTRUE(ok))
})

# ---- Skip helpers -------------------------------------------------------

skip_if_no_pkg <- function(...) {
  pkgs <- c(...)
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1L), quietly = TRUE)]
  if (length(missing) > 0L) {
    testthat::skip(paste("missing package(s):", paste(missing, collapse = ", ")))
  }
  invisible(TRUE)
}

# Network tests are opt-in. CI sets FPMRS_RUN_NETWORK_TESTS=true only in the
# scheduled workflow, so pull-request runs never depend on NCBI/OpenAlex
# uptime.
skip_if_offline_suite <- function() {
  if (!identical(tolower(Sys.getenv("FPMRS_RUN_NETWORK_TESTS", "false")), "true")) {
    testthat::skip("network suite disabled (set FPMRS_RUN_NETWORK_TESTS=true)")
  }
  invisible(TRUE)
}

# Create a throwaway output directory that is removed when the calling
# test exits. Avoids depending on withr being attached.
withr_tempdir <- function(envir = parent.frame()) {
  path <- file.path(tempdir(), paste0("fpmrs-", basename(tempfile(""))))
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  withr::defer(unlink(path, recursive = TRUE), envir = envir)
  path
}

# ---- Shared fixtures ----------------------------------------------------

# Annual trends frame matching the contract of plot_annual_publications()
# and plot_citation_trends().
fixture_annual_trends <- function(year_start = 1975L, year_end = 2024L) {
  n <- year_end - year_start + 1L
  data.frame(
    publication_year  = seq(year_start, year_end),
    publication_count = round(seq(100, 2700, length.out = n)),
    total_citations   = round(seq(3000, 7000, length.out = n)),
    mean_citations    = seq(22, 5, length.out = n)
  )
}

fixture_country_trends <- function() {
  out <- expand.grid(
    publication_year = 2000:2024,
    country          = c("USA", "CHINA", "ITALY", "UNITED KINGDOM"),
    stringsAsFactors = FALSE
  )
  out$publication_count <- seq_len(nrow(out)) %% 17L + 1L
  out
}

fixture_journal_trends <- function() {
  out <- expand.grid(
    publication_year = 2000:2024,
    journal          = c(
      "THE JOURNAL OF UROLOGY",
      "AMERICAN JOURNAL OF OBSTETRICS AND GYNECOLOGY",
      "INTERNATIONAL UROGYNECOLOGY JOURNAL",
      "EUROPEAN JOURNAL OF OBSTETRICS, GYNECOLOGY, AND REPRODUCTIVE BIOLOGY"
    ),
    stringsAsFactors = FALSE
  )
  out$publication_count <- seq_len(nrow(out)) %% 23L + 1L
  out
}

fixture_keyword_trends <- function() {
  out <- expand.grid(
    publication_year = 2000:2024,
    keyword          = c(
      "PELVIC FLOOR", "QUALITY OF LIFE", "PELVIC ORGAN PROLAPSE",
      "URINARY INCONTINENCE"
    ),
    stringsAsFactors = FALSE
  )
  out$keyword_count <- seq_len(nrow(out)) %% 11L + 1L
  out
}

# Minimal comparison table accepted by generate_abstract_results_text().
fixture_comparison_table <- function() {
  tibble::tibble(
    subspecialty             = c("URPS", "Urology", "MFM"),
    total_documents          = c(10000L, 45000L, 20000L),
    total_citations          = c(300000L, 900000L, 500000L),
    mean_citations_per_paper = c(30, 20, 25),
    cagr_pct                 = c(6.6, 4.1, 3.2),
    unique_countries         = c(120L, 140L, 100L),
    top_country              = c("USA", "USA", "USA"),
    top_country_pct          = c(33.5, 40.1, 38.0),
    top_author               = c("DIETZ HP", "SMITH J A", "DOE J"),
    top_author_pubs          = c(348L, 500L, 200L),
    top_journal_by_citations = c(
      "THE JOURNAL OF UROLOGY", "J UROL", "AJOG"
    ),
    rank_by_volume           = c(3L, 1L, 2L),
    rank_by_citations        = c(3L, 1L, 2L),
    rank_by_impact           = c(1L, 3L, 2L),
    rank_by_cagr             = c(1L, 2L, 3L),
    mean_authors_per_paper   = c(5.2, 4.8, 6.1),
    rank_by_collaboration    = c(2L, 3L, 1L)
  )
}

# Render a ggplot/patchwork object to PDF and return its extracted text as
# a single normalised string. Used to prove captions are not clipped at the
# device edge -- ggplot2 does not wrap captions, so an over-long one is
# silently truncated in the output file.
render_pdf_text <- function(plot, width = 7, height = 5) {
  skip_if_no_pkg("pdftools")
  path <- tempfile(fileext = ".pdf")
  on.exit(unlink(path), add = TRUE)
  # The base pdf device warns when substituting non-Latin-1 glyphs (en
  # dashes in subtitles). That substitution is harmless here because
  # normalise_text() folds all dash variants together, and the warning
  # would otherwise fail the suite under options(warn = 2).
  suppressWarnings(suppressMessages(ggplot2::ggsave(
    filename = path, plot = plot,
    width = width, height = height, device = "pdf"
  )))
  normalise_text(paste(pdftools::pdf_text(path), collapse = " "))
}

# PDF rendering turns hyphens into Unicode minus signs and reflows
# whitespace, so compare on a normalised form.
normalise_text <- function(x) {
  x <- gsub("−", "-", x)   # Unicode minus -> ASCII hyphen
  x <- gsub("–", "-", x)   # en dash
  x <- gsub("—", "-", x)   # em dash
  x <- gsub("[[:space:]]+", " ", x)
  trimws(x)
}

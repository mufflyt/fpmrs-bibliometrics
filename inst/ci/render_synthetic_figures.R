#!/usr/bin/env Rscript
# Render the manuscript figures from a synthetic corpus.
#
# Run in CI so every pull request produces inspectable artifacts without
# touching PubMed or OpenAlex. Also acts as an integration smoke test: any
# error in the filter -> trends -> plot -> ggsave chain fails the build.

suppressWarnings(suppressMessages(
  sys.source("R/fpmrs_bibliometrics_pipeline.R", envir = globalenv())
))

out_dir <- "ci-figures"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

message("Generating synthetic corpus ...")
bib <- generate_synthetic_bibliography(
  n_papers         = 2000L,
  n_subspecialties = 4L,
  year_start       = 1990L,
  year_end         = 2024L,
  seed             = 42L,
  verbose          = FALSE
)

filtered <- .standardize_and_filter_bibliography(
  bibliography_raw = bib,
  year_start       = 1990L,
  year_end         = 2024L,
  english_only     = TRUE,
  verbose          = FALSE
)

counts <- attr(filtered, "filter_counts")
message(sprintf(
  "Corpus: %d retrieved -> %d after year -> %d after language",
  counts$n_retrieved, counts$n_after_year_filter, counts$n_after_lang_filter
))

trends <- .compute_annual_publication_trends(filtered, verbose = FALSE)

# Derive the per-journal and per-keyword frames the figures expect.
journal_trends <- as.data.frame(
  dplyr::summarise(
    dplyr::group_by(filtered, publication_year, journal = SO),
    publication_count = dplyr::n(), .groups = "drop"
  )
)

keyword_trends <- as.data.frame(
  dplyr::summarise(
    dplyr::group_by(filtered, publication_year, keyword = toupper(DE)),
    keyword_count = dplyr::n(), .groups = "drop"
  )
)

figures <- list(
  annual_publications = plot_annual_publications(
    trends, 1990L, 2024L, verbose = FALSE
  ),
  citation_trends = plot_citation_trends(
    trends, 1990L, 2024L, verbose = FALSE
  ),
  journal_trends = plot_journal_trends(
    journal_trends, top_n_journals = 6L, verbose = FALSE
  ),
  keyword_evolution = plot_keyword_evolution(
    keyword_trends, top_n_keywords = 12L, verbose = FALSE
  )
)

# Caption integrity: ggplot2 silently clips captions wider than the device,
# so verify each one survives the render rather than trusting the object.
check_caption <- function(name, plot, path) {
  cap <- plot$labels$caption
  if (is.null(cap) || !nzchar(cap)) return(invisible(TRUE))
  if (!requireNamespace("pdftools", quietly = TRUE)) return(invisible(TRUE))

  norm <- function(x) {
    x <- gsub("−|–|—", "-", x)
    trimws(gsub("[[:space:]]+", " ", x))
  }
  rendered <- norm(paste(pdftools::pdf_text(path), collapse = " "))
  if (!grepl(norm(cap), rendered, fixed = TRUE)) {
    stop(sprintf(
      "Caption clipped in %s.\n  expected: %s", name, norm(cap)
    ), call. = FALSE)
  }
  invisible(TRUE)
}

# Route through the pipeline's own saver so CI exercises the real export
# path (primary format plus companions) rather than a parallel ggsave call.
paths <- suppressWarnings(.save_all_figures(
  figures_list       = figures,
  output_dir         = out_dir,
  figure_format      = "pdf",
  figure_width       = 7,
  figure_height      = 5,
  verbose            = FALSE,
  additional_formats = "jpeg"
))

for (nm in names(figures)) {
  check_caption(nm, figures[[nm]], paths[[nm]])
}

companions <- attr(paths, "companion_paths")
message(sprintf(
  "Wrote %d figures as pdf + %s, all captions intact.",
  length(paths), paste(names(companions), collapse = ", ")
))
for (f in sort(list.files(out_dir))) message("  ", f)

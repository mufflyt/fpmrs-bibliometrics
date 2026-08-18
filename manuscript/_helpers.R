# Helpers used by the manuscript Rmd.
#
# Kept out of the Rmd so the document stays readable and so these can be
# unit-tested or reused by other reports.

# Load a cached pipeline result, or run the pipeline if asked/absent.
load_or_run_analysis <- function(results_rds,
                                 refresh    = FALSE,
                                 year_start = 1975L,
                                 year_end   = 2024L,
                                 output_dir = "output",
                                 verbose    = FALSE) {
  if (!refresh && file.exists(results_rds)) {
    message("Loading cached analysis: ", results_rds)
    return(readRDS(results_rds))
  }
  message("Running the full pipeline (this fetches from PubMed and may take ~30 min) ...")
  res <- run_fpmrs_bibliometric_pipeline(
    data_source  = "pubmed",
    pubmed_query = get_subspecialty_pubmed_query("fpmrs"),
    output_dir   = output_dir,
    year_start   = year_start,
    year_end     = year_end,
    verbose      = verbose
  )
  saveRDS(res, results_rds)
  res
}

# Build the single-subspecialty comparison row the abstract generator needs.
#
# run_subspecialty_comparison() builds this across several corpora; with one
# corpus we assemble the focal row directly so the non-comparative sentences
# (corpus size, citations, growth, trend, geography, equity) are still
# generated from real data. Ranks are 1 by construction and the comparative
# sentences degrade accordingly -- see the caveat printed by
# comparative_sentences_available().
build_focal_row <- function(res, focal = "URPS") {
  co <- res$country_summary
  co <- co[!is.na(co$country) & !toupper(co$country) %in% c("NA", "NULL") &
             nzchar(co$country), ]

  total_cit <- sum(res$annual_trends$total_citations, na.rm = TRUE)
  n_docs    <- nrow(res$bibliography)

  tibble::tibble(
    subspecialty             = focal,
    total_documents          = n_docs,
    total_citations          = total_cit,
    mean_citations_per_paper = round(total_cit / max(n_docs, 1L), 1),
    cagr_pct                 = res$growth_metrics$cagr_pct,
    unique_countries         = dplyr::n_distinct(co$country),
    top_country              = if (nrow(co)) co$country[[1L]] else NA_character_,
    top_country_pct          = if (nrow(co)) co$pct_of_total[[1L]] else NA_real_,
    top_author               = res$author_metrics$author[[1L]],
    top_author_pubs          = res$author_metrics$publication_count[[1L]],
    top_journal_by_citations = res$journal_citations$journal[[1L]],
    rank_by_volume           = 1L,
    rank_by_citations        = 1L,
    rank_by_impact           = 1L,
    rank_by_cagr             = 1L,
    mean_authors_per_paper   = res$authorship_metrics$mean_authors_per_paper,
    rank_by_collaboration    = 1L
  )
}

comparative_sentences_available <- function(comparison_table) {
  nrow(comparison_table) >= 2L
}

# Country table with World Bank income tier attached.
country_table <- function(res, top_n = 15L) {
  co <- res$country_summary
  co <- co[!is.na(co$country) & !toupper(co$country) %in% c("NA", "NULL") &
             nzchar(co$country), ]
  nm <- .normalize_country_string(toupper(co$country))
  data.frame(
    Country      = .title_case_journal(nm),
    Code         = co$country,
    `Income tier` = .wb_income_tier(nm),
    Publications = co$publication_count,
    `% of corpus` = round(co$pct_of_total, 1),
    check.names   = FALSE,
    stringsAsFactors = FALSE
  )[seq_len(min(top_n, nrow(co))), ]
}

journal_table <- function(res, top_n = 10L) {
  j <- res$journal_citations
  data.frame(
    Journal            = .title_case_journal(j$journal),
    Publications       = j$publication_count,
    `Total citations`  = j$total_citations,
    `Mean citations`   = round(j$mean_citations, 1),
    `Median citations` = j$median_citations,
    check.names = FALSE, stringsAsFactors = FALSE
  )[seq_len(min(top_n, nrow(j))), ]
}

author_table <- function(res, top_n = 10L) {
  a <- res$author_metrics
  out <- data.frame(
    Author             = a$author,
    Publications       = a$publication_count,
    `Total citations`  = a$total_citations,
    check.names = FALSE, stringsAsFactors = FALSE
  )
  if ("first_author_count" %in% names(a)) {
    out$`First-author papers` <- a$first_author_count
  }
  out[seq_len(min(top_n, nrow(out))), ]
}

# CONSORT record flow, read back from the artifact the pipeline writes so the
# manuscript and the provenance file can never disagree.
consort_table <- function(path = "output/consort_flow.json") {
  if (!file.exists(path)) return(NULL)
  fl <- jsonlite::fromJSON(path)
  data.frame(
    Stage   = vapply(fl, function(x) x$label, character(1L)),
    Records = vapply(fl, function(x) as.integer(x$n), integer(1L)),
    check.names = FALSE, stringsAsFactors = FALSE, row.names = NULL
  )
}

equity_table <- function(eq) {
  data.frame(
    `Income group` = c("High income", "Upper-middle income",
                       "Lower-middle income", "Low income", "Unmapped"),
    `% of publications` = c(eq$pct_high_income, eq$pct_upper_mid,
                            eq$pct_lower_mid, eq$pct_low_income,
                            eq$pct_unmapped),
    check.names = FALSE, stringsAsFactors = FALSE
  )
}

# Render a data frame as a table appropriate to the output format.
#
# kable() is used for every format: pandoc turns its markdown into a real
# Word table. flextable was tried first and silently flattened into text
# ("header1StageRecordsbody1...") in the .docx, producing zero <w:tbl>
# elements. kableExtra styling is HTML/LaTeX-only, so it is layered on
# conditionally rather than being required.
manuscript_table <- function(df, caption) {
  tbl <- knitr::kable(
    df, caption = caption, booktabs = TRUE, row.names = FALSE,
    format.args = list(big.mark = ",")
  )
  if (knitr::is_html_output() || knitr::is_latex_output()) {
    tbl <- kableExtra::kable_styling(
      tbl,
      bootstrap_options = c("striped", "condensed"),
      full_width = FALSE, position = "left"
    )
  }
  tbl
}

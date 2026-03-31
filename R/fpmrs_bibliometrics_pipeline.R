# ============================================================
# fpmrs_bibliometrics_pipeline.R
#
# Manuscript-ready longitudinal bibliometric analysis for
# Female Pelvic Medicine and Reconstructive Surgery (FPMRS)
# literature. Supports PubMed API, Web of Science, and Scopus.
#
# Author: Tyler Muffly, MD
# ============================================================


# ============================================================
# INTERNAL HELPERS  (@noRd -- not exported)
# ============================================================

#' @noRd
.log_step <- function(msg, verbose) {
  if (isTRUE(verbose)) message(msg)
  invisible(NULL)
}

#' @noRd
.log_input <- function(label, value, verbose) {
  if (isTRUE(verbose)) {
    display_value <- if (is.null(value)) {
      "NULL"
    } else {
      pasted <- paste(as.character(value), collapse = ", ")
      if (nchar(pasted) > 80) paste0(substr(pasted, 1, 77), "...") else pasted
    }
    message(sprintf("  [INPUT]  %-22s: %s", label, display_value))
  }
  invisible(NULL)
}

#' @noRd
.validate_pipeline_inputs <- function(
    data_source,
    file_path,
    pubmed_query,
    output_dir,
    year_start,
    year_end,
    figure_format,
    figure_width,
    figure_height,
    verbose
) {
  assertthat::assert_that(assertthat::is.string(data_source))
  assertthat::assert_that(
    data_source %in% c("pubmed", "wos", "scopus", "both"),
    msg = paste(
      "`data_source` must be one of: 'pubmed', 'wos', 'scopus', 'both'.",
      "Received:", data_source
    )
  )
  assertthat::assert_that(
    isTRUE(verbose) || isFALSE(verbose),
    msg = paste(
      "`verbose` must be TRUE or FALSE (not NA or NULL).",
      "Received:", deparse(verbose)
    )
  )
  assertthat::assert_that(assertthat::is.string(output_dir))
  assertthat::assert_that(
    nchar(stringr::str_trim(output_dir)) > 0,
    msg = "`output_dir` must be a non-empty string."
  )
  assertthat::assert_that(
    is.numeric(year_start) && length(year_start) == 1L,
    msg = "`year_start` must be a single numeric value."
  )
  assertthat::assert_that(
    year_start >= 1946L,
    msg = paste(
      "`year_start` must be >= 1946 (MEDLINE indexing began 1946).",
      "Received:", year_start
    )
  )
  assertthat::assert_that(
    year_start == as.integer(year_start),
    msg = sprintf(
      "`year_start` must be a whole number. Received: %g", year_start
    )
  )
  assertthat::assert_that(
    is.numeric(year_end) && length(year_end) == 1L,
    msg = "`year_end` must be a single numeric value."
  )
  assertthat::assert_that(
    year_end == as.integer(year_end),
    msg = sprintf(
      "`year_end` must be a whole number. Received: %g", year_end
    )
  )
  assertthat::assert_that(
    year_start < year_end,
    msg = sprintf(
      "`year_start` (%d) must be less than `year_end` (%d).",
      year_start, year_end
    )
  )
  # Allow year_end up to current year + 1 (for in-progress year corpus).
  assertthat::assert_that(
    year_end <= as.integer(format(Sys.Date(), "%Y")) + 1L,
    msg = sprintf(
      "`year_end` (%d) cannot exceed next year (%s).",
      year_end, as.integer(format(Sys.Date(), "%Y")) + 1L
    )
  )
  assertthat::assert_that(assertthat::is.string(figure_format))
  assertthat::assert_that(
    figure_format %in% c("pdf", "png", "svg"),
    msg = paste(
      "`figure_format` must be one of: 'pdf', 'png', 'svg'.",
      "Received:", figure_format
    )
  )
  assertthat::assert_that(assertthat::is.number(figure_width))
  assertthat::assert_that(
    is.finite(figure_width) && figure_width > 0,
    msg = paste("`figure_width` must be a finite positive number.",
                "Received:", figure_width)
  )
  assertthat::assert_that(assertthat::is.number(figure_height))
  assertthat::assert_that(
    is.finite(figure_height) && figure_height > 0,
    msg = paste("`figure_height` must be a finite positive number.",
                "Received:", figure_height)
  )

  if (data_source %in% c("wos", "scopus", "both")) {
    assertthat::assert_that(
      !is.null(file_path),
      msg = paste(
        "`file_path` is required when `data_source` is 'wos' or 'scopus'.",
        "Received NULL."
      )
    )
    assertthat::assert_that(assertthat::is.string(file_path))
    assertthat::assert_that(
      nchar(stringr::str_trim(file_path)) > 0,
      msg = "`file_path` must be a non-empty string."
    )
    assertthat::assert_that(
      file.exists(file_path),
      msg = sprintf("File not found at path: '%s'", file_path)
    )
  }

  if (data_source %in% c("pubmed", "both")) {
    assertthat::assert_that(
      !is.null(pubmed_query),
      msg = paste(
        "`pubmed_query` is required when `data_source` is 'pubmed'.",
        "Received NULL."
      )
    )
    assertthat::assert_that(assertthat::is.string(pubmed_query))
    assertthat::assert_that(
      nchar(stringr::str_trim(pubmed_query)) > 0,
      msg = "`pubmed_query` must be a non-empty string."
    )
  }

  # Warn if reproducibility packages unavailable
  for (repro_pkg in c("jsonlite", "digest")) {
    if (!requireNamespace(repro_pkg, quietly = TRUE)) {
      warning(sprintf(
        paste("Package '%s' not installed.",
              "Reproducibility artefacts (features 1-7) will be skipped."),
        repro_pkg
      ), call. = FALSE)
    }
  }
  invisible(TRUE)
}

#' @noRd
#' Build a data-driven era list from the corpus year window.
#'
#' Divides [year_start, year_end] into up to 4 eras that reflect
#' meaningful bibliometric periods in the OB/GYN literature:
#'   - Pre-digital: before 1990 (pre-MEDLINE indexing at scale)
#'   - Early digital: 1990-1999 (MEDLINE widespread, laparoscopic era)
#'   - Modern: 2000-2012 (PubMed free access, evidence-based medicine)
#'   - Contemporary: 2013-present (open access, RCT proliferation)
#'
#' When year_start >= 2000 the pre-digital and early-digital eras are
#' collapsed, preserving the familiar 4-era scheme.
#' When year_start < 1990 a 5-era scheme is returned.
#'
#' @param year_start Integer first year of corpus.
#' @param year_end   Integer last year of corpus.
#' @return A named list suitable for the `eras` argument of
#'   .compute_thematic_evolution(), .compute_evidence_evolution(), etc.
make_eras <- function(year_start, year_end) {
  assertthat::assert_that(
    is.numeric(year_start) && is.numeric(year_end),
    msg = "`year_start` and `year_end` must be numeric."
  )
  # Single-year corpora (year_start == year_end) are valid for smoke tests
  # and corpora that span exactly one calendar year. Return a 1-era list.
  if (year_start >= year_end) {
    single_label <- sprintf("%d", as.integer(year_start))
    return(setNames(list(c(as.integer(year_start), as.integer(year_end))),
                    single_label))
  }

  ys <- as.integer(year_start)
  ye <- as.integer(year_end)

  # Anchor years for era boundaries
  DIGITAL_BREAK    <- 1990L   # MEDLINE/PubMed at scale
  MODERN_BREAK     <- 2000L   # PubMed free access + EBM
  CONTEMP_BREAK    <- 2013L   # Open access + RCT surge

  if (ys >= CONTEMP_BREAK) {
    # Short window: single era spanning the whole range
    eras <- list()
    eras[[sprintf("%d+", ys)]] <- c(ys, ye)
    return(eras)
  }

  if (ys >= MODERN_BREAK) {
    # 2000-2012 / 2013+
    eras <- list()
    mid  <- min(CONTEMP_BREAK - 1L, ye)
    eras[[sprintf("%d\u2013%d", ys, mid)]] <- c(ys, mid)
    if (ye >= CONTEMP_BREAK) {
      eras[[sprintf("%d+", CONTEMP_BREAK)]] <- c(CONTEMP_BREAK, ye)
    }
    # Add 2000-2007 / 2008-2012 sub-split if range wide enough
    if (ye - ys >= 12L) {
      mid2 <- 2007L
      eras <- list()
      eras[["2000\u20132007"]] <- c(2000L, 2007L)
      eras[["2008\u20132012"]] <- c(2008L, 2012L)
      eras[["2013\u20132018"]] <- c(2013L, 2018L)
      eras[["2019+"]]            <- c(2019L, ye)
    }
    return(eras)
  }

  if (ys >= DIGITAL_BREAK) {
    # 1990s + 2000-2012 + 2013+
    eras <- list()
    eras[[sprintf("%d\u20131999", ys)]] <- c(ys, 1999L)
    eras[["2000\u20132007"]]            <- c(2000L, 2007L)
    eras[["2008\u20132012"]]            <- c(2008L, 2012L)
    if (ye >= 2013L) {
      eras[["2013\u20132018"]] <- c(2013L, 2018L)
    }
    if (ye >= 2019L) {
      eras[["2019+"]] <- c(2019L, ye)
    }
    return(eras)
  }

  # Pre-1990 start: full 5-era scheme
  eras <- list()
  eras[[sprintf("%d\u20131989", ys)]]  <- c(ys,    1989L)
  eras[["1990\u20131999"]]             <- c(1990L, 1999L)
  eras[["2000\u20132007"]]             <- c(2000L, 2007L)
  eras[["2008\u20132018"]]             <- c(2008L, 2018L)
  if (ye >= 2019L) {
    eras[["2019+"]] <- c(2019L, ye)
  }
  return(eras)
}


#' @noRd
.merge_bibliographies <- function(bib_pubmed, bib_wos, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bib_pubmed))
  assertthat::assert_that(is.data.frame(bib_wos))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  n_pm  <- nrow(bib_pubmed)
  n_wos <- nrow(bib_wos)
  .log_step(sprintf(
    "[MERGE] PubMed: %d records | WoS: %d records", n_pm, n_wos
  ), verbose)

  bib_pubmed[["bib_source_db"]] <- "pubmed"
  bib_wos[["bib_source_db"]]    <- "wos"

  if (n_wos == 0L) {
    .log_step("[MERGE] WoS corpus empty -- returning PubMed records only.",
              verbose)
    return(bib_pubmed)
  }

  # ---- Step 1: DOI-based deduplication ----
  get_doi <- function(bib) {
    if ("DI" %in% names(bib)) toupper(trimws(bib[["DI"]]))
    else rep(NA_character_, nrow(bib))
  }
  doi_pm  <- get_doi(bib_pubmed)
  doi_wos <- get_doi(bib_wos)

  doi_pm_valid   <- doi_pm[!is.na(doi_pm) & nchar(doi_pm) > 0L]
  dup_doi_in_wos <- (!is.na(doi_wos)) & (nchar(doi_wos) > 0L) &
                    (doi_wos %in% doi_pm_valid)
  n_doi_dups <- sum(dup_doi_in_wos)
  if (n_doi_dups > 0L) {
    .log_step(sprintf("[MERGE] DOI dedup: removed %d WoS duplicates",
                      n_doi_dups), verbose)
  }
  bib_wos_deduped <- bib_wos[!dup_doi_in_wos, , drop = FALSE]

  # ---- Step 2: Title+Year fallback deduplication ----
  # Only applies to WoS records that have NO valid DOI (to avoid
  # over-deduplication when both corpora have valid but distinct DOIs).
  .norm_title <- function(bib) {
    ti <- if ("TI" %in% names(bib)) bib[["TI"]] else
          rep(NA_character_, nrow(bib))
    py <- if ("PY" %in% names(bib)) bib[["PY"]] else
          rep(NA_character_, nrow(bib))
    # Year-independent key for title dedup:
    # epub vs print dates can differ by 1 year in the same paper.
    # Use title alone (first 40 alphanumeric chars) as the dedup key.
    toupper(trimws(substr(gsub("[^A-Za-z0-9]", "", ti), 1L, 40L)))
  }

  # Title dedup only on WoS records without a valid DOI
  wos_has_doi  <- !is.na(doi_wos[!dup_doi_in_wos]) &
                  nchar(doi_wos[!dup_doi_in_wos]) > 0L

  keys_pm  <- .norm_title(bib_pubmed)
  keys_wos <- .norm_title(bib_wos_deduped)

  # For records WITH a WoS DOI: skip title dedup (DOI is authoritative)
  # For records WITHOUT a WoS DOI: apply title+year dedup
  dup_title <- !wos_has_doi & (keys_wos %in% keys_pm[!is.na(keys_pm)])
  n_title_dups <- sum(dup_title)
  if (n_title_dups > 0L) {
    .log_step(sprintf(
      "[MERGE] Title+year dedup (DOI-less records only): removed %d WoS duplicates",
      n_title_dups), verbose)
  }
  bib_wos_unique <- bib_wos_deduped[!dup_title, , drop = FALSE]

  # ---- Step 3: Column harmonisation ----
  all_cols <- union(names(bib_pubmed), names(bib_wos_unique))
  for (cn in setdiff(all_cols, names(bib_pubmed)))
    bib_pubmed[[cn]] <- NA
  for (cn in setdiff(all_cols, names(bib_wos_unique)))
    bib_wos_unique[[cn]] <- NA

  # Coerce all shared columns to character before binding to prevent
  # type-mismatch errors when PubMed exports TC as character and
  # WoS exports it as integer (or similar cross-source type mismatches).
  # Downstream functions (e.g. .compute_citation_metrics) always
  # use suppressWarnings(as.numeric(TC)) so character storage is safe.
  .coerce_all_char <- function(df) {
    df[] <- lapply(df, function(col) {
      if (is.character(col) || is.logical(col)) col
      else as.character(col)
    })
    df
  }
  merged_bibliography <- dplyr::bind_rows(
    .coerce_all_char(bib_pubmed[, all_cols]),
    .coerce_all_char(bib_wos_unique[, all_cols])
  )

  n_unique <- nrow(merged_bibliography)
  .log_step(sprintf(
    "[MERGE] Final corpus: %d unique records (%d PubMed + %d WoS-only)",
    n_unique, n_pm, nrow(bib_wos_unique)
  ), verbose)

  merged_bibliography
}

#' @noRd
.load_bibliography <- function(
    data_source,
    file_path,
    pubmed_query,
    pubmed_api_key,
    verbose
) {
  if (data_source == "pubmed") {
    .log_step("[LOAD] Querying PubMed API ...", verbose)
    .log_step(sprintf(
      "[LOAD] Query: %s", substr(pubmed_query, 1, 100)
    ), verbose)

    pubmed_api_response <- pubmedR::pmApiRequest(
      query   = pubmed_query,
      limit   = 10000L,
      api_key = pubmed_api_key
    )
    .log_step(sprintf(
      "[LOAD] PubMed API: %s total records available.",
      pubmed_api_response$TotalCount
    ), verbose)

    pubmed_raw_df <- pubmedR::pmApi2df(pubmed_api_response)
    .log_step(sprintf(
      "[LOAD] Converted PubMed response: %d rows x %d cols",
      nrow(pubmed_raw_df), ncol(pubmed_raw_df)
    ), verbose)

    bibliography_standardized <- bibliometrix::convert2df(
      file     = pubmed_raw_df,
      dbsource = "pubmed",
      format   = "api"
    )

  } else if (data_source == "both") {
    # ---- Dual-database: PubMed API + WoS file, deduped and merged ----
    .log_step("[LOAD] Dual-database mode: PubMed API + WoS file ...", verbose)

    pubmed_both <- pubmedR::pmApiRequest(
      query = pubmed_query, limit = 10000L, api_key = pubmed_api_key
    )
    .log_step(sprintf("[LOAD] PubMed: %s records available.",
                      pubmed_both$TotalCount), verbose)
    bib_pm_both <- bibliometrix::convert2df(
      file = pubmedR::pmApi2df(pubmed_both),
      dbsource = "pubmed", format = "api"
    )
    bib_wos_both <- bibliometrix::convert2df(
      file = file_path, dbsource = "wos", format = "plaintext"
    )
    bibliography_standardized <- .merge_bibliographies(
      bib_pubmed = bib_pm_both,
      bib_wos    = bib_wos_both,
      verbose    = verbose
    )

  } else {
    db_source_lookup <- c("wos" = "wos", "scopus" = "scopus")
    format_lookup    <- c("wos" = "plaintext", "scopus" = "csv")

    .log_step(sprintf(
      "[LOAD] Reading %s file: %s",
      toupper(data_source), file_path
    ), verbose)

    bibliography_standardized <- bibliometrix::convert2df(
      file     = file_path,
      dbsource = db_source_lookup[[data_source]],
      format   = format_lookup[[data_source]]
    )
  }

  assertthat::assert_that(
    is.data.frame(bibliography_standardized),
    msg = paste(
      "bibliometrix::convert2df() did not return a data frame.",
      "Check your file format and data_source argument."
    )
  )
  assertthat::assert_that(
    nrow(bibliography_standardized) > 0,
    msg = "Bibliography is empty after loading. Check input data."
  )

  .log_step(sprintf(
    "[LOAD] Standardized bibliography: %d records, %d fields",
    nrow(bibliography_standardized), ncol(bibliography_standardized)
  ), verbose)

  return(bibliography_standardized)
}

#' @noRd
.standardize_and_filter_bibliography <- function(
    bibliography_raw,
    year_start,
    year_end,
    english_only = TRUE,
    verbose      = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography_raw))
  assertthat::assert_that(
    isTRUE(english_only) || isFALSE(english_only),
    msg = paste(
      "`english_only` must be TRUE or FALSE (not NA, not a string).",
      "Received:", deparse(english_only)
    )
  )
  assertthat::assert_that(
    nrow(bibliography_raw) > 0,
    msg = paste(
      "`bibliography_raw` has zero rows.",
      "Check that the input file or PubMed query returned records."
    )
  )
  assertthat::assert_that(
    "PY" %in% names(bibliography_raw),
    msg = paste(
      "Column 'PY' (Publication Year) not found in bibliography.",
      "Available columns:", paste(names(bibliography_raw), collapse = ", ")
    )
  )
  assertthat::assert_that(
    !is.factor(bibliography_raw[["PY"]]),
    msg = paste(
      "Column 'PY' is a factor.",
      "as.integer() on a factor returns level indices (1, 2, 3...),",
      "not the year values. Convert PY to character first:",
      "dplyr::mutate(bibliography_raw, PY = as.character(PY))"
    )
  )

  n_raw <- nrow(bibliography_raw)

  bibliography_with_year <- bibliography_raw |>
    dplyr::mutate(
      # Extract the 4-digit year from PY before coercing to integer.
      # PubMed exports sometimes include partial strings such as
      # "2010 epub ahead", "2019 Aug 15", or "2021 Online First".
      # str_extract finds the first 4-digit sequence, which is always
      # the calendar year; plain integer PY values work unchanged.
      publication_year = suppressWarnings(as.integer(
        stringr::str_extract(as.character(.data$PY), "[0-9]{4}")
      ))
    )

  n_unparseable <- sum(is.na(bibliography_with_year$publication_year))
  if (n_unparseable > 0) {
    .log_step(sprintf(
      "[FILTER] %d records had unparseable PY values and will be dropped.",
      n_unparseable
    ), verbose)
  }

  bibliography_year_filtered <- bibliography_with_year |>
    dplyr::filter(
      !is.na(.data$publication_year),
      .data$publication_year >= year_start,
      .data$publication_year <= year_end
    )

  n_outside_range <- n_raw - n_unparseable - nrow(bibliography_year_filtered)
  .log_step(sprintf(
    "[FILTER] Year range %d-%d: kept %d / %d records (%d outside range)",
    year_start, year_end,
    nrow(bibliography_year_filtered), n_raw,
    n_outside_range
  ), verbose)

  # ---- Language filter (controlled by english_only parameter) ----
  # When english_only = FALSE all languages are retained and a warning
  # is emitted noting that abstract-level analyses (keyword evolution,
  # thematic evolution) will be degraded for non-English records because
  # DE/AB fields are typically absent or untranslated in PubMed/WoS.
  bibliography_filtered <- if (!"LA" %in% names(bibliography_year_filtered)) {
    .log_step(
      "[FILTER] Language: LA column absent -- retaining all records.",
      verbose
    )
    bibliography_year_filtered
  } else if (!isTRUE(english_only)) {
    n_total_lang  <- nrow(bibliography_year_filtered)
    n_non_english <- sum(
      !is.na(bibliography_year_filtered$LA) &
        !toupper(trimws(bibliography_year_filtered$LA)) %in%
          c("ENGLISH", "ENG", "EN"),
      na.rm = TRUE
    )
    .log_step(sprintf(
      "[FILTER] Language: retaining ALL languages (%d non-English records, %.1f%%).",
      n_non_english, n_non_english / max(n_total_lang, 1L) * 100
    ), verbose)
    if (n_non_english > 0L) {
      warning(paste(
        sprintf(
          "%d non-English records retained (english_only = FALSE).",
          n_non_english
        ),
        "Keyword and thematic analyses may be incomplete for these records",
        "because DE/AB fields are often absent for non-English publications",
        "in PubMed and Web of Science exports. Consider whether the",
        "research question requires language-inclusive coverage."
      ), call. = FALSE)
    }
    bibliography_year_filtered
  } else {
    n_before_lang <- nrow(bibliography_year_filtered)
    bib_eng <- bibliography_year_filtered |>
      dplyr::filter(
        is.na(.data$LA) |
          toupper(trimws(.data$LA)) %in% c("ENGLISH", "ENG", "EN")
      )
    n_non_english <- n_before_lang - nrow(bib_eng)
    if (n_non_english > 0L) {
      .log_step(sprintf(
        "[FILTER] Language: removed %d non-English records (%.1f%%).",
        n_non_english,
        n_non_english / max(n_before_lang, 1L) * 100
      ), verbose)
    } else {
      .log_step("[FILTER] Language: all records are English.", verbose)
    }
    bib_eng
  }

  assertthat::assert_that(
    nrow(bibliography_filtered) > 0,
    msg = sprintf(
      paste(
        "No records remain after filtering to %d-%d.",
        "Check year_start/year_end or widen the date range."
      ),
      year_start, year_end
    )
  )

  return(bibliography_filtered)
}

#' @noRd
.compute_annual_publication_trends <- function(
    bibliography_filtered,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))
  assertthat::assert_that(
    "publication_year" %in% names(bibliography_filtered),
    msg = "Column 'publication_year' not found. Run standardize step first."
  )

  # Gracefully handle missing TC (Times Cited) column
  if (!"TC" %in% names(bibliography_filtered)) {
    .log_step(
      "[TRENDS] 'TC' (Times Cited) column absent; citation counts set to 0.",
      verbose
    )
    bibliography_filtered <- bibliography_filtered |>
      dplyr::mutate(TC = 0L)
  }

  # Use as.numeric first to avoid silent integer overflow:
  # as.integer("9999999999") coerces to NA with a suppressed warning,
  # then sum(na.rm=TRUE) gives 0 -- a silent wrong answer.
  # ifelse() with out-of-range guard produces NA explicitly.
  annual_trends_table <- bibliography_filtered |>
    dplyr::mutate(
      citation_numeric = suppressWarnings(as.numeric(.data$TC)),
      citation_count   = suppressWarnings(ifelse(
        !is.na(.data$citation_numeric) &
          .data$citation_numeric <= .Machine$integer.max &
          .data$citation_numeric >= -.Machine$integer.max,
        as.integer(.data$citation_numeric),
        NA_integer_
      ))
    ) |>
    dplyr::select(-"citation_numeric") |>
    dplyr::group_by(.data$publication_year) |>
    dplyr::summarise(
      publication_count = dplyr::n(),
      total_citations   = as.numeric(sum(.data$citation_count, na.rm = TRUE)),
      mean_citations    = mean(.data$citation_count, na.rm = TRUE),
      median_citations  = stats::median(.data$citation_count, na.rm = TRUE),
      max_citations     = suppressWarnings(
        max(.data$citation_count, na.rm = TRUE)
      ),
      max_citations     = dplyr::if_else(
        is.infinite(.data$max_citations), NA_real_, .data$max_citations
      ),
      .groups           = "drop"
    ) |>
    dplyr::arrange(.data$publication_year) |>
    dplyr::mutate(
      cumulative_publications = cumsum(.data$publication_count),
      cumulative_citations    = cumsum(as.numeric(.data$total_citations))
    )

  .log_step(sprintf(
    "[TRENDS] Annual trends: %d year-rows | %d total publications | %d total citations",
    nrow(annual_trends_table),
    sum(annual_trends_table$publication_count),
    sum(annual_trends_table$total_citations)
  ), verbose)

  return(annual_trends_table)
}

#' @noRd
.compute_keyword_evolution <- function(
    bibliography_filtered,
    keyword_column,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))
  assertthat::assert_that(assertthat::is.string(keyword_column))

  available_keyword_cols <- intersect(
    c("DE", "ID"), names(bibliography_filtered)
  )

  # Use caller-specified column if present, else fall back
  chosen_keyword_col <- if (keyword_column %in% names(bibliography_filtered)) {
    keyword_column
  } else if (length(available_keyword_cols) > 0) {
    available_keyword_cols[[1]]
  } else {
    NULL
  }

  if (is.null(chosen_keyword_col)) {
    .log_step(
      "[KEYWORDS] No keyword column found (DE or ID). Returning empty tibble.",
      verbose
    )
    return(dplyr::tibble(
      publication_year = integer(),
      keyword          = character(),
      keyword_count    = integer()
    ))
  }

  .log_step(sprintf(
    "[KEYWORDS] Parsing keyword column: '%s'", chosen_keyword_col
  ), verbose)

  assertthat::assert_that(
    "publication_year" %in% names(bibliography_filtered),
    msg = paste(
      "Column 'publication_year' not found in bibliography.",
      "Run .standardize_and_filter_bibliography() first."
    )
  )

  keyword_evolution_table <- bibliography_filtered |>
    dplyr::select(
      "publication_year",
      raw_keyword_string = dplyr::all_of(chosen_keyword_col)
    ) |>
    dplyr::filter(!is.na(.data$raw_keyword_string)) |>
    dplyr::mutate(
      keyword_tokens = stringr::str_split(.data$raw_keyword_string, ";")
    ) |>
    tidyr::unnest(cols = "keyword_tokens") |>
    dplyr::mutate(
      keyword = stringr::str_trim(
        stringr::str_to_upper(.data$keyword_tokens)
      )
    ) |>
    dplyr::filter(nchar(.data$keyword) > 0) |>
    dplyr::group_by(.data$publication_year, .data$keyword) |>
    dplyr::summarise(keyword_count = dplyr::n(), .groups = "drop") |>
    dplyr::arrange(.data$publication_year, dplyr::desc(.data$keyword_count))

  n_unique_keywords <- dplyr::n_distinct(keyword_evolution_table$keyword)
  .log_step(sprintf(
    "[KEYWORDS] %d keyword-year rows | %d unique keywords",
    nrow(keyword_evolution_table), n_unique_keywords
  ), verbose)

  return(keyword_evolution_table)
}

#' @noRd
.compute_country_contributions <- function(
    bibliography_filtered,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))

  if (!"AU_CO" %in% names(bibliography_filtered)) {
    .log_step(
      "[COUNTRIES] 'AU_CO' column not found. Returning empty tibble.",
      verbose
    )
    return(dplyr::tibble(
      publication_year  = integer(),
      country           = character(),
      publication_count = integer()
    ))
  }

  country_trends_table <- bibliography_filtered |>
    dplyr::mutate(paper_id = dplyr::row_number()) |>
    dplyr::select(
      "paper_id",
      publication_year,
      raw_country_string = "AU_CO"
    ) |>
    dplyr::filter(!is.na(.data$raw_country_string)) |>
    dplyr::mutate(
      country_tokens = stringr::str_split(.data$raw_country_string, ";")
    ) |>
    tidyr::unnest(cols = "country_tokens") |>
    dplyr::mutate(
      country = stringr::str_trim(
        stringr::str_to_upper(.data$country_tokens)
      )
    ) |>
    dplyr::filter(nchar(.data$country) > 0) |>
    # One country counted once per article (deduplicate within paper_id)
    dplyr::distinct(.data$paper_id, .data$publication_year,
                    .data$country) |>
    dplyr::group_by(.data$publication_year, .data$country) |>
    dplyr::summarise(publication_count = dplyr::n(), .groups = "drop") |>
    dplyr::arrange(.data$publication_year, dplyr::desc(.data$publication_count))

  n_unique_countries <- dplyr::n_distinct(country_trends_table$country)
  .log_step(sprintf(
    "[COUNTRIES] %d country-year rows | %d unique countries",
    nrow(country_trends_table), n_unique_countries
  ), verbose)

  return(country_trends_table)
}

#' @noRd
.compute_journal_trends <- function(
    bibliography_filtered,
    top_n_journals,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))
  assertthat::assert_that(assertthat::is.count(top_n_journals))

  if (!"SO" %in% names(bibliography_filtered)) {
    .log_step(
      "[JOURNALS] 'SO' column not found. Returning empty tibble.", verbose
    )
    return(dplyr::tibble(
      publication_year  = integer(),
      journal           = character(),
      publication_count = integer()
    ))
  }

  top_journals_vector <- bibliography_filtered |>
    dplyr::filter(!is.na(.data$SO)) |>
    dplyr::count(.data$SO, name = "total_articles") |>
    dplyr::arrange(dplyr::desc(.data$total_articles)) |>
    dplyr::slice_head(n = top_n_journals) |>
    dplyr::pull(.data$SO)

  journal_trends_table <- bibliography_filtered |>
    dplyr::filter(
      !is.na(.data$SO),
      .data$SO %in% top_journals_vector
    ) |>
    dplyr::group_by(.data$publication_year, .data$SO) |>
    dplyr::summarise(publication_count = dplyr::n(), .groups = "drop") |>
    dplyr::rename(journal = "SO") |>
    dplyr::arrange(.data$publication_year, dplyr::desc(.data$publication_count))

  .log_step(sprintf(
    "[JOURNALS] %d journal-year rows | top %d journals retained",
    nrow(journal_trends_table), top_n_journals
  ), verbose)

  return(journal_trends_table)
}


#' @noRd
.compute_author_metrics <- function(
    bibliography_filtered,
    top_n_authors,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))
  assertthat::assert_that(assertthat::is.count(top_n_authors))

  if (!"AU" %in% names(bibliography_filtered)) {
    .log_step("[AUTHORS] 'AU' column not found. Returning empty tibble.",
      verbose)
    return(dplyr::tibble(author=character(), publication_count=integer(),
      total_citations=numeric(), first_author_count=integer()))
  }

  bibliography_with_tc <- if ("TC" %in% names(bibliography_filtered)) {
    bibliography_filtered |>
      dplyr::mutate(tc_num = suppressWarnings(as.numeric(.data$TC)))
  } else {
    bibliography_filtered |> dplyr::mutate(tc_num = 0)
  }

  first_author_counts <- bibliography_with_tc |>
    dplyr::mutate(first_author = stringr::str_trim(
      stringr::str_extract(.data$AU, "^[^;]+"))) |>
    dplyr::filter(!is.na(.data$first_author),
                  nchar(.data$first_author) > 0) |>
    dplyr::count(.data$first_author, name = "first_author_count") |>
    dplyr::rename(author = "first_author")

  all_author_metrics <- bibliography_with_tc |>
    dplyr::mutate(author_tokens = stringr::str_split(.data$AU, ";")) |>
    tidyr::unnest(cols = "author_tokens") |>
    dplyr::mutate(author = stringr::str_trim(.data$author_tokens)) |>
    dplyr::filter(!is.na(.data$author), nchar(.data$author) > 0) |>
    dplyr::group_by(.data$author) |>
    dplyr::summarise(
      publication_count = dplyr::n(),
      total_citations   = as.numeric(sum(.data$tc_num, na.rm = TRUE)),
      .groups           = "drop"
    ) |>
    dplyr::left_join(first_author_counts, by = "author") |>
    dplyr::mutate(
      first_author_count = dplyr::coalesce(.data$first_author_count, 0L)
    ) |>
    dplyr::arrange(dplyr::desc(.data$publication_count)) |>
    dplyr::slice_head(n = top_n_authors)

  if (nrow(all_author_metrics) > 0L) {
    .log_step(sprintf("[AUTHORS] Top %d authors extracted | most prolific: %s (%d pubs)",
      top_n_authors,
      all_author_metrics$author[[1L]],
      all_author_metrics$publication_count[[1L]]), verbose)
  } else {
    .log_step("[AUTHORS] No classifiable authors found (all AU values absent or empty).",
      verbose)
  }

  return(all_author_metrics)
}

#' @noRd
.compute_institution_metrics <- function(
    bibliography_filtered,
    top_n_institutions,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))
  assertthat::assert_that(assertthat::is.count(top_n_institutions))

  if (!"AU_UN" %in% names(bibliography_filtered)) {
    .log_step(
      "[INSTITUTIONS] 'AU_UN' column not found. Returning empty tibble.",
      verbose)
    return(dplyr::tibble(institution=character(),
      publication_count=integer(), total_citations=numeric()))
  }

  bibliography_with_tc <- if ("TC" %in% names(bibliography_filtered)) {
    bibliography_filtered |>
      dplyr::mutate(tc_num = suppressWarnings(as.numeric(.data$TC)))
  } else {
    bibliography_filtered |> dplyr::mutate(tc_num = 0)
  }

  institution_metrics <- bibliography_with_tc |>
    dplyr::mutate(paper_id = dplyr::row_number()) |>
    dplyr::mutate(inst_tokens = stringr::str_split(.data$AU_UN, ";")) |>
    tidyr::unnest(cols = "inst_tokens") |>
    dplyr::mutate(institution = stringr::str_trim(
      stringr::str_to_upper(.data$inst_tokens))) |>
    dplyr::filter(!is.na(.data$institution),
                  nchar(.data$institution) > 0) |>
    dplyr::distinct(.data$paper_id, .data$institution, .data$tc_num) |>
    dplyr::group_by(.data$institution) |>
    dplyr::summarise(
      publication_count = dplyr::n(),
      total_citations   = as.numeric(sum(.data$tc_num, na.rm = TRUE)),
      .groups           = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$publication_count)) |>
    dplyr::slice_head(n = top_n_institutions)

  if (nrow(institution_metrics) > 0L) {
    .log_step(sprintf(
      "[INSTITUTIONS] Top %d institutions | most productive: %s (%d pubs)",
      top_n_institutions,
      institution_metrics$institution[[1L]],
      institution_metrics$publication_count[[1L]]), verbose)
  } else {
    .log_step(
      "[INSTITUTIONS] No classifiable institutions (all AU_UN values absent or empty).",
      verbose)
  }

  return(institution_metrics)
}

#' @noRd
.compute_journal_citation_metrics <- function(
    bibliography_filtered,
    top_n_journals,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))
  assertthat::assert_that(assertthat::is.count(top_n_journals))

  if (!"SO" %in% names(bibliography_filtered)) {
    .log_step(
      "[JOURNAL CITATIONS] 'SO' column not found. Returning empty tibble.",
      verbose)
    return(dplyr::tibble(journal=character(), publication_count=integer(),
      total_citations=numeric(), mean_citations=numeric(),
      median_citations=numeric()))
  }

  bibliography_with_tc <- if ("TC" %in% names(bibliography_filtered)) {
    bibliography_filtered |>
      dplyr::mutate(tc_num = suppressWarnings(as.numeric(.data$TC)))
  } else {
    bibliography_filtered |> dplyr::mutate(tc_num = 0)
  }

  journal_citation_table <- bibliography_with_tc |>
    dplyr::filter(!is.na(.data$SO)) |>
    dplyr::group_by(.data$SO) |>
    dplyr::summarise(
      publication_count = dplyr::n(),
      total_citations  = as.numeric(sum(.data$tc_num, na.rm = TRUE)),
      mean_citations   = mean(.data$tc_num, na.rm = TRUE),
      median_citations = stats::median(.data$tc_num, na.rm = TRUE),
      .groups          = "drop"
    ) |>
    dplyr::rename(journal = "SO") |>
    dplyr::arrange(dplyr::desc(.data$total_citations)) |>
    dplyr::slice_head(n = top_n_journals)

  if (nrow(journal_citation_table) > 0L) {
    .log_step(sprintf(
      "[JOURNAL CITATIONS] Top %d by citations | highest: %s (%g total)",
      top_n_journals,
      journal_citation_table$journal[[1L]],
      journal_citation_table$total_citations[[1L]]), verbose)
  } else {
    .log_step(
      "[JOURNAL CITATIONS] No journals found (all SO values absent or NA).",
      verbose)
  }

  return(journal_citation_table)
}

#' @noRd
.compute_growth_metrics <- function(
    annual_publication_trends,
    verbose
) {
  assertthat::assert_that(is.data.frame(annual_publication_trends))
  assertthat::assert_that(
    all(c("publication_year", "publication_count") %in%
      names(annual_publication_trends)),
    msg = paste("annual_publication_trends must contain",
      "'publication_year' and 'publication_count'.")
  )
  # Gracefully return NA metrics for very short time series instead of
  # crashing — enables smoke tests and single-year bootstrap runs.
  if (nrow(annual_publication_trends) < 2L) {
    .log_step("[GROWTH] Only 1 year of data -- returning NA growth metrics.",
              verbose)
    return(list(
      growth_by_year  = annual_publication_trends,
      loess_r_squared = NA_real_,
      peak_year       = annual_publication_trends$publication_year[[1L]],
      inflection_year = NA_integer_,
      cagr_pct        = NA_real_
    ))
  }
  if (nrow(annual_publication_trends) < 3L) {
    .log_step("[GROWTH] Fewer than 3 years -- LOESS skipped, limited metrics.",
              verbose)
  }

  growth_table <- annual_publication_trends |>
    dplyr::arrange(.data$publication_year) |>
    dplyr::mutate(
      prev_count     = dplyr::lag(.data$publication_count),
      yoy_change     = .data$publication_count - .data$prev_count,
      yoy_pct_change = round(
        100 * .data$yoy_change / .data$prev_count, 1L)
    ) |>
    dplyr::filter(!is.na(.data$yoy_change))

  # loess() warns or errors with very few data points (e.g. n=3 with span=0.75).
  # Use suppressWarnings + tryCatch so the rest of growth computation continues.
  loess_r_squared <- tryCatch({
    loess_fit <- suppressWarnings(loess(
      publication_count ~ publication_year,
      data = annual_publication_trends,
      span = 0.75
    ))
    ss_res <- sum((annual_publication_trends$publication_count -
        suppressWarnings(predict(loess_fit)))^2, na.rm = TRUE)
    ss_tot <- sum((annual_publication_trends$publication_count -
        mean(annual_publication_trends$publication_count))^2, na.rm = TRUE)
    if (ss_tot == 0) NA_real_ else round(1 - ss_res / ss_tot, 3L)
  }, error = function(e) {
    NA_real_
  })

  peak_year <- annual_publication_trends$publication_year[
    which.max(annual_publication_trends$publication_count)]

  inflection_year <- growth_table$publication_year[
    which.max(growth_table$yoy_change)]

  n_years     <- nrow(annual_publication_trends) - 1L
  count_first <- annual_publication_trends$publication_count[[1L]]
  count_last  <- annual_publication_trends$publication_count[[
    nrow(annual_publication_trends)]]
  cagr_pct <- if (count_first > 0 && n_years > 0) {
    round(100 * ((count_last / count_first)^(1 / n_years) - 1), 1)
  } else {
    NA_real_
  }

  .log_step(sprintf(
    "[GROWTH] LOESS R2=%.3f | Peak=%d | Inflection=%d | CAGR=%.1f%%",
    loess_r_squared, peak_year, inflection_year, cagr_pct), verbose)

  return(list(
    growth_by_year  = growth_table,
    loess_r_squared = loess_r_squared,
    peak_year       = peak_year,
    inflection_year = inflection_year,
    cagr_pct        = cagr_pct
  ))
}

#' @noRd
.normalize_country_string <- function(country_upper) {
  # Consolidates known variant spellings that bibliometrix produces from
  # C1 affiliation strings. All variants mapped to one canonical name.
  dplyr::case_when(
    country_upper %in% c(
      "USA", "UNITED STATES", "US", "U.S.A.", "U.S.",
      "UNITED STATES OF AMERICA", "AMERICA"
    )                                              ~ "USA",
    country_upper %in% c(
      "UK", "UNITED KINGDOM", "ENGLAND", "GREAT BRITAIN",
      "SCOTLAND", "WALES", "NORTHERN IRELAND", "BRITAIN"
    )                                              ~ "UNITED KINGDOM",
    country_upper %in% c(
      "GERMANY", "DEUTSCHLAND", "WEST GERMANY", "FEDERAL REPUBLIC OF GERMANY"
    )                                              ~ "GERMANY",
    country_upper %in% c(
      "PEOPLE'S REPUBLIC OF CHINA", "PEOPLES REPUBLIC OF CHINA",
      "P.R. CHINA", "P.R.CHINA", "CHINA PR"
    )                                              ~ "CHINA",
    country_upper %in% c(
      "SOUTH KOREA", "REPUBLIC OF KOREA", "KOREA (SOUTH)",
      "KOREA, REPUBLIC OF"
    )                                              ~ "SOUTH KOREA",
    country_upper %in% c(
      "NORTH KOREA", "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA"
    )                                              ~ "NORTH KOREA",
    country_upper %in% c(
      "TAIWAN", "REPUBLIC OF CHINA", "TAIWAN (ROC)"
    )                                              ~ "TAIWAN",
    country_upper %in% c(
      "THE NETHERLANDS", "NETHERLANDS, THE", "HOLLAND"
    )                                              ~ "NETHERLANDS",
    country_upper %in% c(
      "CZECH REPUBLIC", "CZECHIA"
    )                                              ~ "CZECH REPUBLIC",
    country_upper %in% c(
      "RUSSIA", "RUSSIAN FEDERATION", "USSR"
    )                                              ~ "RUSSIA",
    country_upper %in% c(
      "IRAN", "ISLAMIC REPUBLIC OF IRAN", "IRAN (ISLAMIC REPUBLIC OF)"
    )                                              ~ "IRAN",
    country_upper %in% c(
      "VIETNAM", "VIET NAM"
    )                                              ~ "VIETNAM",
    TRUE                                           ~ country_upper
  )
}

.compute_country_summary <- function(
    bibliography_filtered,
    verbose
) {
  assertthat::assert_that(is.data.frame(bibliography_filtered))

  if (!"AU_CO" %in% names(bibliography_filtered)) {
    .log_step(
      "[COUNTRY SUMMARY] 'AU_CO' column not found. Returning empty tibble.",
      verbose)
    return(dplyr::tibble(country=character(),
      publication_count=integer(), pct_of_total=numeric()))
  }

  n_papers <- nrow(bibliography_filtered)

  country_summary_table <- bibliography_filtered |>
    dplyr::mutate(paper_id = dplyr::row_number()) |>
    dplyr::select("paper_id", raw_country_string = "AU_CO") |>
    dplyr::filter(!is.na(.data$raw_country_string)) |>
    dplyr::mutate(
      country_tokens = stringr::str_split(.data$raw_country_string, ";")
    ) |>
    tidyr::unnest(cols = "country_tokens") |>
    dplyr::mutate(
      country_raw  = stringr::str_trim(
        stringr::str_to_upper(.data$country_tokens)
      ),
      country = purrr::map_chr(.data$country_raw, .normalize_country_string)
    ) |>
    dplyr::filter(nchar(.data$country) > 0) |>
    dplyr::distinct(.data$paper_id, .data$country) |>
    dplyr::count(.data$country, name = "publication_count") |>
    dplyr::mutate(
      pct_of_total = round(100 * .data$publication_count / n_papers, 1)
    ) |>
    dplyr::arrange(dplyr::desc(.data$publication_count))

  if (nrow(country_summary_table) > 0L) {
    .log_step(sprintf(
      "[COUNTRY SUMMARY] %d countries | top: %s (%g%%)",
      nrow(country_summary_table),
      country_summary_table$country[[1L]],
      country_summary_table$pct_of_total[[1L]]), verbose)
  } else {
    .log_step(
      "[COUNTRY SUMMARY] No classifiable countries (all AU_CO values absent or empty).",
      verbose)
  }

  return(country_summary_table)
}

#' @noRd
.theme_fpmrs_manuscript <- function(base_size = 11) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(
        face   = "bold",
        size   = base_size + 2,
        margin = ggplot2::margin(b = 6)
      ),
      plot.subtitle = ggplot2::element_text(
        size   = base_size - 1,
        color  = "grey40",
        margin = ggplot2::margin(b = 10)
      ),
      plot.caption = ggplot2::element_text(
        size  = base_size - 2,
        color = "grey55",
        hjust = 0
      ),
      axis.title = ggplot2::element_text(
        size = base_size - 1,
        face = "bold"
      ),
      axis.text        = ggplot2::element_text(size = base_size - 2),
      panel.grid.minor = ggplot2::element_blank(),
      panel.grid.major = ggplot2::element_line(
        color     = "grey92",
        linewidth = 0.4
      ),
      legend.position  = "bottom",
      legend.title     = ggplot2::element_text(size = base_size - 2,
                                               face = "bold"),
      legend.text      = ggplot2::element_text(size = base_size - 3),
      legend.key.width = ggplot2::unit(1.2, "cm"),
      plot.margin      = ggplot2::margin(14, 14, 14, 14),
      strip.text       = ggplot2::element_text(face = "bold",
                                               size = base_size - 1)
    )
}

#' @noRd
.save_all_figures <- function(
    figures_list,
    output_dir,
    figure_format,
    figure_width,
    figure_height,
    verbose
) {
  assertthat::assert_that(is.list(figures_list))
  assertthat::assert_that(length(figures_list) > 0,
    msg = "`figures_list` is empty; nothing to save.")

  if (figure_format == "svg") {
    assertthat::assert_that(
      requireNamespace("svglite", quietly = TRUE),
      msg = paste(
        "Saving figures as SVG requires the 'svglite' package.",
        "Install it with: install.packages('svglite')",
        "Or choose figure_format = 'pdf' or 'png' instead."
      )
    )
  }

  figure_paths_vector <- purrr::imap_chr(
    figures_list,
    function(figure_object, figure_name) {
      output_file_name <- paste0(figure_name, ".", figure_format)
      output_file_path <- file.path(output_dir, output_file_name)

      ggplot2::ggsave(
        filename = output_file_path,
        plot     = figure_object,
        width    = figure_width,
        height   = figure_height,
        dpi      = 300,
        units    = "in"
      )

      resolved_path <- normalizePath(output_file_path)
      .log_step(sprintf(
        "[SAVE]  %-32s -> %s", figure_name, resolved_path
      ), verbose)

      return(resolved_path)
    }
  )

  return(figure_paths_vector)
}



# ============================================================
# VALIDATED PUBMED QUERIES -- 8 SUBSPECIALTIES
# ============================================================



#' Default Subspecialty Full-Name Labels for Abstract Prose
#'
#' @description
#' Named character vector mapping short display names used throughout
#' the pipeline to full prose forms for manuscript abstracts. Pass to
#' \code{generate_abstract_results_text(subspecialty_labels = ...)}.
#' The default \code{NULL} leaves subspecialty names as-is.
#'
#' @examples
#' # Use defaults:
#' \dontrun{
#' ab <- generate_abstract_results_text(
#'   comparison_summary_table,
#'   subspecialty_labels = default_subspecialty_labels
#' )
#' # Extend with custom entries:
#' my_labels <- c(default_subspecialty_labels,
#'                "Custom" = "custom subspecialty name")
#' }
#'
#' @export
default_subspecialty_labels <- c(
  "FPMRS"        = "female pelvic medicine and reconstructive surgery",
  "REI"          = "reproductive endocrinology and infertility",
  "Gyn Oncology" = "gynecologic oncology",
  "MFM"          = "maternal-fetal medicine",
  "MIGS"         = "minimally invasive gynecologic surgery",
  "Complex FP"   = "complex family planning",
  "CFP"          = "complex family planning",
  "Pedi Gyn"     = "pediatric and adolescent gynecology",
  "PAG"          = "pediatric and adolescent gynecology"
)

# ============================================================
# FPMRS GOLD-STANDARD LANDMARK PMID VALIDATION
# ============================================================

#' FPMRS Landmark Trial PMIDs for Query Validation
#'
#' @description
#' A curated list of PMIDs for landmark randomised controlled trials,
#' multicentre studies, and highly cited observational studies that
#' define the FPMRS evidence base. Used by
#' \code{\link{validate_fpmrs_query_recall}()} to assess whether a
#' PubMed search strategy successfully retrieves the studies a reviewer
#' would consider essential.
#'
#' @format Named character vector; names are short study labels,
#'   values are PubMed IDs (as character strings).
#'
#' @export
fpmrs_landmark_pmids <- c(
  # Surgical stress urinary incontinence RCTs
  "CARE_2006"          = "16543589",
  "TOMUS_2010"         = "20375407",
  "OPUS_2012"          = "22436048",
  "SISTEr_2007"        = "17475733",
  # Surgical pelvic organ prolapse RCTs
  "OPTIMAL_2014"       = "24996525",
  "e_OPTIMAL_2020"     = "32453876",
  "ESTEEM_2019"        = "31356825",
  "POP_UP_2022"        = "35709735",
  "PREFER_2019"        = "31283136",
  # Pelvic floor muscle training
  "Dumoulin_PFMT_2015" = "25572825",
  "ACHIEVE_2021"       = "34224659",
  "LILY_2017"          = "28402769",
  # Epidemiology
  "Wu_burden_2009"     = "19104521",
  "Nygaard_NHANES_2008"= "18697447",
  "Swift_POPQ_2000"    = "10712561",
  "Milsom_OAB_2001"    = "11459447",
  # Mesh regulatory
  "Maher_Cochrane_2016"= "27933387",
  "Barber_mesh_2019"   = "30644493",
  # Outcome instruments
  "Barber_PFDI_2001"   = "11370665",
  "Srikrishna_PISQ_2010"="20151108",
  "Kelleher_KHQ_1997"  = "9380257",
  "Uebersax_UDI_1995"  = "7659676",
  # Workforce
  "Sung_workforce_2008"= "18442118"
)

#' Validate PubMed Query Recall Against FPMRS Landmark Trials
#'
#' @description
#' Checks whether a PubMed search query retrieves a curated set of
#' landmark FPMRS publications. Recall below 80\% triggers a warning
#' recommending query refinement before running the full pipeline.
#'
#' @param query Character string. The PubMed search query to test.
#' @param api_key Character or NULL. NCBI API key.
#' @param recall_threshold Numeric in (0, 1]. Minimum acceptable recall.
#'   Defaults to \code{0.80}.
#' @param pmid_list Named character vector. Defaults to
#'   \code{\link{fpmrs_landmark_pmids}}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A list with elements \code{recall}, \code{n_retrieved},
#'   \code{n_landmarks}, \code{hits}, \code{misses},
#'   \code{passes_threshold}.
#'
#' @examples
#' \dontrun{
#' validate_fpmrs_query_recall(
#'   query   = get_subspecialty_pubmed_query("fpmrs"),
#'   api_key = Sys.getenv("NCBI_API_KEY")
#' )
#' }
#'
#' @importFrom assertthat assert_that is.string is.number is.flag
#' @export
validate_fpmrs_query_recall <- function(
    query,
    api_key           = NULL,
    recall_threshold  = 0.80,
    pmid_list         = fpmrs_landmark_pmids,
    verbose           = TRUE
) {
  assertthat::assert_that(assertthat::is.string(query))
  assertthat::assert_that(assertthat::is.number(recall_threshold) &&
                            recall_threshold > 0 && recall_threshold <= 1)
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[VALIDATE] Testing query recall against %d landmark PMIDs ...",
    length(pmid_list)
  ), verbose)

  query_result <- tryCatch(
    pubmedR::pmApiRequest(query = query, limit = 50000L,
                          api_key = api_key),
    error = function(e) stop(sprintf("[VALIDATE] PubMed API error: %s", e$message))
  )
  returned_pmids <- as.character(query_result$IdList$Id)

  hit_flags  <- pmid_list %in% returned_pmids
  recall_val <- mean(hit_flags)
  hits       <- names(pmid_list)[hit_flags]
  misses     <- names(pmid_list)[!hit_flags]
  passes     <- recall_val >= recall_threshold

  .log_step(sprintf(
    "[VALIDATE] Recall = %.0f%% (%d/%d) | %s",
    recall_val * 100, sum(hit_flags), length(pmid_list),
    if (passes) "PASS" else "FAIL - refine query"
  ), verbose)

  if (!passes) {
    warning(sprintf(
      "Query recall %.0f%% < %.0f%% threshold. Missed: %s",
      recall_val * 100, recall_threshold * 100,
      paste(misses, collapse = ", ")
    ), call. = FALSE)
  }
  list(recall = recall_val, n_retrieved = length(returned_pmids),
       n_landmarks = length(pmid_list),
       hits = hits, misses = misses, passes_threshold = passes)
}


# MeSH term introduction dates for FPMRS-relevant terms.
# Used by .check_mesh_coverage_years() to flag pre-MeSH indexing gaps.
.FPMRS_MESH_INTRODUCTION_YEARS <- list(
  "Pelvic Floor"                 = 1998L,
  "Pelvic Organ Prolapse"        = 1999L,
  "Pelvic Floor Disorders"       = 2002L,
  "Urinary Incontinence, Stress" = 1978L,
  "Urinary Incontinence, Urge"   = 1980L,
  "Uterine Prolapse"             = 1966L,
  "Cystocele"                    = 1966L
)

#' @noRd
.check_mesh_coverage_years <- function(year_start, verbose = TRUE) {
  affected <- Filter(function(yr) yr > year_start,
    .FPMRS_MESH_INTRODUCTION_YEARS)
  if (length(affected) > 0L) {
    affected_str <- paste(
      sprintf("  - '%s' (introduced %d)", names(affected), unlist(affected)),
      collapse = "\n"
    )
    .log_step(sprintf(
      "[MESH] year_start=%d predates %d MeSH terms: \n%s\n%s",
      year_start, length(affected), affected_str,
      paste("Pre-MeSH records captured via title/abstract only.",
            "Breakpoints near 1998-2002 may partly reflect indexing",
            "changes. Flag in manuscript limitations.")
    ), verbose)
    warning(sprintf(
      "%d MeSH terms not yet introduced at year_start=%d. See console.",
      length(affected), year_start), call. = FALSE)
  } else {
    .log_step(sprintf(
      "[MESH] year_start=%d: all key MeSH terms available.", year_start
    ), verbose)
  }
  dplyr::tibble(
    mesh_term     = names(.FPMRS_MESH_INTRODUCTION_YEARS),
    introduced_yr = as.integer(unlist(.FPMRS_MESH_INTRODUCTION_YEARS)),
    predates_start = as.integer(unlist(.FPMRS_MESH_INTRODUCTION_YEARS)) > year_start
  )
}

#' @noRd
.write_mesh_coverage_note <- function(output_dir, year_start, verbose = TRUE) {
  mesh_tbl <- .check_mesh_coverage_years(year_start, verbose)
  affected  <- mesh_tbl[mesh_tbl$predates_start, ]
  lines <- c(
    "MESH TERM COVERAGE NOTE",
    paste(rep("-", 40L), collapse = ""),
    sprintf("Analysis year_start: %d", year_start), ""
  )
  if (nrow(affected) > 0L) {
    lines <- c(lines,
      sprintf("%d MeSH terms introduced after year_start:", nrow(affected)),
      sprintf("  - '%s' (introduced %d, gap: %d yrs)",
              affected$mesh_term, affected$introduced_yr,
              affected$introduced_yr - year_start),
      "",
      paste("IMPLICATION: Pre-MeSH literature captured via title/abstract.",
        "A growth step near",
        paste(sort(unique(affected$introduced_yr)), collapse=" / "),
        "may reflect vocabulary expansion. Acknowledge in Methods.")
    )
  } else {
    lines <- c(lines, "All key MeSH terms predate year_start. No gap.")
  }
  note_path <- file.path(output_dir, "mesh_coverage_note.txt")
  writeLines(lines, note_path)
  if (requireNamespace("jsonlite", quietly=TRUE)) {
    jsonlite::write_json(
      list(year_start=year_start, affected=as.list(affected)),
      file.path(output_dir, "mesh_coverage_note.json"),
      pretty=TRUE, auto_unbox=TRUE)
  }
  .log_step(sprintf("[MESH] Coverage note: %s", note_path), verbose)
  invisible(note_path)
}

#' Return the validated PubMed query for a given subspecialty
#'
#' @description
#' Returns a curated, validated PubMed search string for one of eight
#' OB/GYN subspecialties or urology. Queries use MeSH terms as the
#' primary strategy with Title/Abstract fallbacks for concepts that
#' lack dedicated MeSH headings (e.g., MIGS, FPMRS abbreviation).
#'
#' @param subspecialty_key Character string. One of:
#'   \code{"fpmrs"}, \code{"rei"}, \code{"gyn_onc"}, \code{"mfm"},
#'   \code{"migs"}, \code{"cfp"}, \code{"pag"}, \code{"urology"}.
#'
#' @return A single character string -- a valid PubMed query.
#'
#' @importFrom assertthat assert_that is.string
#' @export
get_subspecialty_pubmed_query <- function(subspecialty_key) {
  assertthat::assert_that(assertthat::is.string(subspecialty_key))
  subspecialty_key <- tolower(trimws(subspecialty_key))
  # Normalise to lowercase so "FPMRS" and "fpmrs" both work
  # Alias map: common alternative spellings -> canonical key
  alias_map <- c(
    pedi_gyn      = "pag",
    pediatric_gyn = "pag",
    peds_gyn      = "pag",
    complex_fp    = "cfp",
    family_plan   = "cfp",
    gyn_onc       = "gyn_onc",  # already canonical
    gynonc        = "gyn_onc",
    gynecologic_onc = "gyn_onc",
    gynecologic_oncology = "gyn_onc",
    urogynecology = "fpmrs",
    urps          = "fpmrs"
  )
  if (subspecialty_key %in% names(alias_map)) {
    subspecialty_key <- alias_map[[subspecialty_key]]
  }

  query_lookup <- list(

    # FPMRS/URPS query -- historically aware.
    # Includes URPS terminology (effective 2024) alongside FPMRS.
    # MeSH term introduction dates:
    #   "Pelvic Floor"              introduced 1998
    #   "Pelvic Organ Prolapse"     introduced 1999
    #   "Pelvic Floor Disorders"    introduced 2002
    #   "Urinary Incontinence, Stress" introduced 1978
    # Pre-1998 papers were indexed under older MeSH terms;
    # Title/Abstract arms capture literature that predates modern headings.
    fpmrs = paste0(
      "(",
      # Modern MeSH terms
      "\"female pelvic medicine\"[MeSH Terms] OR ",
      "\"pelvic organ prolapse\"[MeSH Terms] OR ",
      "\"urinary incontinence, stress\"[MeSH Terms] OR ",
      "\"pelvic floor disorders\"[MeSH Terms] OR ",
      "\"urinary incontinence, urge\"[MeSH Terms] OR ",
      "\"pelvic floor\"[MeSH Terms] OR ",
      # Pre-1998 MeSH terms for prolapse / incontinence
      "\"uterine prolapse\"[MeSH Terms] OR ",
      "\"cystocele\"[MeSH Terms] OR ",
      "\"rectocele\"[MeSH Terms] OR ",
      # Title/Abstract fallbacks for all eras
      "\"urogynecology\"[Title/Abstract] OR ",
      "\"urogynecological\"[Title/Abstract] OR ",
      "\"FPMRS\"[Title/Abstract] OR ",
      "\"URPS\"[Title/Abstract] OR ",
      "\"urogynecology and reconstructive pelvic surgery\"[Title/Abstract] OR ",
      "\"pelvic floor\"[Title/Abstract] OR ",
      "\"pelvic organ prolapse\"[Title/Abstract] OR ",
      "\"stress urinary incontinence\"[Title/Abstract] OR ",
      "\"urge urinary incontinence\"[Title/Abstract] OR ",
      "\"urinary incontinence\"[Title/Abstract] OR ",
      "\"pelvic floor dysfunction\"[Title/Abstract] OR ",
      "\"reconstructive pelvic surgery\"[Title/Abstract] OR ",
      "\"vaginal prolapse\"[Title/Abstract] OR ",
      "\"uterine prolapse\"[Title/Abstract] OR ",
      "\"cystocele\"[Title/Abstract] OR ",
      "\"rectocele\"[Title/Abstract] OR ",
      "\"colporrhaphy\"[Title/Abstract] OR ",
      "\"sacrocolpopexy\"[Title/Abstract] OR ",
      "\"suburethral sling\"[Title/Abstract] OR ",
      "\"pubovaginal sling\"[Title/Abstract] OR ",
      "\"colposuspension\"[Title/Abstract]",
      ")"
    ),

    rei = paste0(
      "(\"reproductive medicine\"[MeSH Terms] OR ",
      "\"fertilization in vitro\"[MeSH Terms] OR ",
      "\"reproductive endocrinology\"[Title/Abstract] OR ",
      "\"infertility\"[MeSH Terms] OR ",
      "\"ovulation induction\"[MeSH Terms] OR ",
      "\"embryo transfer\"[MeSH Terms] OR ",
      "\"polycystic ovary syndrome\"[MeSH Terms] OR ",
      "\"REI\"[Title/Abstract])"
    ),

    gyn_onc = paste0(
      "(\"genital neoplasms, female\"[MeSH Terms] OR ",
      "\"gynecologic oncology\"[Title/Abstract] OR ",
      "\"ovarian neoplasms\"[MeSH Terms] OR ",
      "\"uterine cervical neoplasms\"[MeSH Terms] OR ",
      "\"endometrial neoplasms\"[MeSH Terms] OR ",
      "\"vulvar neoplasms\"[MeSH Terms] OR ",
      "\"fallopian tube neoplasms\"[MeSH Terms])"
    ),

    mfm = paste0(
      "(\"maternal-fetal medicine\"[Title/Abstract] OR ",
      "\"perinatology\"[Title/Abstract] OR ",
      "\"fetal medicine\"[Title/Abstract] OR ",
      "\"preterm birth\"[MeSH Terms] OR ",
      "\"pre-eclampsia\"[MeSH Terms] OR ",
      "\"prenatal diagnosis\"[MeSH Terms] OR ",
      "\"fetal growth retardation\"[MeSH Terms] OR ",
      "\"MFM\"[Title/Abstract])"
    ),

    migs = paste0(
      # Covers both the modern MIGS era (2010+) and pre-2010 literature
      # which used 'laparoscopic gynecologic surgery' before the subspecialty
      # was formally named. Both eras required for 2000-2023 analysis.
      "(\"minimally invasive gynecologic surgery\"[Title/Abstract] OR ",
      "\"MIGS\"[Title/Abstract] OR ",
      "\"laparoscopic hysterectomy\"[Title/Abstract] OR ",
      "\"robotic hysterectomy\"[Title/Abstract] OR ",
      "\"robotic gynecologic surgery\"[Title/Abstract] OR ",
      "\"laparoscopic gynecologic surgery\"[Title/Abstract] OR ",
      "\"operative laparoscopy\"[Title/Abstract] OR ",
      "\"gynecologic laparoscopy\"[Title/Abstract] OR ",
      "(\"endometriosis\"[MeSH Terms] AND \"laparoscopy\"[MeSH Terms]) OR ",
      "\"uterine myomectomy\"[Title/Abstract])"
    ),

    cfp = paste0(
      "(\"complex family planning\"[Title/Abstract] OR ",
      "\"abortion, induced\"[MeSH Terms] OR ",
      "\"abortion care\"[Title/Abstract] OR ",
      "\"intrauterine devices\"[MeSH Terms] OR ",
      "\"contraception\"[MeSH Terms] OR ",
      "\"emergency contraception\"[MeSH Terms] OR ",
      "\"family planning services\"[MeSH Terms])"
    ),

    pag = paste0(
      "(\"pediatric gynecology\"[Title/Abstract] OR ",
      "\"adolescent gynecology\"[Title/Abstract] OR ",
      "\"pediatric and adolescent gynecology\"[Title/Abstract] OR ",
      "\"puberty, precocious\"[MeSH Terms] OR ",
      "\"disorders of sex development\"[MeSH Terms] OR ",
      "(\"menstrual disorders\"[MeSH Terms] AND ",
      "\"adolescent\"[MeSH Terms]))"
    ),

    urology = paste0(
      "(\"urology\"[MeSH Terms] OR ",
      "\"urologic diseases\"[MeSH Terms] OR ",
      "\"urologic surgical procedures\"[MeSH Terms] OR ",
      "\"prostatic neoplasms\"[MeSH Terms] OR ",
      "\"bladder neoplasms\"[MeSH Terms] OR ",
      "\"kidney neoplasms\"[MeSH Terms] OR ",
      "\"urinary calculi\"[MeSH Terms] OR ",
      "\"erectile dysfunction\"[MeSH Terms])"
    )
  )

  assertthat::assert_that(
    subspecialty_key %in% names(query_lookup),
    msg = paste0(
      "`subspecialty_key` must be one of: ",
      paste(names(query_lookup), collapse = ", "),
      ". Received: ", subspecialty_key
    )
  )

  return(query_lookup[[subspecialty_key]])
}


# ============================================================
# COMPARISON HELPERS  (@noRd)
# ============================================================

#' @noRd
.build_comparison_summary_table <- function(
    subspecialty_results_list,
    verbose
) {
  assertthat::assert_that(is.list(subspecialty_results_list))
  assertthat::assert_that(
    length(subspecialty_results_list) >= 2L,
    msg = "Need at least 2 subspecialty results to compare."
  )

  required_elements <- c(
    "subspecialty", "bibliography", "annual_trends",
    "author_metrics", "journal_citations",
    "country_summary", "growth_metrics", "journal_trends"
  )

  purrr::iwalk(subspecialty_results_list, function(sp, key) {
    missing_els <- setdiff(required_elements, names(sp))
    assertthat::assert_that(
      length(missing_els) == 0L,
      msg = sprintf(
        "Subspecialty '%s' is missing elements: %s",
        key, paste(missing_els, collapse = ", ")
      )
    )
  })

  comparison_table <- purrr::map_dfr(
    subspecialty_results_list,
    function(sp_result) {
      annual_tbl  <- sp_result$annual_trends
      growth_obj  <- sp_result$growth_metrics
      authors_tbl <- sp_result$author_metrics
      jcit_tbl    <- sp_result$journal_citations
      jtrend_tbl  <- sp_result$journal_trends
      co_sum_tbl  <- sp_result$country_summary
      bib_tbl     <- sp_result$bibliography

      top_journal_by_articles <- if (nrow(jtrend_tbl) > 0L) {
        jtrend_tbl |>
          dplyr::group_by(.data$journal) |>
          dplyr::summarise(
            n = sum(.data$publication_count), .groups = "drop"
          ) |>
          dplyr::arrange(dplyr::desc(.data$n)) |>
          dplyr::slice_head(n = 1L) |>
          dplyr::pull(.data$journal)
      } else {
        NA_character_
      }

      dplyr::tibble(
        subspecialty             = sp_result$subspecialty,
        total_documents          = nrow(bib_tbl),
        total_citations          = sum(
          annual_tbl$total_citations, na.rm = TRUE
        ),
        mean_citations_per_paper = round(
          sum(annual_tbl$total_citations, na.rm = TRUE) /
            max(nrow(bib_tbl), 1L),
          1L
        ),
        # Guard against 0-row annual_tbl: min/max on integer(0) returns
        # Inf/-Inf which silently corrupts downstream comparisons.
        year_first               = if (nrow(annual_tbl) > 0L)
          min(annual_tbl$publication_year) else NA_integer_,
        year_last                = if (nrow(annual_tbl) > 0L)
          max(annual_tbl$publication_year) else NA_integer_,
        n_active_years           = dplyr::n_distinct(
          annual_tbl$publication_year
        ),
        unique_countries         = dplyr::n_distinct(co_sum_tbl$country),
        top_country              = if (nrow(co_sum_tbl) > 0L)
          co_sum_tbl$country[[1L]] else NA_character_,
        top_country_pct          = if (nrow(co_sum_tbl) > 0L)
          co_sum_tbl$pct_of_total[[1L]] else NA_real_,
        cagr_pct                 = growth_obj$cagr_pct,
        peak_year                = growth_obj$peak_year,
        inflection_year          = growth_obj$inflection_year,
        loess_r_squared          = growth_obj$loess_r_squared,
        top_author               = if (nrow(authors_tbl) > 0L)
          authors_tbl$author[[1L]] else NA_character_,
        top_author_pubs          = if (nrow(authors_tbl) > 0L)
          authors_tbl$publication_count[[1L]] else NA_integer_,
        top_journal_by_articles  = top_journal_by_articles,
        top_journal_by_citations = if (nrow(jcit_tbl) > 0L)
          jcit_tbl$journal[[1L]] else NA_character_,
        top_journal_total_citations = if (nrow(jcit_tbl) > 0L)
          jcit_tbl$total_citations[[1L]] else NA_real_,
        # ---- Authorship / team-size metrics ----
        mean_authors_per_paper   = {
          au_metrics <- if (!is.null(sp_result$authorship_metrics))
            sp_result$authorship_metrics
          else
            .compute_authorship_metrics(sp_result$bibliography,
                                        verbose = FALSE)
          au_metrics$mean_authors_per_paper
        },
        median_authors_per_paper = {
          au_metrics <- if (!is.null(sp_result$authorship_metrics))
            sp_result$authorship_metrics
          else
            .compute_authorship_metrics(sp_result$bibliography,
                                        verbose = FALSE)
          au_metrics$median_authors_per_paper
        },
        pct_large_team_10plus    = {
          au_metrics <- if (!is.null(sp_result$authorship_metrics))
            sp_result$authorship_metrics
          else
            .compute_authorship_metrics(sp_result$bibliography,
                                        verbose = FALSE)
          au_metrics$pct_large_team_10plus
        },
        # ---- Disruption Index ----
        median_di      = {
          di_res <- if (!is.null(sp_result$disruption_index))
            sp_result$disruption_index
          else
            .compute_disruption_index(sp_result$bibliography,
                                      verbose = FALSE)
          di_res$median_di
        },
        pct_disruptive = {
          di_res <- if (!is.null(sp_result$disruption_index))
            sp_result$disruption_index
          else
            .compute_disruption_index(sp_result$bibliography,
                                      verbose = FALSE)
          di_res$pct_disruptive
        },
        # ---- Citation quality metrics ----
        h_index = {
          cm <- if (!is.null(sp_result$citation_metrics))
            sp_result$citation_metrics
          else .compute_citation_metrics(sp_result$bibliography,
                                         verbose = FALSE)
          cm$h_index
        },
        citation_velocity_median = {
          cm <- if (!is.null(sp_result$citation_metrics))
            sp_result$citation_metrics
          else .compute_citation_metrics(sp_result$bibliography,
                                         verbose = FALSE)
          cm$citation_velocity_median
        },
        citation_gini = {
          cm <- if (!is.null(sp_result$citation_metrics))
            sp_result$citation_metrics
          else .compute_citation_metrics(sp_result$bibliography,
                                         verbose = FALSE)
          cm$citation_gini
        },
        # ---- International collaboration ----
        pct_intl_collab = {
          ic <- if (!is.null(sp_result$intl_collab))
            sp_result$intl_collab
          else .compute_intl_collaboration(sp_result$bibliography,
                                           verbose = FALSE)
          ic$pct_intl_collab
        },
        mean_countries_paper = {
          ic <- if (!is.null(sp_result$intl_collab))
            sp_result$intl_collab
          else .compute_intl_collaboration(sp_result$bibliography,
                                           verbose = FALSE)
          ic$mean_countries_paper
        },
        # ---- Evidence quality ----
        evidence_quality_score = {
          st <- if (!is.null(sp_result$study_type_metrics))
            sp_result$study_type_metrics
          else .compute_study_type_metrics(sp_result$bibliography,
                                           verbose = FALSE)
          eq <- if (!is.null(sp_result$evidence_quality))
            sp_result$evidence_quality
          else .compute_evidence_quality_score(st, verbose = FALSE)
          eq$evidence_quality_score
        },
        pct_high_evidence = {
          st <- if (!is.null(sp_result$study_type_metrics))
            sp_result$study_type_metrics
          else .compute_study_type_metrics(sp_result$bibliography,
                                           verbose = FALSE)
          eq <- if (!is.null(sp_result$evidence_quality))
            sp_result$evidence_quality
          else .compute_evidence_quality_score(st, verbose = FALSE)
          eq$pct_high_evidence
        },
        # ---- Reference age ----
        price_index = {
          ra <- if (!is.null(sp_result$ref_age_metrics))
            sp_result$ref_age_metrics
          else .compute_reference_age_metrics(
            sp_result$bibliography, verbose = FALSE
          )
          ra$price_index
        },
        mean_ref_age = {
          ra <- if (!is.null(sp_result$ref_age_metrics))
            sp_result$ref_age_metrics
          else .compute_reference_age_metrics(
            sp_result$bibliography, verbose = FALSE
          )
          ra$mean_ref_age
        },
        # ---- Relative Citation Ratio ----
        rcr_median = {
          rcr_obj <- sp_result$rcr_result
          if (!is.null(rcr_obj) && "rcr_median" %in% names(rcr_obj))
            rcr_obj[["rcr_median"]] else NA_real_
        },
        rcr_above_1_pct = {
          rcr_obj <- sp_result$rcr_result
          if (!is.null(rcr_obj) && "rcr_above_1" %in% names(rcr_obj))
            rcr_obj[["rcr_above_1"]] else NA_real_
        }
      )
    }
  )

  # Add ranks across subspecialties
  comparison_table <- comparison_table |>
    dplyr::mutate(
      rank_by_volume        = rank(-total_documents,          ties.method = "min", na.last = "keep"),
      rank_by_citations     = rank(-total_citations,          ties.method = "min", na.last = "keep"),
      rank_by_cagr          = rank(-cagr_pct,                 ties.method = "min", na.last = "keep"),
      rank_by_countries     = rank(-unique_countries,         ties.method = "min", na.last = "keep"),
      rank_by_impact        = rank(-mean_citations_per_paper, ties.method = "min", na.last = "keep"),
      rank_by_collaboration = rank(-mean_authors_per_paper,   ties.method = "min", na.last = "keep"),
      rank_by_disruption    = dplyr::if_else(
        is.na(.data$median_di),
        NA_integer_,
        as.integer(rank(-.data$median_di, ties.method = "min",
                        na.last = "keep"))
      ),
      rank_by_h_index       = as.integer(
        rank(-.data$h_index, ties.method = "min", na.last = "keep")
      ),
      rank_by_velocity      = as.integer(
        rank(-.data$citation_velocity_median, ties.method = "min", na.last = "keep")
      ),
      rank_by_citation_gini = as.integer(
        rank(-.data$citation_gini, ties.method = "min", na.last = "keep")
      ),
      rank_by_evidence      = as.integer(
        rank(-.data$evidence_quality_score, ties.method = "min", na.last = "keep")
      ),
      rank_by_intl_collab   = as.integer(
        rank(-.data$pct_intl_collab, ties.method = "min", na.last = "keep")
      ),
      rank_by_price_index   = dplyr::if_else(
        is.na(.data$price_index),
        NA_integer_,
        as.integer(rank(-.data$price_index, ties.method = "min",
                        na.last = "keep"))
      )
    )

  n_subspecialties <- nrow(comparison_table)
  .log_step(sprintf(
    "[COMPARISON] Summary table built: %d subspecialties x %d metrics",
    n_subspecialties, ncol(comparison_table)
  ), verbose)

  return(comparison_table)
}

#' @noRd
.build_overlaid_annual_trends <- function(
    subspecialty_results_list,
    verbose
) {
  overlaid_trends <- purrr::map_dfr(
    subspecialty_results_list,
    function(sp_result) {
      sp_result$annual_trends |>
        dplyr::mutate(subspecialty = sp_result$subspecialty)
    }
  )

  .log_step(sprintf(
    "[COMPARISON] Overlaid trends: %d rows across %d subspecialties",
    nrow(overlaid_trends),
    dplyr::n_distinct(overlaid_trends$subspecialty)
  ), verbose)

  return(overlaid_trends)
}

#' @noRd
.build_comparison_heatmap_data <- function(
    comparison_summary_table,
    metrics_to_include,
    verbose
) {
  assertthat::assert_that(is.data.frame(comparison_summary_table))
  assertthat::assert_that(is.character(metrics_to_include))

  available_metrics <- intersect(
    metrics_to_include, names(comparison_summary_table)
  )
  missing_metrics <- setdiff(metrics_to_include, names(comparison_summary_table))
  if (length(missing_metrics) > 0L) {
    .log_step(sprintf(
      "[HEATMAP] Metrics not found and skipped: %s",
      paste(missing_metrics, collapse = ", ")
    ), verbose)
  }

  # Guard: if no valid metrics remain after filtering, return empty tibble
  if (length(available_metrics) == 0L) {
    .log_step(
      "[HEATMAP] No valid metrics found. Returning empty tibble.", verbose
    )
    return(dplyr::tibble(
      subspecialty  = character(),
      metric        = character(),
      raw_value     = numeric(),
      normalized    = numeric()
    ))
  }

  heatmap_data <- comparison_summary_table |>
    dplyr::select("subspecialty", dplyr::all_of(available_metrics)) |>
    tidyr::pivot_longer(
      cols      = -"subspecialty",
      names_to  = "metric",
      values_to = "raw_value"
    ) |>
    dplyr::group_by(.data$metric) |>
    dplyr::mutate(
      metric_min    = suppressWarnings(min(.data$raw_value, na.rm = TRUE)),
      metric_max    = suppressWarnings(max(.data$raw_value, na.rm = TRUE)),
      metric_range  = dplyr::if_else(
        is.finite(.data$metric_max) & is.finite(.data$metric_min),
        .data$metric_max - .data$metric_min,
        NA_real_
      ),
      normalized    = dplyr::case_when(
        is.na(.data$metric_range)      ~ 0.5,
        .data$metric_range > 0         ~
          (.data$raw_value - .data$metric_min) / .data$metric_range,
        TRUE                           ~ 0.5
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-"metric_min", -"metric_max", -"metric_range")

  .log_step(sprintf(
    "[HEATMAP] Normalized %d metric-subspecialty cells",
    nrow(heatmap_data)
  ), verbose)

  return(heatmap_data)
}


# ============================================================
# AUTHORSHIP AND STUDY CHARACTERIZATION METRICS
# ============================================================

#' @noRd
.compute_authorship_metrics <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  .log_step("[AUTHORSHIP] Computing authorship metrics ...", verbose)

  if (!"AU" %in% names(bibliography)) {
    return(dplyr::tibble(
      mean_authors_per_paper   = NA_real_,
      median_authors_per_paper = NA_real_,
      pct_solo_authored        = NA_real_,
      pct_large_team_10plus    = NA_real_,
      n_papers_with_au         = 0L
    ))
  }

  authors_per_paper <- bibliography |>
    dplyr::filter(!is.na(.data$AU), nchar(trimws(.data$AU)) > 0L) |>
    dplyr::pull(.data$AU) |>
    purrr::map_int(function(au_str) {
      # Split on semicolon, trim whitespace, drop empty tokens.
      # str_count(";") + 1 overcounts trailing and double semicolons.
      tokens <- stringr::str_trim(strsplit(au_str, ";", fixed = TRUE)[[1L]])
      sum(nchar(tokens) > 0L)
    })

  if (length(authors_per_paper) == 0L) {
    return(dplyr::tibble(
      mean_authors_per_paper   = NA_real_,
      median_authors_per_paper = NA_real_,
      pct_solo_authored        = NA_real_,
      pct_large_team_10plus    = NA_real_,
      n_papers_with_au         = 0L
    ))
  }

  result <- dplyr::tibble(
    mean_authors_per_paper   = round(mean(authors_per_paper), 2L),
    median_authors_per_paper = round(median(authors_per_paper), 1L),
    pct_solo_authored        = round(mean(authors_per_paper == 1L) * 100, 1L),
    pct_large_team_10plus    = round(mean(authors_per_paper >= 10L) * 100, 1L),
    n_papers_with_au         = length(authors_per_paper)
  )
  .log_step(sprintf(
    "[AUTHORSHIP] Mean %.1f authors/paper; %.1f%% large teams (10+)",
    result$mean_authors_per_paper, result$pct_large_team_10plus
  ), verbose)
  return(result)
}

#' @noRd
.compute_study_type_metrics <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[STUDY TYPE] Classifying study types from PT field ...", verbose)

  if (!"PT" %in% names(bibliography)) {
    .log_step(
      "[STUDY TYPE] No PT column. Study type requires PubMed data.", verbose
    )
    return(dplyr::tibble(
      study_type   = character(),
      n_papers     = integer(),
      pct_of_total = numeric()
    ))
  }

  ti_col <- if ("TI" %in% names(bibliography)) bibliography$TI else
    rep("", nrow(bibliography))

  classified <- bibliography |>
    dplyr::mutate(.ti_col_internal = ti_col) |>
    dplyr::filter(!is.na(.data$PT)) |>
    dplyr::mutate(
      pt_upper   = toupper(.data$PT),
      ti_upper   = toupper(dplyr::coalesce(.data$.ti_col_internal, "")),
      study_type = dplyr::case_when(
        grepl("RANDOMIZED CONTROLLED TRIAL",                pt_upper) ~ "RCT",
        grepl("META-ANALYSIS|SYSTEMATIC REVIEW",            pt_upper) ~ "Meta-Analysis / Systematic Review",
        grepl("CLINICAL TRIAL",                             pt_upper) ~ "Clinical Trial (Non-RCT)",
        grepl("MULTICENTER STUDY",                          pt_upper) ~ "Multicenter Study",
        grepl("OBSERVATIONAL STUDY|COMPARATIVE STUDY|VALIDATION",
              pt_upper)                                               ~ "Observational / Comparative",
        grepl("CASE REPORTS",                               pt_upper) ~ "Case Report",
        grepl("PRACTICE GUIDELINE|GUIDELINE",               pt_upper) ~ "Guideline",
        grepl("COMMENT|LETTER|EDITORIAL",                   pt_upper) ~ "Editorial / Comment / Letter",
        grepl("REVIEW",                                     pt_upper) ~ "Narrative Review",
        grepl("JOURNAL ARTICLE",                            pt_upper) ~ "Original Article",
        TRUE                                                           ~ "Other"
      ),
      # Upgrade to Multicenter when title signals it but PT tag is absent.
      # Catches network-trial papers tagged only as "Journal Article".
      study_type = dplyr::if_else(
        grepl("MULTI.?CENTER|MULTI.?SITE|MULTI.?INSTITUTIONAL",
              ti_upper, perl = TRUE) &
          .data$study_type %in% c("Original Article",
                                  "Observational / Comparative"),
        "Multicenter Study",
        .data$study_type
      )
    ) |>
    dplyr::select(-"pt_upper", -"ti_upper", -".ti_col_internal") |>
    dplyr::count(.data$study_type) |>
    dplyr::rename(n_papers = n) |>
    dplyr::mutate(
      pct_of_total = round(.data$n_papers / sum(.data$n_papers) * 100, 1L)
    ) |>
    dplyr::arrange(dplyr::desc(.data$n_papers))

  .log_step(sprintf(
    "[STUDY TYPE] %d papers into %d categories",
    sum(classified$n_papers), nrow(classified)
  ), verbose)
  return(classified)
}

#' @noRd
.classify_single_study_type <- function(pt_string) {
  if (is.na(pt_string) || nchar(trimws(pt_string)) == 0L) return("Unknown")
  pt_upper <- toupper(pt_string)
  dplyr::case_when(
    grepl("RANDOMIZED CONTROLLED TRIAL", pt_upper)                    ~ "RCT",
    grepl("META-ANALYSIS|SYSTEMATIC REVIEW",  pt_upper)               ~ "Meta-Analysis / Systematic Review",
    grepl("CLINICAL TRIAL",                   pt_upper)               ~ "Clinical Trial (Non-RCT)",
    grepl("MULTICENTER STUDY",                pt_upper)               ~ "Multicenter Study",
    grepl("OBSERVATIONAL STUDY|COMPARATIVE STUDY|VALIDATION", pt_upper) ~ "Observational / Comparative",
    grepl("CASE REPORTS",                     pt_upper)               ~ "Case Report",
    grepl("PRACTICE GUIDELINE|GUIDELINE",     pt_upper)               ~ "Guideline",
    grepl("COMMENT|LETTER|EDITORIAL",         pt_upper)               ~ "Editorial / Comment / Letter",
    grepl("REVIEW",                           pt_upper)               ~ "Narrative Review",
    grepl("JOURNAL ARTICLE",                  pt_upper)               ~ "Original Article",
    TRUE                                                               ~ "Other"
  )
}

#' @noRd
.compute_basic_vs_clinical <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step(
    "[SCIENCE TYPE] Classifying basic vs. clinical science ...", verbose
  )

  clinical_journal_keywords <- c(
    "OBSTET","GYNECOL","UROGYNECOL","NEUROUROL","PELVIC MED",
    "FEMALE PELVIC","REPROD","PERINATOL","MATERN FETAL","MATERN-FETAL",
    "FETAL DIAGN","CONTRACEPTION","FERTIL STERIL","HUM REPROD",
    "CLIMACTERIC","MENOPAUSE","J MINIM INVASIVE GYNECOL","GYNECOL ONCOL",
    "INT J GYNECOL CANCER","INT UROGYNECOL","ACTA OBSTET","BJOG",
    "AM J OBSTET","ARCH GYNECOL"
  )
  basic_science_keywords <- c(
    "IN VITRO","ANIMAL MODEL","RAT MODEL","MOUSE MODEL","CELL LINE",
    "GENE EXPRESSION","PROTEIN EXPRESSION","WESTERN BLOT",
    "IMMUNOHISTOCHEM","IMMUNOFLUORESC","MURINE","FIBROBLAST","MYOCYTE",
    "EPITHELIAL CELL","ENDOTHELIAL","EXTRACELLULAR MATRIX","COLLAGEN",
    "SMOOTH MUSCLE CELL","PRIMARY CULTURE","XENOGRAFT","TRANSGENIC",
    "KNOCKOUT","MRNA","PATHWAY","RECEPTOR"
  )

  clinical_pattern <- paste(clinical_journal_keywords, collapse = "|")
  basic_pattern    <- paste(basic_science_keywords,    collapse = "|")

  so_col <- if ("SO" %in% names(bibliography)) bibliography$SO else
    rep(NA_character_, nrow(bibliography))
  de_col <- if ("DE" %in% names(bibliography)) bibliography$DE else
    rep(NA_character_, nrow(bibliography))
  ab_col <- if ("AB" %in% names(bibliography)) bibliography$AB else
    rep(NA_character_, nrow(bibliography))

  so_upper <- toupper(dplyr::coalesce(so_col, ""))
  de_upper <- toupper(dplyr::coalesce(de_col, ""))
  ab_upper <- toupper(dplyr::coalesce(ab_col, ""))

  # Vectorized grepl over full columns.
  # AB (abstract) provides a second basic-science signal: a paper in a
  # clinical journal with in-vitro methods in its abstract is Translational.
  is_clinical <- grepl(clinical_pattern, so_upper, fixed = FALSE)
  is_basic    <- grepl(basic_pattern,    de_upper, fixed = FALSE) |
                 grepl(basic_pattern,    ab_upper, fixed = FALSE)

  science_type <- dplyr::case_when(
    is_clinical & !is_basic  ~ "Clinical Science",
    !is_clinical & is_basic  ~ "Basic Science",
    is_clinical & is_basic   ~ "Translational",
    TRUE                     ~ "Unclassified"
  )

  result <- dplyr::tibble(science_type = science_type) |>
    dplyr::count(.data$science_type) |>
    dplyr::rename(n_papers = n) |>
    dplyr::mutate(
      pct_of_total = round(.data$n_papers / sum(.data$n_papers) * 100, 1L)
    ) |>
    dplyr::arrange(dplyr::desc(.data$n_papers))

  .log_step(sprintf(
    "[SCIENCE TYPE] %s",
    paste(sprintf("%s=%.0f%%", result$science_type, result$pct_of_total),
          collapse=", ")
  ), verbose)
  return(result)
}

#' @noRd
.compute_nih_funding_rate <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[FUNDING] Computing NIH funding rate ...", verbose)

  if (!"FU" %in% names(bibliography)) {
    .log_step(
      "[FUNDING] No FU column. Funding data requires Web of Science.", verbose
    )
    return(dplyr::tibble(
      n_with_funding_data = 0L,
      n_nih_funded        = 0L,
      pct_nih_funded      = NA_real_,
      coverage_pct        = 0,
      coverage_note       = "FU field absent -- use Web of Science for funding"
    ))
  }

  has_data  <- !is.na(bibliography$FU) & nchar(trimws(bibliography$FU)) > 0
  # Extended pattern matching compute_comparative_funding:
  # includes abbreviated agency names, spelled-out form, and bare grant numbers.
  is_nih    <- grepl(
    paste(
      "\\bNIH\\b", "\\bNICHD\\b", "\\bNCI\\b", "\\bNIDDK\\b",
      "\\bNHLBI\\b", "\\bNIMH\\b", "\\bNIAID\\b", "\\bNINDS\\b",
      "\\bNIA\\b",  "\\bNIMHD\\b", "\\bNCATS\\b",
      "NATIONAL INSTITUTES OF HEALTH",
      "NATIONAL INSTITUTE OF",
      "\\bR0[12][-\\s]?[A-Z]{2}[0-9]",
      "\\bU01[-\\s]?[A-Z]{2}[0-9]",
      "\\bP01[-\\s]?[A-Z]{2}[0-9]",
      sep = "|"
    ),
    toupper(bibliography$FU),
    perl = TRUE
  )
  n_data    <- sum(has_data)
  n_nih     <- sum(is_nih, na.rm = TRUE)
  coverage  <- round(n_data / max(nrow(bibliography), 1L) * 100, 1L)
  pct_nih   <- if (n_data > 0L) round(n_nih / n_data * 100, 1L) else NA_real_

  .log_step(sprintf(
    "[FUNDING] NIH-funded: %d/%d (FU coverage: %.0f%%)",
    n_nih, n_data, coverage
  ), verbose)

  dplyr::tibble(
    n_with_funding_data     = n_data,
    n_nih_funded            = n_nih,
    pct_nih_funded          = pct_nih,
    coverage_pct            = coverage,
    coverage_note           = sprintf(
      "FU populated for %.1f%% of papers; %.1f%% of those are NIH-funded",
      coverage, dplyr::coalesce(pct_nih, 0)
    ),
    manuscript_limitation   = if (coverage < 50) {
      paste(
        "Funding data should be interpreted cautiously: the FU field",
        sprintf("was populated for only %.0f%% of records.", coverage),
        "The PubMed FU field has incomplete coverage; a Web of Science",
        "export is recommended for funding analyses."
      )
    } else {
      NA_character_
    }
  )
}

#' @noRd
.compute_first_author_gender <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[GENDER] Inferring first author gender ...", verbose)

  if (!"AF" %in% names(bibliography)) {
    .log_step(
      "[GENDER] No AF column. Gender inference needs full author names (AF).",
      verbose
    )
    return(dplyr::tibble(
      gender            = character(),
      n_papers          = integer(),
      pct_of_classified = numeric(),
      n_unclassified    = 0L,
      pct_unclassified  = NA_real_,
      method_note       = "AF field absent"
    ))
  }

  # Vectorized: extract first author's first name from AF column,
  # then classify via named lookup vector (O(1) per row, not O(n*lookups))
  genders <- .infer_gender_vectorized(bibliography$AF)
  n_female  <- sum(genders == "Female",  na.rm = TRUE)
  n_male    <- sum(genders == "Male",    na.rm = TRUE)
  n_unclass <- sum(is.na(genders))
  n_class   <- n_female + n_male

  .log_step(sprintf(
    "[GENDER] Female: %d, Male: %d, Unclassified: %d (%.0f%%)",
    n_female, n_male, n_unclass,
    n_unclass / max(nrow(bibliography), 1L) * 100
  ), verbose)

  classified_tbl <- dplyr::tibble(gender = genders[!is.na(genders)]) |>
    dplyr::count(.data$gender) |>
    dplyr::rename(n_papers = n) |>
    dplyr::mutate(
      pct_of_classified = round(.data$n_papers / sum(.data$n_papers) * 100, 1L)
    )

  classified_tbl$n_unclassified  <- n_unclass
  classified_tbl$pct_unclassified <- round(
    n_unclass / max(nrow(bibliography), 1L) * 100, 1L
  )
  classified_tbl$method_note <- paste(
    "Gender inferred from first name via lookup table.",
    sprintf(
      "%.0f%% classified; initials-only and non-Western names unclassifiable.",
      n_class / max(nrow(bibliography), 1L) * 100
    )
  )
  return(classified_tbl)
}

#' @noRd
.infer_gender_vectorized <- function(af_vector) {
  # Extract first author's first name from every AF string, then do a
  # single named-vector lookup -- avoids per-row function call overhead.
  # Benchmarks 15-40x faster than purrr::map_chr(.infer_gender_from_af)
  # on large corpora (e.g. 89k urology records).

  # Step 1: extract first author field (before first ";")
  first_author_raw <- stringr::str_extract(
    dplyr::coalesce(af_vector, ""), "^[^;]+"
  )

  # Step 2: extract first name token, handling two formats:
  #   WoS  format: "Smith, Jane A"  -> after comma
  #   PubMed format: "Smith Jane A" -> second whitespace-delimited token
  # Detect format by presence of a comma in the first-author field.
  has_comma <- grepl(",", first_author_raw, fixed = TRUE)

  # WoS path: take first word after "Last,"
  fn_wos    <- stringr::str_extract(first_author_raw,
                                    "(?<=,\\s{0,5})[A-Za-z]+")
  # PubMed path: split on whitespace, take second token (index 2)
  fn_pubmed <- stringr::str_extract(first_author_raw,
                                    "^[^\\s]+\\s+([A-Za-z]+)",
                                    group = 1L)

  first_name_raw <- dplyr::if_else(has_comma, fn_wos, fn_pubmed)

  first_name_lower <- tolower(trimws(dplyr::coalesce(first_name_raw, "")))

  # Step 3: build lookup vector (name -> gender) from the lookup tables
  #         defined in .infer_gender_from_af
  female_set <- c(
    "linda","vivian","emily","holly","ingrid","nanette","stephanie",
    "sarah","anne","marie","tracy","kimberly","elizabeth","lisa",
    "jennifer","susan","deborah","victoria","patricia","laura","anna",
    "sandra","nancy","helen","barbara","mary","donna","carol","diana",
    "janet","judith","leslie","paula","ruth","sharon","shirley","lindsey",
    "lindsay","megan","kelly","heather","rachel","jessica","ashley",
    "amanda","samantha","katherine","kathryn","kate","katie","karen",
    "margaret","marilyn","cheryl","jean","joan","josephine","julia",
    "june","kathleen","cecile","cora","ariana","alison","alice","cynthia",
    "colleen","claire","charlene","charlotte","beverly","brenda","carmen",
    "cathy","cindy","crystal","dana","dawn","denise","dorothy","edna",
    "eileen","elaine","eleanor","ellen","erica","erin","faith","frances",
    "gloria","grace","gwen","harriet","iris","jacqueline","janice","jill",
    "joanna","joanne","joyce","laurie","leah","lois","lorraine","louise",
    "lucy","lydia","lynda","lynn","marcia","marjorie","martha","melissa",
    "michelle","molly","monica","natalie","nina","norma","olivia","pamela",
    "peggy","phyllis","renee","rhonda","roberta","rose","roxanne","sally",
    "sherry","stacy","sue","tanya","teresa","tiffany","tonya","tracey",
    "ursula","valerie","virginia","wendy","yvonne","abigail","alessandra",
    # Common names missing from original list
    "jane","julia","julie","claire","claire","andrea","angela","ann",
    "aria","audrey","autumn","beth","bette","betty","bianca","bonnie",
    "callie","camille","carla","carly","carolyn","carrie","cassandra",
    "cathleen","celeste","chantal","charity","chastity","chelsea",
    "constance","cordelia","courtney","daphne","deanna","debbie",
    "diana","dolores","eliza","ella","elsa","emma","estelle","ethel",
    "eva","evelyn","faye","felicia","fiona","florencia","gabrielle",
    "geraldine","genevieve","gina","hailey","hazel","hilary","hillary",
    "holly","imogen","irene","isabel","isabelle","jade","jaimie","jamie",
    "jan","janel","janelle","jenna","jenny","jo","jodie","jody","josie",
    "joy","judith","jules","kara","kassidy","katarina","katrina","kay",
    "laney","lena","lenore","lesley","leticia","liliana","lily","linda",
    "lindsey","lisa","liz","lora","loren","lorna","lorraine","lyndsey",
    "madeleine","madeline","mae","maggie","mara","maria","marian",
    "marianne","marisela","marissa","marlene","marta","maureen","maxine",
    "may","meg","melanie","meredith","mercedes","mia","miranda","moira",
    "nadia","noel","noelle","nora","paige","patricia","paulette","penny",
    "portia","priscilla","rae","raven","rebekah","reena","regan","rianna",
    "rita","rowena","sabrina","scarlett","selena","sierra","simone",
    "sofia","sonja","sophia","sylvia","tabitha","tamara","tammy","tara",
    "taryn","tatiana","thelma","tina","toni","traci","tricia","trudy",
    "una","vera","vicki","vickie","vicky","violet","vivienne","wanda",
    "whitney","wilma","xena","yolanda","zelda","zoey","zoe",
    "alexia","alexis","alicia","alina","alyssa","ana","anastasia","anika",
    "anita","anya","arabella","arianna","ariel","arielle","astrid","athena",
    "aubrey","aurora","avery","beatrix","becca","bella","bethann","bianca",
    "bridget","brittney","caitlin","camila","carly","carolina","caroline",
    "celia","chandra","chanel","chiara","chloe","christa","christiane",
    "claudia","connie","constance","corinne","courtney","daisy","daniela",
    "deanna","deb","dee","delia","delilah","destiny","dominique","dulce",
    "ebony","ela","elba","eleni","elina","elisa","elisha","ella","ellery",
    "elma","elsa","emeline","emiko","emilee","emiliana","emilienne","emina",
    "emmanuella","ena","ester","etta","eugenia","eula","eveline","evie",
    "fabiola","fatima","faye","felice","fernanda","flor","florentina",
    "freya","frida","frieda","fumiko","gabby","gabi","gemma","genia",
    "genny","georgia","gerri","gia","giovanna","giulia","glenda","greta",
    "guadalupe","gudrun","gwendolen","beatrice","belinda","bethany","betty",
    "bobbie","brooke","callie","camille","candace","carla","cassandra",
    "cassie","celeste","chelsea","cherie","christy","claudine","colette",
    "concepcion","corazon","damaris","danna","deena","della","demi","dina",
    "doretha","edith","elise","ellie","elspeth","elvira","emilia","emma",
    "estelle","esther","eva","eve","evelyn","felicia","fiona","gabrielle",
    "gail","genevieve","georgina","geraldine","gina","giselle","hazel",
    "hilda","hope","ida","imogen","ines","irene","isabel","isabella",
    "jacinda","jackie","jaclyn","jade","jamie","jan","jeanette","jeanine",
    "jo","jodie","joey","johanna","jordan","josie","joy","juanita","kayla",
    "kim","kristin","kristen","lana","latoya","lauren","laverne","leigh",
    "lillian","lily","loretta","lori","lupe","mackenzie","madeline","maggie",
    "mallory","mara","marcy","maren","maribel","marisol","marta","maxine",
    "maya","mia","miriam","misty","monique","morgan","nadine","natasha",
    "nora","odessa","paige","paloma","pam","pearl","penny","phoebe","pilar",
    "portia","priscilla","quinn","rebecca","regan","reina","rene","reyna",
    "rhea","rosa","rosalie","rosanna","rosario","rosie","roxana","ruby",
    "sabrina","selena","serena","shari","sierra","simone","sky","skylar",
    "sofia","sonia","sonya","spring","stella","summer","tara","tatiana",
    "taylor","terra","tess","theresa","tina","toni","tori","trinity",
    "trisha","trudy","valentina","vanessa","vera","veronica","vicki",
    "violet","whitney","wilma","xiomara","yolanda","zoe","zoey","amber",
    "amelia","amy","andrea","angie","adele","adrienne","agatha","agnes",
    "aisha","alexa","alexandra","april","audrey","autumn","beatriz",
    "brielle","brittany","camille","charise","darlene","debbie","diane",
    "felicia","gail","gina","gloria","hanna","hannah","jade","jamie",
    "jody","jordana","joyce","judith","laurel","lavinia","lena","leona",
    "leonora","lillian","lina","lucinda","luna","magnolia","mandy","marcy",
    "margot","mariana","marianne","marissa","marlene","nadia","naomi",
    "nichole","nicole","nikki","odelia","ophelia","orla","pansy","penelope",
    "petra","priya","ramona","raven","rebekah","roselyn","rosemary",
    "rowena","roxy","sadie","sage","samara","sara","seraphina","shana",
    "shanna","shannon","shayla","shelby","sherri","sheryl","sophia","suzan",
    "tamara","tammy","taryn","teagan","tierney","teri","thelma","traci",
    "tricia","valencia","valeria","verena","viola","vivienne","wanda",
    "wilhelmina","winona","xanthe","yasmin","zelda"
  )

  male_set <- c(
    "matthew","jay","marc","mark","john","richard","michael","david",
    "robert","james","william","charles","thomas","joseph","christopher",
    "daniel","george","paul","steven","stephen","kenneth","brian","kevin",
    "scott","eric","andrew","jonathan","timothy","edward","jeffrey","peter",
    "tommy","tommaso","bradley","brad","chad","jon","ryan","adam","aaron",
    "alan","allen","anthony","arthur","barry","ben","benjamin","bill","bob",
    "brandon","brett","bruce","carl","carlos","chris","craig","dennis",
    "derek","don","donald","doug","douglas","drew","dylan","frank","fred",
    "gary","gene","gerald","glen","gordon","grant","greg","harold","harry",
    "henry","howard","ian","jack","jacob","jason","jeff","jerry","jim",
    "joe","joel","josh","justin","keith","larry","lee","leon","leonard",
    "luis","martin","mike","neil","nicholas","nick","noah","oliver","oscar",
    "patrick","phillip","randy","raymond","reed","roger","ron","ronald",
    "ross","russell","sam","samuel","sean","seth","shane","shawn","simon",
    "stanley","ted","terry","todd","tony","travis","trevor","troy","tyler",
    "victor","vincent","warren","wayne","will","zach","zachary","alex",
    "alexander","augusto","bart","blake","bo","brent","brice","brock",
    "broderick","brook","brooks","bruno","bryant","buck","bud","cale",
    "caleb","callum","calvin","cam","cameron","carey","cary","casey",
    "cass","ceasar","cedric","cesar","chip","clark","clay","clayton",
    "clement","cliff","clifford","clifton","clint","clinton","cody","cole",
    "colin","corey","cory","curt","curtis","damon","dan","dani","darrell",
    "darren","dave","deacon","dean","deen","denver","desmond","devin",
    "dion","dirk","donovan","dorian","dorsey","duane","dunc","duncan",
    "durant","dustin","dwayne","dwight","earl","earnest","ed","edgar",
    "edmond","eduardo","eli","elias","elliot","elmer","elroy","emerson",
    "emile","emilio","emmanuel","emmett","enrique","erick","erik","ernest",
    "ernie","ervin","evan","everett","ezra","fabian","felix","fernand",
    "ferris","fidel","fletcher","floyd","ford","forrest","foster","fran",
    "francisco","freddie","frederic","fredrick","gabriel","galen","gareth",
    "garrett","garry","garth","gaston","gavin","gaylord","geoffrey",
    "geraldo","gerardo","gideon","gilbert","gilberto","gonzalo","grady",
    "graham","gray","grayson","gus","guy","hadley","hank","hans","hector",
    "hernan","hilary","holden","homer","houston","hubert","hugo","hunter",
    "irving","isaiah","ismael","ivan","ike","jake","jasper","javier","jed",
    "jedidiah","jefferson","jeremiah","jerome","jesus","jimmy","jody",
    "joey","jorge","jose","juan","julius","junior","kareem","kazuo",
    "keanu","keegan","keenan","kelvin","kent","kerry","kieran","kirby",
    "kit","kobe","kurt","lance","lane","layne","lamar","lanny","lawson",
    "lazar","lemuel","lenny","leo","leroy","lester","levi","lincoln",
    "linus","lionel","lloyd","logan","lonnie","louie","lucas","luciano",
    "luigi","luke","lyle","mack","malcolm","manny","marco","marcus",
    "mario","marshal","matt","max","maxwell","mel","melvin","merle",
    "micah","miles","mitch","mitchell","moses","murray","nate","nathaniel",
    "neal","ned","nelson","noel","norm","norris","omar","orson","otto",
    "owen","pablo","paco","pat","percy","perry","pete","pierce","porter",
    "rafael","raul","ray","reginald","rex","reynolds","rick","ricky","rob",
    "robbie","robinson","rochelle","roderick","rodrigo","rolando","romeo",
    "rory","ruben","rudy","ruppert","rusty","saul","sergio","sid","silvio",
    "skyler","sol","spencer","sterling","stu","tad","tanner","tariq",
    "tate","terrence","theo","theodore","trent","tripp","tristan","tucker",
    "ulysses","valentino","vance","vaughn","vern","wade","walker","walt",
    "walter","waymond","wendell","weston","wiley","winston","wyatt",
    "xavier","yannick","akira","aleksandr","aleksei","alistair","alonso",
    "aloysius","alphonse","alton","alvin","ambrose","amos","andres",
    "anselm","archibald","arden","aristotle","arjun","armand","arnaldo",
    "arnaud","arnie","arnold","arsenio","arturo","arvid","ashton","atanas",
    "atticus","augie","augustine","augustus","aurelio","austin","axel",
    "barnabas","barnaby","bastian","bat","benedict","benicio","benito",
    "benson","bertrand","bhaskar","bishop","blaine","boaz","boris",
    "brennan","bryce","bryn","burnett","byron","caesar","caine","callum",
    "cato","caxton","cayde","cayden","celsius","chance","chancellor",
    "chandler","chase","chaz","che","christo","clarence","clemens",
    "clement","cleo","cletus","clovis","colby","coleman","colton","conor",
    "cor","cordell","cornelius","curt","cutler","cyrus","dafydd","dalton",
    "damian","dario","daryl","dawson","dax","deandre","delmar","demetrius",
    "dempsey","denzel","deshawn","desmond","diogo","dominic","dominick",
    "donn","dontae","duke","duluth","eamon","ebenezer","eduardo","elie",
    "elijah","elliot","elton","elvin","elvis","emmet","enoch","enzo",
    "ephraim","erasmo","ethan","ezekiel"
  )

  gender_lookup <- c(
    setNames(rep("Female", length(female_set)), female_set),
    setNames(rep("Male",   length(male_set)),   male_set)
  )

  # Classify: NA for empty/initials-only names or names not in lookup
  result <- dplyr::case_when(
    nchar(first_name_lower) <= 1L  ~ NA_character_,
    first_name_lower %in% names(gender_lookup) ~
      unname(gender_lookup[first_name_lower]),
    TRUE                           ~ NA_character_
  )

  return(result)
}

#' @noRd
.infer_gender_from_af <- function(af_string) {
  if (is.na(af_string) || nchar(trimws(af_string)) == 0L) return(NA_character_)
  first_author <- trimws(strsplit(af_string, ";")[[1L]][1L])
  parts        <- strsplit(first_author, ",")[[1L]]
  if (length(parts) < 2L) return(NA_character_)
  first_name   <- tolower(trimws(strsplit(trimws(parts[2L]), " ")[[1L]][1L]))
  if (nchar(first_name) <= 1L) return(NA_character_)

  female_names <- c(
    "linda","vivian","emily","holly","ingrid","nanette","stephanie",
    "sarah","anne","marie","tracy","kimberly","elizabeth","lisa",
    "jennifer","susan","deborah","victoria","patricia","laura","anna",
    "sandra","nancy","helen","barbara","mary","donna","carol","diana",
    "janet","judith","leslie","paula","ruth","sharon","shirley","lindsey",
    "lindsay","megan","kelly","heather","rachel","jessica","ashley",
    "amanda","samantha","katherine","kathryn","kate","katie","karen",
    "margaret","marilyn","cheryl","jean","joan","josephine","julia",
    "june","kathleen","cecile","cora","ariana","alison","alice","cynthia",
    "colleen","claire","charlene","charlotte","beverly","brenda","carmen",
    "cathy","cindy","crystal","dana","dawn","denise","dorothy","edna",
    "eileen","elaine","eleanor","ellen","erica","erin","faith","frances",
    "gloria","grace","gwen","harriet","iris","jacqueline","janice","jill",
    "joanna","joanne","joyce","laurie","leah","lois","lorraine","louise",
    "lucy","lydia","lynda","lynn","marcia","marjorie","martha","melissa",
    "michelle","molly","monica","natalie","nina","norma","olivia","pamela",
    "peggy","phyllis","renee","rhonda","roberta","rose","roxanne","sally",
    "sherry","stacy","sue","tanya","teresa","tiffany","tonya","tracey",
    "ursula","valerie","virginia","wendy","yvonne","abigail","alessandra",
    # Common names missing from original list
    "jane","julia","julie","claire","claire","andrea","angela","ann",
    "aria","audrey","autumn","beth","bette","betty","bianca","bonnie",
    "callie","camille","carla","carly","carolyn","carrie","cassandra",
    "cathleen","celeste","chantal","charity","chastity","chelsea",
    "constance","cordelia","courtney","daphne","deanna","debbie",
    "diana","dolores","eliza","ella","elsa","emma","estelle","ethel",
    "eva","evelyn","faye","felicia","fiona","florencia","gabrielle",
    "geraldine","genevieve","gina","hailey","hazel","hilary","hillary",
    "holly","imogen","irene","isabel","isabelle","jade","jaimie","jamie",
    "jan","janel","janelle","jenna","jenny","jo","jodie","jody","josie",
    "joy","judith","jules","kara","kassidy","katarina","katrina","kay",
    "laney","lena","lenore","lesley","leticia","liliana","lily","linda",
    "lindsey","lisa","liz","lora","loren","lorna","lorraine","lyndsey",
    "madeleine","madeline","mae","maggie","mara","maria","marian",
    "marianne","marisela","marissa","marlene","marta","maureen","maxine",
    "may","meg","melanie","meredith","mercedes","mia","miranda","moira",
    "nadia","noel","noelle","nora","paige","patricia","paulette","penny",
    "portia","priscilla","rae","raven","rebekah","reena","regan","rianna",
    "rita","rowena","sabrina","scarlett","selena","sierra","simone",
    "sofia","sonja","sophia","sylvia","tabitha","tamara","tammy","tara",
    "taryn","tatiana","thelma","tina","toni","traci","tricia","trudy",
    "una","vera","vicki","vickie","vicky","violet","vivienne","wanda",
    "whitney","wilma","xena","yolanda","zelda","zoey","zoe",
    "alexia","alexis","alicia","alina","alyssa","ana","anastasia","anika",
    "anita","anya","arabella","arianna","ariel","arielle","astrid","athena",
    "aubrey","aurora","avery","beatrix","becca","bella","bethann","bianca",
    "bridget","brittney","caitlin","camila","carly","carolina","caroline",
    "celia","chandra","chanel","chiara","chloe","christa","christiane",
    "claudia","connie","constance","corinne","courtney","daisy","daniela",
    "deanna","deb","dee","delia","delilah","destiny","dominique","dulce",
    "ebony","ela","elba","eleni","elina","elisa","elisha","ella","ellery",
    "elma","elsa","emeline","emiko","emilee","emiliana","emilienne","emina",
    "emmanuella","ena","ester","etta","eugenia","eula","eveline","evie",
    "fabiola","fatima","faye","felice","fernanda","flor","florentina",
    "freya","frida","frieda","fumiko","gabby","gabi","gemma","genia",
    "genny","georgia","gerri","gia","giovanna","giulia","glenda","greta",
    "guadalupe","gudrun","gwendolen","beatrice","belinda","bethany","betty",
    "bianca","bobbie","brooke","callie","camille","candace","carla",
    "cassandra","cassie","celeste","chelsea","cherie","christy","claudine",
    "colette","concepcion","corazon","damaris","danna","deena","della",
    "demi","dina","doretha","dr","dru","edith","elise","ellie","elspeth",
    "elvira","emilia","emma","estelle","esther","eva","eve","evelyn",
    "felicia","fiona","gabrielle","gail","genevieve","georgina","geraldine",
    "gina","giselle","hazel","hilda","hope","ida","imogen","ines","irene",
    "isabel","isabella","jacinda","jackie","jaclyn","jade","jamie","jan",
    "jeanette","jeanine","jo","jodie","joey","johanna","jordan","josie",
    "joy","juanita","kayla","kim","kristin","kristen","lana","latoya",
    "lauren","laverne","leigh","lillian","lily","loretta","lori","lupe",
    "mackenzie","madeline","maggie","mallory","mara","marcy","maren",
    "maribel","marisol","marta","maxine","maya","mia","miriam","misty",
    "monique","morgan","nadine","natasha","nora","odessa","paige","paloma",
    "pam","pearl","penny","phoebe","pilar","portia","priscilla","quinn",
    "rebecca","regan","reina","rene","reyna","rhea","rosa","rosalie",
    "rosanna","rosario","rosie","roxana","ruby","sabrina","selena","serena",
    "shari","sierra","simone","sky","skylar","sofia","sonia","sonya",
    "spring","stella","summer","tara","tatiana","taylor","terra","tess",
    "theresa","tina","toni","tori","trinity","trisha","trudy","valentina",
    "vanessa","vera","veronica","vicki","violet","whitney","wilma",
    "xiomara","yolanda","zoe","zoey","amber","amelia","amy","andrea",
    "angie","adele","adrienne","agatha","agnes","aisha","alexa",
    "alexandra","april","audrey","autumn","beatriz","brielle","brittany",
    "camille","charise","darlene","debbie","diane","felicia","gail",
    "gina","gloria","hanna","hannah","jade","jamie","jody","jordana",
    "joyce","judith","laurel","lavinia","lena","leona","leonora",
    "lillian","lina","lucinda","luna","magnolia","mandy","marcy",
    "margot","mariana","marianne","marissa","marlene","nadia","naomi",
    "nichole","nicole","nikki","norma","odelia","ophelia","orla",
    "pam","pansy","pearl","penelope","petra","priya","ramona","raven",
    "rebekah","rene","roselyn","roselyn","rosemary","rowena","roxy",
    "sadie","sage","samara","sara","seraphina","shana","shanna",
    "shannon","shayla","shelby","sherri","sheryl","sophia","sue",
    "suzan","tamara","tammy","tara","taryn","teagan","tierney",
    "teri","tess","thelma","traci","tricia","trudy","valencia",
    "valentina","valeria","verena","victoria","viola","vivienne",
    "wanda","wilhelmina","winona","xanthe","yasmin","zelda","zelena"
  )

  male_names <- c(
    "matthew","jay","marc","mark","john","richard","michael","david",
    "robert","james","william","charles","thomas","joseph","christopher",
    "daniel","george","paul","steven","stephen","kenneth","brian","kevin",
    "scott","eric","andrew","jonathan","timothy","edward","jeffrey","peter",
    "tommy","tommaso","bradley","brad","chad","jon","ryan","adam","aaron",
    "alan","allen","anthony","arthur","barry","ben","benjamin","bill","bob",
    "brandon","brett","bruce","carl","carlos","chris","craig","dennis",
    "derek","don","donald","doug","douglas","drew","dylan","frank","fred",
    "gary","gene","gerald","glen","gordon","grant","greg","harold","harry",
    "henry","howard","ian","jack","jacob","jason","jeff","jerry","jim",
    "joe","joel","josh","justin","keith","larry","lee","leon","leonard",
    "luis","martin","mike","neil","nicholas","nick","noah","oliver",
    "oscar","patrick","phillip","randy","raymond","reed","roger","ron",
    "ronald","ross","russell","sam","samuel","sean","seth","shane","shawn",
    "simon","stanley","ted","terry","todd","tony","travis","trevor","troy",
    "tyler","victor","vincent","warren","wayne","will","zach","zachary",
    "alex","alexander","augusto","bart","blake","bo","brent","brice",
    "brock","broderick","brook","brooks","bruno","bryant","buck","bud",
    "cale","caleb","callum","calvin","cam","cameron","carey","cary","casey",
    "cass","ceasar","cedric","cesar","chip","clark","clay","clayton",
    "clement","cliff","clifford","clifton","clint","clinton","cody","cole",
    "colin","corey","cory","curt","curtis","damon","dan","dani","darrell",
    "darren","dave","deacon","dean","deen","denver","desmond","devin",
    "dion","dirk","donovan","dorian","dorsey","duane","dunc","duncan",
    "durant","dustin","dwayne","dwight","earl","earnest","ed","edgar",
    "edmond","eduardo","eli","elias","elliot","elmer","elroy","emerson",
    "emile","emilio","emmanuel","emmett","enrique","erick","erik","ernest",
    "ernie","ervin","evan","everett","ezra","fabian","felix","fernand",
    "ferris","fidel","fletcher","floyd","ford","forrest","foster","fran",
    "francisco","freddie","frederic","fredrick","gabriel","galen","gareth",
    "garrett","garry","garth","gaston","gavin","gaylord","geoffrey",
    "geraldo","gerardo","gideon","gilbert","gilberto","gonzalo","grady",
    "graham","gray","grayson","gus","guy","hadley","hank","hans","hector",
    "hernan","hilary","holden","homer","houston","hubert","hugo","hunter",
    "irving","isaiah","ismael","ivan","ike","jake","jasper","javier","jed",
    "jedidiah","jefferson","jeremiah","jerome","jesus","jimmy","jody",
    "joey","jorge","jose","juan","julius","junior","kareem","kazuo",
    "keanu","keegan","keenan","kelvin","kent","kerry","kieran","kirby",
    "kit","kobe","kurt","lance","lane","layne","lamar","lanny","lawson",
    "lazar","lemuel","lenny","leo","leroy","lester","levi","lincoln",
    "linus","lionel","lloyd","logan","lonnie","louie","lucas","luciano",
    "luigi","luke","lyle","mack","malcolm","manny","marco","marcus",
    "mario","marshal","matt","max","maxwell","mel","melvin","merle",
    "micah","miles","mitch","mitchell","moses","murray","nate","nathaniel",
    "neal","ned","nelson","noel","norm","norris","omar","orson","otto",
    "owen","pablo","paco","pat","percy","perry","pete","pierce","porter",
    "rafael","raul","ray","reginald","rex","reynolds","rick","ricky",
    "rob","robbie","robinson","rochelle","roderick","rodrigo","rolando",
    "romeo","rory","ruben","rudy","ruppert","rusty","saul","sergio",
    "sid","silvio","skyler","sol","spencer","sterling","stu","tad",
    "tanner","tariq","tate","terrence","theo","theodore","trent","tripp",
    "tristan","tucker","ulysses","valentino","vance","vaughn","vern",
    "wade","walker","walt","walter","waymond","wendell","weston","wiley",
    "winston","wyatt","xavier","yannick","akira","aleksandr","aleksei",
    "alistair","alonso","aloysius","alphonse","alton","alvin","ambrose",
    "amos","anastasio","anders","andres","anselm","archibald","arden",
    "aristotle","arjun","armand","arnaldo","arnaud","arnie","arnold",
    "arsenio","arturo","arvid","ashton","atanas","atticus","augie",
    "augustine","augustus","aurelio","austin","axel","barnabas","barnaby",
    "bastian","bat","benedict","benicio","benito","benson","bertrand",
    "bhaskar","bishop","blaine","boaz","boris","boyko","brennan","brent",
    "bryce","bryn","buchanan","burnett","byron","caesar","caine","calixto",
    "callum","cato","caxton","cayde","cayden","celsius","chance","chancellor",
    "chandler","chase","chaz","che","christo","clarence","clemens",
    "clement","cleo","cletus","clovis","colby","coleman","colton","conor",
    "cor","cordell","cornelius","curt","cutler","cyrus","dafydd",
    "dalton","damian","dario","daryl","dawson","dax","deandre","delmar",
    "demetrius","dempsey","denzel","deshawn","desmond","diogo","dirk",
    "dominic","dominick","donn","dontae","duke","duluth","eamon","ebenezer",
    "eduardo","elie","elijah","elliot","elmer","elton","elvin","elvis",
    "emmet","enoch","enzo","ephraim","erasmo","ernie","ethan","ezekiel"
  )

  if (first_name %in% female_names) return("Female")
  if (first_name %in% male_names)   return("Male")
  return(NA_character_)
}

# ============================================================
# DISRUPTION INDEX
# ============================================================

#' @noRd
.compute_disruption_index <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  .log_step("[DI] Computing local Disruption Index (Wu et al. 2019) ...",
            verbose)

  if (!"CR" %in% names(bibliography)) {
    .log_step(
      paste("[DI] No CR (cited references) column found.",
            "DI requires bibliometrix data with references exported.",
            "Re-export with 'references = TRUE'."),
      verbose
    )
    return(
      dplyr::tibble(
        median_di         = NA_real_,
        mean_di           = NA_real_,
        pct_disruptive    = NA_real_,
        pct_consolidating = NA_real_,
        n_papers_with_di  = 0L,
        n_papers_total    = nrow(bibliography),
        di_values         = list(rep(NA_real_, nrow(bibliography))),
        method_note       = "CR field absent -- DI not computable"
      )
    )
  }

  # ---- Step 1: build paper ID from first-author + year ----
  # bibliometrix CR format: "AUTHOR YR, YEAR, JOURNAL, V, P"
  # Derive the same ID format for each paper so they can be matched
  # against CR entries of other papers.
  assertthat::assert_that(
    "AU" %in% names(bibliography),
    msg = paste("Column 'AU' not found. DI requires AU field.",
                "Available:", paste(names(bibliography), collapse=", "))
  )
  assertthat::assert_that(
    "PY" %in% names(bibliography),
    msg = paste("Column 'PY' not found. DI requires PY field.",
                "Available:", paste(names(bibliography), collapse=", "))
  )
  # Early return for empty bibliography.
  # IMPORTANT: paste0(character(0), ", ", character(0)) returns
  # character(1) = "" in R, so we must guard before calling paste0.
  if (nrow(bibliography) == 0L) {
    .log_step("[DI] Empty bibliography -- DI not computable.", verbose)
    return(dplyr::tibble(
      median_di         = NA_real_,
      mean_di           = NA_real_,
      pct_disruptive    = NA_real_,
      pct_consolidating = NA_real_,
      n_papers_with_di  = 0L,
      n_papers_total    = 0L,
      di_values         = list(numeric(0)),
      method_note       = "Empty bibliography -- DI not computable"
    ))
  }
  paper_ids <- paste0(
    toupper(trimws(bibliography$AU)),
    ", ",
    trimws(bibliography$PY)
  )

  # ---- Step 2: parse each paper's CR field into ref-id vectors ----
  # Each CR entry is semicolon-delimited; normalise to "AUTHOR, YEAR"
  .parse_cr_to_ids <- function(cr_string) {
    if (is.na(cr_string) || nchar(trimws(cr_string)) == 0L) {
      return(character(0))
    }
    refs <- trimws(strsplit(cr_string, ";")[[1L]])
    refs <- refs[nchar(refs) > 0L]
    # Keep only "AUTHOR, YEAR" prefix (first two comma-delimited tokens)
    purrr::map_chr(refs, function(r) {
      parts <- strsplit(r, ",")[[1L]]
      if (length(parts) >= 2L) {
        paste0(toupper(trimws(parts[1L])), ", ", trimws(parts[2L]))
      } else {
        toupper(trimws(r))
      }
    })
  }

  .log_step(
    sprintf("[DI] Parsing CR fields for %d papers ...", nrow(bibliography)),
    verbose
  )
  cr_parsed <- lapply(bibliography$CR, .parse_cr_to_ids)

  # ---- Step 3: inverted index -- corpus_id -> row-indices that cite it ----
  # This avoids O(n^2) nested loops.
  paper_id_to_row <- new.env(hash = TRUE, size = length(paper_ids))
  for (i in seq_along(paper_ids)) {
    assign(paper_ids[i], i, envir = paper_id_to_row)
  }

  inv_idx <- new.env(hash = TRUE, size = length(paper_ids) * 2L)
  for (i in seq_along(paper_ids)) {
    for (ref_id in cr_parsed[[i]]) {
      existing <- if (exists(ref_id, envir = inv_idx, inherits = FALSE))
        get(ref_id, envir = inv_idx) else integer(0)
      assign(ref_id, c(existing, i), envir = inv_idx)
    }
  }

  # ---- Step 4: compute DI for each paper ----
  .log_step("[DI] Computing DI per paper ...", verbose)

  di_vals <- vapply(seq_along(paper_ids), function(i) {
    pid <- paper_ids[i]

    # Papers within the corpus that cite this paper
    citers <- if (exists(pid, envir = inv_idx, inherits = FALSE))
      get(pid, envir = inv_idx) else integer(0)
    citers <- citers[citers != i]

    if (length(citers) == 0L) return(NA_real_)

    # Focal paper's references that are also in the corpus
    focal_refs   <- cr_parsed[[i]]
    focal_in_corpus <- focal_refs[vapply(
      focal_refs,
      function(r) exists(r, envir = paper_id_to_row, inherits = FALSE),
      logical(1)
    )]

    # n_i: citers that do NOT cite any of focal's corpus refs
    # n_j: citers that DO cite at least one of focal's corpus refs
    citer_ref_sets <- lapply(citers, function(c_idx) cr_parsed[[c_idx]])

    if (length(focal_in_corpus) == 0L) {
      # No corpus refs -> all citers are "disruptive" by definition
      n_i <- length(citers)
      n_j <- 0L
      n_k <- 0L
    } else {
      n_j <- sum(vapply(citer_ref_sets, function(cref_set) {
        any(focal_in_corpus %in% cref_set)
      }, logical(1)))
      n_i <- length(citers) - n_j

      # n_k: papers that cite focal's refs but NOT focal itself
      all_citers_of_refs <- unique(unlist(lapply(
        focal_in_corpus,
        function(r) {
          if (exists(r, envir = inv_idx, inherits = FALSE))
            get(r, envir = inv_idx)
          else integer(0)
        }
      )))
      n_k <- sum(
        !all_citers_of_refs %in% c(i, citers)
      )
    }

    denom <- n_i + n_j + n_k
    if (denom == 0L) NA_real_ else (n_i - n_j) / denom
  }, numeric(1))

  # ---- Step 5: summarise ----
  n_valid  <- sum(!is.na(di_vals))
  n_total  <- length(di_vals)

  if (n_valid == 0L) {
    .log_step("[DI] No papers had within-corpus citations. DI not estimable.",
              verbose)
    return(dplyr::tibble(
      median_di         = NA_real_,
      mean_di           = NA_real_,
      pct_disruptive    = NA_real_,
      pct_consolidating = NA_real_,
      n_papers_with_di  = 0L,
      n_papers_total    = n_total,
      di_values         = list(rep(NA_real_, n_total)),
      method_note       = paste(
        "No within-corpus citations found.",
        "DI is only estimable for papers cited by other papers in the corpus."
      )
    ))
  }

  pct_disruptive    <- round(mean(di_vals > 0,  na.rm = TRUE) * 100, 1L)
  pct_consolidating <- round(mean(di_vals < 0,  na.rm = TRUE) * 100, 1L)

  .log_step(sprintf(
    "[DI] Median DI=%.3f | Disruptive: %.1f%% | Consolidating: %.1f%% | n=%d/%d",
    median(di_vals, na.rm = TRUE),
    pct_disruptive, pct_consolidating,
    n_valid, n_total
  ), verbose)

  dplyr::tibble(
    median_di         = round(median(di_vals, na.rm = TRUE), 3L),
    mean_di           = round(mean(di_vals,   na.rm = TRUE), 3L),
    pct_disruptive    = pct_disruptive,
    pct_consolidating = pct_consolidating,
    n_papers_with_di  = n_valid,
    n_papers_total    = n_total,
    di_values         = list(di_vals),      # full distribution for plots
    method_note       = paste(
      "Local DI computed from within-corpus citation network (CR field).",
      "External citations excluded -- values are comparable across",
      "subspecialties but understate absolute disruption.",
      sprintf("Valid DI for %d / %d papers (%.0f%%).",
              n_valid, n_total, n_valid / n_total * 100)
    )
  )
}



# ============================================================
# ACOG DISTRICT GEOGRAPHY
# ============================================================
# Source: Authoritative table provided by project team, cross-checked against
# acog.org/community/districts-and-sections (March 2026).
# Florida assigned to District XII only.
# District X = Armed Forces District; no fixed US states — excluded from
# state-based geographic analysis.

#' ACOG District Lookup Table
#'
#' @description
#' Authoritative mapping of all 50 US states and the District of Columbia to
#' their ACOG district. Florida is assigned to District XII. District X
#' (Armed Forces) has no fixed state assignment and is excluded. Use
#' \code{acog_district_vector} for fast named-vector lookups by abbreviation,
#' or \code{state_to_acog_district()} for the vectorized function interface.
#'
#' @format A tibble with 51 rows and 3 columns:
#' \describe{
#'   \item{state_name}{Full state name (character).}
#'   \item{state_abbreviation}{Two-letter postal abbreviation (character).}
#'   \item{acog_district}{ACOG district label, e.g. \code{"District XI"} (character).}
#' }
#'
#' @source \url{https://www.acog.org/community/districts-and-sections}
#'
#' @export
acog_district_table <- tibble::tribble(
  ~state_name,            ~state_abbreviation,  ~acog_district,
  "Alabama",              "AL",                 "District VII",
  "Alaska",               "AK",                 "District VIII",
  "Arizona",              "AZ",                 "District VIII",
  "Arkansas",             "AR",                 "District VII",
  "California",           "CA",                 "District IX",
  "Colorado",             "CO",                 "District VIII",
  "Connecticut",          "CT",                 "District I",
  "Delaware",             "DE",                 "District III",
  "District of Columbia", "DC",                 "District IV",
  "Florida",              "FL",                 "District XII",
  "Georgia",              "GA",                 "District IV",
  "Hawaii",               "HI",                 "District VIII",
  "Idaho",                "ID",                 "District VIII",
  "Illinois",             "IL",                 "District VI",
  "Indiana",              "IN",                 "District V",
  "Iowa",                 "IA",                 "District VI",
  "Kansas",               "KS",                 "District VII",
  "Kentucky",             "KY",                 "District V",
  "Louisiana",            "LA",                 "District VII",
  "Maine",                "ME",                 "District I",
  "Maryland",             "MD",                 "District IV",
  "Massachusetts",        "MA",                 "District I",
  "Michigan",             "MI",                 "District V",
  "Minnesota",            "MN",                 "District VI",
  "Mississippi",          "MS",                 "District VII",
  "Missouri",             "MO",                 "District VII",
  "Montana",              "MT",                 "District VIII",
  "Nebraska",             "NE",                 "District VI",
  "Nevada",               "NV",                 "District VIII",
  "New Hampshire",        "NH",                 "District I",
  "New Jersey",           "NJ",                 "District III",
  "New Mexico",           "NM",                 "District VIII",
  "New York",             "NY",                 "District II",
  "North Carolina",       "NC",                 "District IV",
  "North Dakota",         "ND",                 "District VI",
  "Ohio",                 "OH",                 "District V",
  "Oklahoma",             "OK",                 "District VII",
  "Oregon",               "OR",                 "District VIII",
  "Pennsylvania",         "PA",                 "District III",
  "Rhode Island",         "RI",                 "District I",
  "South Carolina",       "SC",                 "District IV",
  "South Dakota",         "SD",                 "District VI",
  "Tennessee",            "TN",                 "District VII",
  "Texas",                "TX",                 "District XI",
  "Utah",                 "UT",                 "District VIII",
  "Vermont",              "VT",                 "District I",
  "Virginia",             "VA",                 "District IV",
  "Washington",           "WA",                 "District VIII",
  "West Virginia",        "WV",                 "District IV",
  "Wisconsin",            "WI",                 "District VI",
  "Wyoming",              "WY",                 "District VIII"
)

#' Named vector of ACOG district assignments keyed by state abbreviation
#'
#' @description
#' Convenience vector derived from \code{acog_district_table}.
#' Names are two-letter postal abbreviations; values are district labels.
#' Used internally by \code{state_to_acog_district()} and useful for
#' direct \code{[}-style lookups in dplyr pipelines.
#'
#' @format Named character vector of length 51.
#' @export
acog_district_vector <- stats::setNames(
  object = acog_district_table$acog_district,
  nm     = acog_district_table$state_abbreviation
)

#' @noRd
# Internal alias kept for backward compatibility with downstream helpers.
.acog_district_lookup <- function() acog_district_vector


#' Map US State Abbreviations or Names to ACOG Districts
#'
#' @description
#' Vectorized lookup converting two-letter US postal abbreviations
#' \emph{or} full state names to their ACOG district label.
#' Returns \code{NA_character_} for unrecognised inputs (territories,
#' Canadian provinces, typos). Case-insensitive for both abbreviations
#' and full names.
#'
#' @param state Character vector of two-letter postal abbreviations
#'   (e.g. \code{"TX"}) or full state names (e.g. \code{"Texas"}).
#'   Mixed inputs are handled: each element is first tried as an
#'   abbreviation, then as a full name.
#'
#' @return Character vector the same length as \code{state}, with values
#'   such as \code{"District I"} through \code{"District XII"}, or
#'   \code{NA_character_} for unrecognised inputs.
#'
#' @examples
#' state_to_acog_district(c("TX", "CA", "NY", "FL"))
#' # [1] "District XI"  "District IX"  "District II"  "District XII"
#'
#' state_to_acog_district(c("Texas", "california", "New York"))
#' # [1] "District XI" "District IX" "District II"
#'
#' state_to_acog_district(c("TX", "PR", "XX"))  # territories return NA
#' # [1] "District XI" NA NA
#'
#' @importFrom assertthat assert_that
#' @export
state_to_acog_district <- function(state) {
  assertthat::assert_that(
    is.character(state),
    msg = paste(
      "`state` must be a character vector of two-letter US postal",
      "abbreviations or full state names (e.g. 'TX', 'Texas')."
    )
  )

  abbrev_lookup <- acog_district_vector   # keyed by abbreviation
  name_lookup   <- stats::setNames(
    acog_district_table$acog_district,
    toupper(acog_district_table$state_name)
  )

  vapply(state, function(s) {
    s_up <- toupper(trimws(s))
    # Try abbreviation first (fast path for most pipeline data)
    if (!is.na(abbrev_lookup[s_up]))       return(unname(abbrev_lookup[s_up]))
    # Fall back to full name match
    if (!is.na(name_lookup[s_up]))         return(unname(name_lookup[s_up]))
    NA_character_
  }, character(1L), USE.NAMES = FALSE)
}

#' Get All US States in an ACOG District
#'
#' @description
#' Returns the two-letter postal abbreviations of all US states (and DC)
#' belonging to a given ACOG district.
#'
#' @param district_label Character scalar. One of \code{"District I"} through
#'   \code{"District XII"} (Roman numerals, case-sensitive).
#'
#' @return Character vector of state abbreviations in alphabetical order.
#'
#' @examples
#' acog_district_states("District XI")
#' # [1] "TX"
#'
#' acog_district_states("District VIII")
#' # [1] "AK" "AZ" "CO" "HI" "ID" "MT" "NM" "NV" "OR" "UT" "WA" "WY"
#'
#' acog_district_states("District IV")
#' # [1] "DC" "GA" "MD" "NC" "SC" "VA" "WV"
#'
#' @importFrom assertthat assert_that is.string
#' @export
acog_district_states <- function(district_label) {
  assertthat::assert_that(
    assertthat::is.string(district_label),
    msg = "`district_label` must be a single character string."
  )

  valid_districts <- sort(unique(acog_district_vector))

  assertthat::assert_that(
    district_label %in% valid_districts,
    msg = sprintf(
      "'%s' is not a recognised ACOG district label.\nValid values: %s",
      district_label,
      paste(valid_districts, collapse = ", ")
    )
  )

  sort(names(acog_district_vector[acog_district_vector == district_label]))
}

#' Add ACOG District Column to a Data Frame
#'
#' @description
#' Appends an \code{acog_district} column to a data frame containing US state
#' abbreviations or full names. Rows with unrecognised codes (territories,
#' provinces) receive \code{NA}. Existing \code{acog_district} columns are
#' overwritten with a warning.
#'
#' @param df A data frame with a column of US state abbreviations or names.
#' @param state_col Character scalar. Name of the column holding state codes.
#'   Defaults to \code{"state"}.
#' @param verbose Logical. If \code{TRUE}, logs counts. Defaults to \code{TRUE}.
#'
#' @return \code{df} with an additional \code{acog_district} column.
#'
#' @examples
#' df <- data.frame(state = c("TX", "CA", "FL", "NY", "CO"),
#'                  value = 1:5)
#' add_acog_district(df, state_col = "state", verbose = FALSE)
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @export
add_acog_district <- function(df, state_col = "state", verbose = TRUE) {
  assertthat::assert_that(is.data.frame(df))
  assertthat::assert_that(assertthat::is.string(state_col))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    state_col %in% names(df),
    msg = sprintf(
      "Column '%s' not found in `df`. Available columns: %s",
      state_col, paste(names(df), collapse = ", ")
    )
  )

  if ("acog_district" %in% names(df)) {
    warning("Column 'acog_district' already exists and will be overwritten.",
            call. = FALSE)
  }

  district_vec <- state_to_acog_district(df[[state_col]])
  n_mapped     <- sum(!is.na(district_vec))
  n_unmapped   <- sum( is.na(district_vec))

  if (isTRUE(verbose)) {
    message(sprintf(
      "[ACOG DISTRICT] Mapped %d / %d rows (%d unmapped).",
      n_mapped, nrow(df), n_unmapped
    ))
    if (n_unmapped > 0L) {
      unmapped_vals <- unique(df[[state_col]][is.na(district_vec)])
      message(sprintf("[ACOG DISTRICT] Unmapped: %s",
                      paste(sort(unmapped_vals), collapse = ", ")))
    }
    dist_counts <- sort(table(district_vec[!is.na(district_vec)]),
                        decreasing = TRUE)
    message(sprintf(
      "[ACOG DISTRICT] Distribution:\n%s",
      paste(sprintf("  %-15s %d rows", names(dist_counts), dist_counts),
            collapse = "\n")
    ))
  }

  df[["acog_district"]] <- district_vec
  df
}

#' Summarise a Metric by ACOG District
#'
#' @description
#' Aggregates a numeric column by ACOG district, returning mean, median,
#' total, and row count per district plus percentage of the national total.
#'
#' @param df A data frame with an \code{acog_district} column (from
#'   \code{add_acog_district()}) and a numeric column to summarise.
#' @param metric_col Character scalar. Name of the numeric column.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A tibble with columns: \code{acog_district}, \code{n_rows},
#'   \code{mean_metric}, \code{median_metric}, \code{total_metric},
#'   \code{pct_of_national_total}.
#'
#' @examples
#' set.seed(1)
#' df <- data.frame(
#'   state      = sample(c("TX","CA","NY","FL","OH"), 200, replace = TRUE),
#'   drive_time = round(abs(rnorm(200, 45, 20)), 1)
#' )
#' df <- add_acog_district(df, verbose = FALSE)
#' summarise_by_acog_district(df, metric_col = "drive_time", verbose = FALSE)
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr filter group_by summarise mutate arrange
#' @importFrom rlang .data
#' @export
summarise_by_acog_district <- function(df, metric_col, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(df))
  assertthat::assert_that(assertthat::is.string(metric_col))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    "acog_district" %in% names(df),
    msg = "Column 'acog_district' not found. Run add_acog_district() first."
  )
  assertthat::assert_that(
    metric_col %in% names(df),
    msg = sprintf("Column '%s' not found.", metric_col)
  )
  assertthat::assert_that(
    is.numeric(df[[metric_col]]),
    msg = sprintf("Column '%s' must be numeric.", metric_col)
  )

  .log_step(sprintf("[ACOG DISTRICT] Summarising '%s' by district ...",
                    metric_col), verbose)

  national_total <- sum(df[[metric_col]], na.rm = TRUE)

  result <- df |>
    dplyr::filter(!is.na(.data$acog_district)) |>
    dplyr::group_by(.data$acog_district) |>
    dplyr::summarise(
      n_rows        = dplyr::n(),
      mean_metric   = round(mean  (.data[[metric_col]], na.rm = TRUE), 2L),
      median_metric = round(median(.data[[metric_col]], na.rm = TRUE), 2L),
      total_metric  = round(sum   (.data[[metric_col]], na.rm = TRUE), 2L),
      .groups       = "drop"
    ) |>
    dplyr::mutate(
      pct_of_national_total = round(
        .data$total_metric / max(national_total, .Machine$double.eps) * 100,
        1L
      )
    ) |>
    dplyr::arrange(.data$acog_district)

  .log_step(sprintf("[ACOG DISTRICT] %d districts, %d rows total",
                    nrow(result), sum(result$n_rows)), verbose)
  result
}

#' Create a Choropleth Map of ACOG Districts
#'
#' @description
#' Renders a \code{ggplot2} choropleth of the contiguous US (plus Alaska and
#' Hawaii in their native positions by default) coloured by ACOG district.
#' State geometries are fetched from \pkg{tigris}; the result can be saved
#' to disk as a PNG.
#'
#' @param region_lookup A tibble with columns \code{state_name},
#'   \code{state_abbreviation}, and \code{acog_district}. Defaults to
#'   the built-in \code{acog_district_table}.
#' @param label_regions Logical. If \code{TRUE}, overlays each district's
#'   label at the centroid of its constituent states. Defaults to \code{TRUE}.
#' @param include_dc Logical. If \code{TRUE}, includes the District of
#'   Columbia. Defaults to \code{TRUE}.
#' @param use_alaska_hawaii_insets Logical. Reserved for future use; currently
#'   all geometries are shown in their native CRS positions. Defaults to
#'   \code{FALSE}.
#' @param save_plot Logical. If \code{TRUE}, saves the plot as a PNG file.
#'   Defaults to \code{FALSE}.
#' @param save_directory Character scalar. Directory for the saved file.
#'   Defaults to \code{"."}.
#' @param file_stem Character scalar. Filename prefix (timestamp appended).
#'   Defaults to \code{"acog_region_map"}.
#'
#' @return A \code{ggplot2} object. As a side effect, saves a PNG when
#'   \code{save_plot = TRUE} and logs the path.
#'
#' @examples
#' \dontrun{
#' # Basic map using the built-in table
#' p <- create_region_map()
#' print(p)
#'
#' # Save to disk
#' create_region_map(
#'   region_lookup = acog_district_table,
#'   label_regions = TRUE,
#'   save_plot     = TRUE,
#'   save_directory = "figures/"
#' )
#' }
#'
#' @importFrom assertthat assert_that is.flag is.string
#' @importFrom dplyr mutate filter left_join select group_by summarise bind_cols
#' @importFrom ggplot2 ggplot aes geom_sf geom_text labs theme_minimal theme
#'   element_blank ggsave
#' @importFrom sf st_point_on_surface st_coordinates st_drop_geometry
#' @importFrom tibble tibble
#' @export
create_region_map <- function(
    region_lookup            = acog_district_table,
    label_regions            = TRUE,
    include_dc               = TRUE,
    use_alaska_hawaii_insets = FALSE,
    save_plot                = FALSE,
    save_directory           = ".",
    file_stem                = "acog_region_map"
) {
  assertthat::assert_that(is.data.frame(region_lookup))
  assertthat::assert_that(assertthat::is.flag(label_regions))
  assertthat::assert_that(assertthat::is.flag(include_dc))
  assertthat::assert_that(assertthat::is.flag(use_alaska_hawaii_insets))
  assertthat::assert_that(assertthat::is.flag(save_plot))
  assertthat::assert_that(assertthat::is.string(save_directory))
  assertthat::assert_that(assertthat::is.string(file_stem))

  required_columns <- c("state_name", "state_abbreviation", "acog_district")
  missing_columns  <- base::setdiff(required_columns, base::names(region_lookup))
  assertthat::assert_that(
    length(missing_columns) == 0L,
    msg = sprintf("region_lookup missing columns: %s",
                  paste(missing_columns, collapse = ", "))
  )

  base::message("create_region_map(): fetching state geometries from tigris.")
  base::message(sprintf(
    "  label_regions=%s  include_dc=%s  save_plot=%s",
    label_regions, include_dc, save_plot
  ))

  options(tigris_use_cache = TRUE)
  state_shapes <- tigris::states(
    cb         = TRUE,
    resolution = "20m",
    year       = 2024,
    class      = "sf"
  )
  base::message(sprintf("  Fetched %d state geometries.", nrow(state_shapes)))

  state_shapes <- dplyr::mutate(
    state_shapes,
    state_name         = .data$NAME,
    state_abbreviation = .data$STUSPS
  )

  if (!include_dc) {
    base::message("  Removing District of Columbia.")
    state_shapes <- dplyr::filter(state_shapes, .data$state_abbreviation != "DC")
  }

  if (!use_alaska_hawaii_insets) {
    base::message("  No AK/HI inset transformation applied.")
  }

  base::message("  Joining ACOG district lookup onto state geometries.")
  map_shapes <- dplyr::left_join(
    state_shapes,
    region_lookup,
    by = c("state_name", "state_abbreviation")
  )

  unmatched <- dplyr::filter(map_shapes, base::is.na(.data$acog_district))
  if (nrow(unmatched) > 0L) {
    base::message(sprintf(
      "  States without district assignment: %s",
      paste(unmatched$state_abbreviation, collapse = ", ")
    ))
  } else {
    base::message("  All mapped states matched to an ACOG district.")
  }

  # ---- district label coordinates (centroid of member states) ----
  label_points      <- sf::st_point_on_surface(map_shapes)
  label_coordinates <- sf::st_coordinates(label_points)
  label_table <- dplyr::bind_cols(
    sf::st_drop_geometry(
      dplyr::select(map_shapes, "state_name", "state_abbreviation",
                    "acog_district")
    ),
    tibble::tibble(x = label_coordinates[, "X"], y = label_coordinates[, "Y"])
  )
  district_labels <- label_table |>
    dplyr::filter(!base::is.na(.data$acog_district)) |>
    dplyr::group_by(.data$acog_district) |>
    dplyr::summarise(
      x = base::mean(.data$x, na.rm = TRUE),
      y = base::mean(.data$y, na.rm = TRUE),
      .groups = "drop"
    )

  base::message("  Building ggplot object.")
  region_plot <- ggplot2::ggplot(map_shapes) +
    ggplot2::geom_sf(
      ggplot2::aes(fill = .data$acog_district),
      color     = "white",
      linewidth = 0.2
    ) +
    ggplot2::labs(
      title    = "ACOG Districts",
      subtitle = "Current US state assignments",
      fill     = "ACOG District",
      caption  = "District X is the Armed Forces District and is not state-based."
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(
      panel.grid.major = ggplot2::element_blank(),
      panel.grid.minor = ggplot2::element_blank(),
      axis.title       = ggplot2::element_blank(),
      axis.text        = ggplot2::element_blank(),
      legend.position  = "right"
    )

  if (label_regions) {
    base::message("  Adding district labels.")
    region_plot <- region_plot +
      ggplot2::geom_text(
        data    = district_labels,
        mapping = ggplot2::aes(x = .data$x, y = .data$y,
                               label = .data$acog_district),
        size = 3
      )
  }

  if (save_plot) {
    timestamp_value <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    file_path <- base::file.path(
      save_directory,
      base::paste0(file_stem, "_", timestamp_value, ".png")
    )
    base::message(sprintf("  Saving map to: %s", file_path))
    ggplot2::ggsave(
      filename = file_path, plot = region_plot,
      width = 11, height = 7, dpi = 300
    )
    base::message(sprintf("  Saved: %s", normalizePath(file_path)))
  }

  base::message("create_region_map(): done.")
  return(region_plot)
}




#' @noRd
#' Remove self-citations from TC (times-cited) counts.
#'
#' A self-citation is any citation where at least one author of the
#' citing paper is also an author of the cited paper. At corpus level
#' we approximate this by: for each paper in the bibliography, remove
#' any CR entry whose first-author surname matches any AU surname in
#' that paper. This is conservative (surname only, no initials) but
#' avoids false positives from initials-only records.
#'
#' @param bibliography Data frame with AU (semicolon-delimited authors)
#'   and CR (semicolon-delimited cited references in bibliometrix format).
#' @param verbose Logical.
#' @return The bibliography with two new columns:
#'   \code{n_self_cites} (integer) and
#'   \code{TC_external} (character, TC minus self-cites; NA if TC absent).
.filter_self_citations <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[SELF-CITE] Computing self-citation counts ...", verbose)

  if (!"AU" %in% names(bibliography) || !"CR" %in% names(bibliography)) {
    .log_step("[SELF-CITE] AU or CR absent -- skipping.", verbose)
    bibliography[["n_self_cites"]] <- NA_integer_
    bibliography[["TC_external"]]  <- if ("TC" %in% names(bibliography))
      suppressWarnings(as.integer(bibliography$TC)) else NA_integer_
    return(bibliography)
  }

  # Corpus-level inverted index: surname -> row indices
  # Extract author identifiers from bibliometrix AU field.
  # AU format: "SURNAME INITIALS;SURNAME INITIALS" (e.g. "BARBER MD;NORTON PA").
  # We keep the full "SURNAME INITIALS" token for matching — stripping to
  # surname-only would cause false-positive self-citations when two different
  # authors share a surname (e.g. "BARBER MD" vs "BARBER JD").
  .extract_au_tokens <- function(au_str) {
    if (is.na(au_str) || nchar(trimws(au_str)) == 0L) return(character(0))
    tokens <- trimws(strsplit(au_str, ";", fixed = TRUE)[[1L]])
    toupper(tokens[nchar(tokens) > 0L])
  }

  # Build inverted index: "SURNAME INITIALS" -> which rows contain this author
  corpus_au_lists <- lapply(bibliography$AU, .extract_au_tokens)
  surname_to_rows <- new.env(hash = TRUE, size = 2000L)
  for (row_i in seq_along(corpus_au_lists)) {
    for (sn in corpus_au_lists[[row_i]]) {
      if (nchar(sn) < 2L) next
      existing <- if (exists(sn, envir = surname_to_rows, inherits = FALSE))
        get(sn, envir = surname_to_rows) else integer(0L)
      assign(sn, c(existing, row_i), envir = surname_to_rows)
    }
  }

  # For each paper: count CR entries whose first-author surname is in AU
  self_cite_counts <- vapply(seq_len(nrow(bibliography)), function(row_i) {
    cr_str   <- bibliography$CR[[row_i]]
    au_sn    <- corpus_au_lists[[row_i]]
    if (length(au_sn) == 0L || is.na(cr_str) || nchar(trimws(cr_str)) == 0L)
      return(0L)

    cr_refs <- trimws(strsplit(cr_str, ";", fixed = TRUE)[[1L]])
    cr_refs <- cr_refs[nchar(cr_refs) > 0L]

    # CR format: "SURNAME INITIALS, YEAR, JOURNAL, ..."
    # Keep full "SURNAME INITIALS" (e.g. "BARBER MD") to match AU tokens
    # precisely — surname-only would cause false positives for common surnames.
    cr_tokens <- toupper(trimws(sub(",.*$", "", cr_refs)))
    sum(cr_tokens %in% au_sn)
  }, integer(1L))

  n_papers_with_self <- sum(self_cite_counts > 0L)
  mean_self          <- round(mean(self_cite_counts), 2L)
  .log_step(sprintf(
    "[SELF-CITE] %d/%d papers have >= 1 self-citation (mean %.2f per paper)",
    n_papers_with_self, nrow(bibliography), mean_self
  ), verbose)

  # Compute TC_external (TC as numeric minus self-cite count, floored at 0)
  bibliography[["n_self_cites"]] <- self_cite_counts
  if ("TC" %in% names(bibliography)) {
    tc_numeric <- suppressWarnings(as.numeric(bibliography$TC))
    tc_external <- pmax(tc_numeric - self_cite_counts, 0, na.rm = FALSE)
    bibliography[["TC_external"]] <- as.integer(round(tc_external))
  } else {
    bibliography[["TC_external"]] <- NA_integer_
  }

  bibliography
}

#' Remove Self-Citations from a Bibliography
#'
#' @description
#' Appends \code{n_self_cites} and \code{TC_external} columns to the
#' bibliography. \code{TC_external} is the Web of Science times-cited
#' count minus within-corpus self-citations, giving a more conservative
#' impact estimate. Pass the output directly to
#' \code{run_fpmrs_bibliometric_pipeline()} or use \code{TC_external}
#' as the citation column in downstream analyses.
#'
#' @param bibliography A data frame from
#'   \code{run_fpmrs_bibliometric_pipeline()$bibliography} or
#'   \code{bibliometrix::convert2df()}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return The input data frame with two added columns:
#'   \code{n_self_cites} (integer count of self-citations per paper) and
#'   \code{TC_external} (character TC minus self-cites, floored at 0).
#'
#' @examples
#' \dontrun{
#' result <- run_fpmrs_bibliometric_pipeline(...)
#' bib_filtered <- remove_self_citations(result$bibliography, verbose = TRUE)
#' # Use TC_external instead of TC for citation analyses
#' }
#'
#' @importFrom assertthat assert_that is.flag
#' @export
remove_self_citations <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.flag(verbose))
  .filter_self_citations(bibliography, verbose = verbose)
}

# ============================================================
# SCIENCE MAPPING: VOSviewer export + network construction
# ============================================================

#' @noRd
.wb_income_tier <- function(country_upper) {
  # World Bank FY2024 income classification.
  # Returns "High", "Upper-Middle", "Lower-Middle", or "Low".
  # Unmapped countries default to NA.
  high <- c(
    "USA","UNITED STATES","AUSTRALIA","AUSTRIA","BELGIUM","CANADA",
    "DENMARK","FINLAND","FRANCE","GERMANY","GREECE","IRELAND","ISRAEL",
    "ITALY","JAPAN","SOUTH KOREA","LUXEMBOURG","NETHERLANDS","NEW ZEALAND",
    "NORWAY","PORTUGAL","SINGAPORE","SPAIN","SWEDEN","SWITZERLAND",
    "UNITED KINGDOM","ENGLAND","SCOTLAND","WALES","ICELAND","CYPRUS",
    "CZECH REPUBLIC","ESTONIA","HUNGARY","LATVIA","LITHUANIA","MALTA",
    "POLAND","SLOVAKIA","SLOVENIA","CROATIA","ROMANIA","BULGARIA",
    "SAUDI ARABIA","UAE","UNITED ARAB EMIRATES","QATAR","BAHRAIN","KUWAIT",
    "OMAN","CHILE","URUGUAY","PANAMA","TRINIDAD AND TOBAGO",
    "ANTIGUA AND BARBUDA","BARBADOS","SAINT KITTS AND NEVIS","HONG KONG",
    "MACAO","TAIWAN","ANDORRA","LIECHTENSTEIN","MONACO","SAN MARINO"
  )
  upper_mid <- c(
    "CHINA","BRAZIL","MEXICO","RUSSIA","TURKEY","ARGENTINA","COLOMBIA",
    "PERU","SOUTH AFRICA","THAILAND","MALAYSIA","INDONESIA","IRAN","IRAQ",
    "JORDAN","LEBANON","ARMENIA","AZERBAIJAN","BELARUS","BOSNIA",
    "GEORGIA","KAZAKHSTAN","NORTH MACEDONIA","SERBIA","ALBANIA",
    "CUBA","DOMINICAN REPUBLIC","ECUADOR","EL SALVADOR","GUATEMALA",
    "JAMAICA","PARAGUAY","VENEZUELA","FIJI","PALAU","NAURU",
    "EQUATORIAL GUINEA","GABON","BOTSWANA","NAMIBIA","TUNISIA",
    "ALGERIA","EGYPT","MOROCCO","LIBYA","AMERICAN SAMOA","TUVALU"
  )
  lower_mid <- c(
    "INDIA","PAKISTAN","BANGLADESH","NIGERIA","KENYA","UKRAINE","PHILIPPINES",
    "VIETNAM","GHANA","CAMBODIA","MYANMAR","SENEGAL","ZAMBIA",
    "HONDURAS","NICARAGUA","BOLIVIA","SRI LANKA","NEPAL","MOLDOVA",
    "KYRGYZSTAN","TAJIKISTAN","UZBEKISTAN","MONGOLIA","PAPUA NEW GUINEA",
    "CAMEROON","IVORY COAST","ANGOLA","TANZANIA","UGANDA",
    "MAURITANIA","MOROCCO","CABO VERDE","DJIBOUTI",
    "MICRONESIA","KIRIBATI","SOLOMON ISLANDS","VANUATU","TIMOR-LESTE",
    "COMOROS","SAO TOME AND PRINCIPE","LESOTHO","ESWATINI",
    "BENIN"
  )
  low <- c(
    "AFGHANISTAN","CHAD","CENTRAL AFRICAN REPUBLIC","DEMOCRATIC REPUBLIC OF CONGO",
    "ERITREA","ETHIOPIA","GUINEA","HAITI","LIBERIA","MADAGASCAR","MALAWI",
    "MALI","MOZAMBIQUE","NIGER","NORTH KOREA","SIERRA LEONE","SOMALIA",
    "SOUTH SUDAN","SYRIA","TOGO","BURKINA FASO","BURUNDI","GAMBIA",
    "GUINEA-BISSAU","RWANDA","SUDAN","YEMEN","ZIMBABWE"
  )
  dplyr::case_when(
    country_upper %in% high        ~ "High",
    country_upper %in% upper_mid   ~ "Upper-Middle",
    country_upper %in% lower_mid   ~ "Lower-Middle",
    country_upper %in% low         ~ "Low",
    TRUE                           ~ NA_character_
  )
}

#' @noRd
.compute_coword_network <- function(
    bibliography,
    keyword_col    = "DE",
    min_frequency  = 2L,
    verbose        = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.string(keyword_col))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step(sprintf("[COWORD] Building keyword co-occurrence matrix (%s) ...",
                    keyword_col), verbose)

  if (!keyword_col %in% names(bibliography)) {
    .log_step(sprintf("[COWORD] Column '%s' not found -- returning NULL.",
                      keyword_col), verbose)
    return(NULL)
  }

  # Parse keyword lists per paper
  kw_lists <- lapply(bibliography[[keyword_col]], function(kw_str) {
    if (is.na(kw_str) || nchar(trimws(kw_str)) == 0L) return(character(0))
    unique(toupper(trimws(strsplit(kw_str, ";")[[1L]])))
  })

  # Frequency filter: keep keywords appearing in >= min_frequency papers
  all_kws    <- unlist(kw_lists)
  kw_freq    <- table(all_kws)
  kept_kws   <- names(kw_freq[kw_freq >= min_frequency])

  if (length(kept_kws) == 0L) {
    .log_step("[COWORD] No keywords pass frequency threshold.", verbose)
    return(NULL)
  }
  kw_idx     <- setNames(seq_along(kept_kws), kept_kws)

  # Build co-occurrence matrix (upper triangle)
  n_kw   <- length(kept_kws)
  co_mat <- Matrix::sparseMatrix(
    i = integer(0), j = integer(0), x = numeric(0),
    dims = c(n_kw, n_kw),
    dimnames = list(kept_kws, kept_kws)
  )

  for (kw_list in kw_lists) {
    valid <- kw_list[kw_list %in% kept_kws]
    if (length(valid) < 2L) next
    pairs <- utils::combn(valid, 2L)
    for (col_idx in seq_len(ncol(pairs))) {
      i_val <- kw_idx[pairs[1L, col_idx]]
      j_val <- kw_idx[pairs[2L, col_idx]]
      if (i_val > j_val) { tmp <- i_val; i_val <- j_val; j_val <- tmp }
      co_mat[i_val, j_val] <- co_mat[i_val, j_val] + 1L
    }
  }
  # Symmetrize
  co_mat <- co_mat + Matrix::t(co_mat)

  n_edges <- sum(co_mat > 0) / 2L
  .log_step(sprintf(
    "[COWORD] %d nodes, %d edges (min_freq=%d)",
    n_kw, as.integer(n_edges), min_frequency
  ), verbose)

  co_mat
}

#' @noRd
.compute_coauthorship_network <- function(
    bibliography,
    unit           = "countries",
    min_occurrences = 2L,
    verbose        = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(unit %in% c("countries", "authors"))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step(sprintf("[COAUTH] Building %s co-authorship network ...",
                    unit), verbose)

  col_name <- if (unit == "countries") "AU_CO" else "AF"
  if (!col_name %in% names(bibliography)) {
    .log_step(sprintf("[COAUTH] Column '%s' not found.", col_name), verbose)
    return(NULL)
  }

  entity_lists <- lapply(bibliography[[col_name]], function(s) {
    if (is.na(s) || nchar(trimws(s)) == 0L) return(character(0))
    tokens <- toupper(trimws(strsplit(s, ";")[[1L]]))
    if (unit == "countries") {
      # Normalize and deduplicate
      unique(purrr::map_chr(tokens[nchar(tokens) > 0L],
                             .normalize_country_string))
    } else {
      unique(tokens[nchar(tokens) > 0L])
    }
  })

  # Frequency filter
  all_ents <- unlist(entity_lists)
  ent_freq <- table(all_ents)
  kept     <- names(ent_freq[ent_freq >= min_occurrences])
  if (length(kept) == 0L) {
    .log_step("[COAUTH] No entities pass frequency threshold.", verbose)
    return(NULL)
  }
  ent_idx <- setNames(seq_along(kept), kept)
  n_ent   <- length(kept)

  co_mat <- Matrix::sparseMatrix(
    i = integer(0), j = integer(0), x = numeric(0),
    dims = c(n_ent, n_ent),
    dimnames = list(kept, kept)
  )

  for (ent_list in entity_lists) {
    valid <- ent_list[ent_list %in% kept]
    if (length(valid) < 2L) next
    pairs <- utils::combn(valid, 2L)
    for (col_idx in seq_len(ncol(pairs))) {
      i_val <- ent_idx[pairs[1L, col_idx]]
      j_val <- ent_idx[pairs[2L, col_idx]]
      if (i_val > j_val) { tmp <- i_val; i_val <- j_val; j_val <- tmp }
      co_mat[i_val, j_val] <- co_mat[i_val, j_val] + 1L
    }
  }
  co_mat <- co_mat + Matrix::t(co_mat)

  n_edges <- sum(co_mat > 0) / 2L
  .log_step(sprintf("[COAUTH] %d %s, %d co-authorship links",
                    n_ent, unit, as.integer(n_edges)), verbose)
  co_mat
}

#' @noRd
.compute_cocitation_network <- function(
    bibliography,
    top_n_refs    = 100L,
    verbose       = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.count(top_n_refs))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[COCITE] Building co-citation network (top cited refs) ...",
            verbose)

  if (!"CR" %in% names(bibliography)) {
    .log_step("[COCITE] No CR column -- returning NULL.", verbose)
    return(NULL)
  }

  # Parse CR: extract "AUTHOR, YEAR" identifiers per paper
  .parse_ref_id <- function(cr_str) {
    if (is.na(cr_str) || nchar(trimws(cr_str)) == 0L) return(character(0))
    refs  <- trimws(strsplit(cr_str, ";")[[1L]])
    refs  <- refs[nchar(refs) > 0L]
    purrr::map_chr(refs, function(r) {
      parts <- strsplit(r, ",")[[1L]]
      if (length(parts) >= 2L)
        paste0(toupper(trimws(parts[1L])), ",", trimws(parts[2L]))
      else toupper(trimws(r))
    })
  }

  ref_lists <- lapply(bibliography$CR, .parse_ref_id)

  # Frequency: keep top_n_refs most cited (most appearing in CR fields)
  ref_freq  <- sort(table(unlist(ref_lists)), decreasing = TRUE)
  kept_refs <- names(ref_freq)[seq_len(min(top_n_refs, length(ref_freq)))]
  if (length(kept_refs) < 2L) {
    .log_step("[COCITE] Fewer than 2 references qualify.", verbose)
    return(NULL)
  }
  ref_idx  <- setNames(seq_along(kept_refs), kept_refs)
  n_ref    <- length(kept_refs)

  co_mat <- Matrix::sparseMatrix(
    i = integer(0), j = integer(0), x = numeric(0),
    dims = c(n_ref, n_ref),
    dimnames = list(kept_refs, kept_refs)
  )

  for (ref_list in ref_lists) {
    valid <- ref_list[ref_list %in% kept_refs]
    if (length(valid) < 2L) next
    pairs <- utils::combn(unique(valid), 2L)
    for (col_idx in seq_len(ncol(pairs))) {
      i_val <- ref_idx[pairs[1L, col_idx]]
      j_val <- ref_idx[pairs[2L, col_idx]]
      if (i_val > j_val) { tmp <- i_val; i_val <- j_val; j_val <- tmp }
      co_mat[i_val, j_val] <- co_mat[i_val, j_val] + 1L
    }
  }
  co_mat <- co_mat + Matrix::t(co_mat)

  n_edges <- sum(co_mat > 0) / 2L
  .log_step(sprintf(
    "[COCITE] Top %d refs, %d co-citation pairs",
    n_ref, as.integer(n_edges)
  ), verbose)
  co_mat
}

#' @noRd
.export_vosviewer_network <- function(
    network_matrix,
    output_dir,
    file_prefix     = "network",
    weight_label   = "Publications",
    min_edge_weight = 1L,
    verbose        = TRUE
) {
  assertthat::assert_that(inherits(network_matrix, "Matrix") ||
                            is.matrix(network_matrix))
  assertthat::assert_that(assertthat::is.string(output_dir))
  assertthat::assert_that(assertthat::is.string(file_prefix))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  node_labels <- rownames(network_matrix)
  assertthat::assert_that(
    !is.null(node_labels) && length(node_labels) > 0L,
    msg = "network_matrix must have rownames (node labels)."
  )

  # ---- Pair file (edge list: id1, id2, weight) ----
  trip     <- Matrix::summary(as(network_matrix, "sparseMatrix"))
  # Upper triangle only to avoid duplicate edges
  trip_upper <- trip[trip$i < trip$j & trip$x >= min_edge_weight, ]

  # Create output directory if it doesn't exist.
  # writeLines gives an unhelpful "cannot open file" error on missing dirs.
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    .log_step(sprintf("[VOSviewer] Created output directory: %s", output_dir),
              verbose)
  }

  pair_path <- file.path(output_dir,
                         paste0(file_prefix, "_vos_pair_file.txt"))
  pair_lines <- paste(trip_upper$i, trip_upper$j, trip_upper$x, sep = "\t")
  writeLines(pair_lines, pair_path)

  # ---- Map file (node attributes: id, label, weight) ----
  node_weights <- Matrix::rowSums(network_matrix)
  map_header   <- paste("id", "label",
                        paste0("weight<", weight_label, ">"),
                        sep = "\t")
  map_lines    <- c(
    map_header,
    paste(seq_along(node_labels), node_labels,
          round(node_weights), sep = "\t")
  )
  map_path <- file.path(output_dir,
                        paste0(file_prefix, "_vos_map_file.txt"))
  writeLines(map_lines, map_path)

  .log_step(sprintf(
    "[VOSviewer] Pair file: %s (%d edges)",
    pair_path, nrow(trip_upper)
  ), verbose)
  .log_step(sprintf(
    "[VOSviewer] Map file:  %s (%d nodes)",
    map_path, length(node_labels)
  ), verbose)
  .log_step(paste(
    "[VOSviewer] USAGE: open VOSviewer ->",
    "Create Map -> Based on Network Data ->",
    "Pair file + Map file (select both files above)"
  ), verbose)

  list(pair_file = pair_path, map_file = map_path,
       n_nodes = length(node_labels), n_edges = nrow(trip_upper))
}


#' Export Network to CiteSpace Format
#'
#' @description
#' Writes the co-citation or co-word network to a format compatible with
#' CiteSpace (Chen 2014). CiteSpace uses an edge-list file with columns
#' \code{Source}, \code{Target}, \code{Weight} plus a node attribute file
#' with \code{Node}, \code{Label}, \code{Frequency}.
#'
#' CiteSpace provides burst detection, emergent theme identification, and
#' Kleinberg burst algorithm analysis that complements the VOSviewer
#' cluster visualisation. Together they enable both spatial (VOSviewer)
#' and temporal (CiteSpace) perspectives on the network.
#'
#' @param network_matrix A symmetric sparse matrix (from
#'   \code{.compute_coword_network()}, \code{.compute_coauthorship_network()},
#'   or \code{.compute_cocitation_network()}) with \code{rownames} as
#'   node labels.
#' @param output_dir Character. Directory where files are written. Created
#'   if it does not exist.
#' @param file_prefix Character. Filename prefix; two files are produced:
#'   \code{<prefix>_citespace_edges.csv} and
#'   \code{<prefix>_citespace_nodes.csv}.
#' @param min_edge_weight Integer. Minimum co-occurrence count to include
#'   an edge. Defaults to \code{1L}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A list with elements \code{edge_file}, \code{node_file},
#'   \code{n_nodes}, \code{n_edges}.
#'
#' @section CiteSpace usage:
#' \enumerate{
#'   \item Open CiteSpace -> File -> Import Network
#'   \item Select "User-defined" format
#'   \item Load edge file, then node file
#'   \item Run -> Burst Detection (Kleinberg algorithm)
#'   \item Clusters tab shows intellectual communities
#' }
#'
#' @examples
#' \dontrun{
#' mat <- .compute_coword_network(bibliography, min_frequency = 3L)
#' export_citespace_network(mat, "output/", "fpmrs_coword")
#' }
#'
#' @importFrom assertthat assert_that is.string is.count is.flag
#' @importFrom utils write.csv
#' @export
export_citespace_network <- function(
    network_matrix,
    output_dir,
    file_prefix     = "network",
    min_edge_weight = 1L,
    verbose         = TRUE
) {
  assertthat::assert_that(inherits(network_matrix, "Matrix") ||
                            is.matrix(network_matrix))
  assertthat::assert_that(assertthat::is.string(output_dir))
  assertthat::assert_that(assertthat::is.string(file_prefix))
  assertthat::assert_that(assertthat::is.count(min_edge_weight))
  assertthat::assert_that(assertthat::is.flag(verbose))

  node_labels <- rownames(network_matrix)
  assertthat::assert_that(
    !is.null(node_labels) && length(node_labels) > 0L,
    msg = "network_matrix must have rownames (node labels)."
  )

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    .log_step(sprintf("[CiteSpace] Created output directory: %s", output_dir),
              verbose)
  }

  # ---- Edge file ----
  trip       <- Matrix::summary(as(network_matrix, "sparseMatrix"))
  trip_upper <- trip[trip$i < trip$j & trip$x >= min_edge_weight, ]

  edge_df <- dplyr::tibble(
    Source = node_labels[trip_upper$i],
    Target = node_labels[trip_upper$j],
    Weight = trip_upper$x
  ) |>
    dplyr::arrange(dplyr::desc(.data$Weight))

  edge_path <- file.path(output_dir,
                         paste0(file_prefix, "_citespace_edges.csv"))
  utils::write.csv(edge_df, edge_path, row.names = FALSE, quote = TRUE)

  # ---- Node file ----
  node_freq   <- Matrix::rowSums(network_matrix)
  node_df     <- dplyr::tibble(
    Node      = seq_along(node_labels),
    Label     = node_labels,
    Frequency = as.integer(node_freq)
  ) |>
    dplyr::arrange(dplyr::desc(.data$Frequency))

  node_path <- file.path(output_dir,
                         paste0(file_prefix, "_citespace_nodes.csv"))
  utils::write.csv(node_df, node_path, row.names = FALSE, quote = TRUE)

  .log_step(sprintf(
    "[CiteSpace] Edge file: %s (%d edges)",
    edge_path, nrow(edge_df)
  ), verbose)
  .log_step(sprintf(
    "[CiteSpace] Node file: %s (%d nodes)",
    node_path, nrow(node_df)
  ), verbose)
  .log_step(paste(
    "[CiteSpace] USAGE: CiteSpace -> File -> Import Network ->",
    "User-defined format -> load edge file + node file.",
    "Run Burst Detection for emergent theme identification."
  ), verbose)

  list(edge_file = edge_path, node_file = node_path,
       n_nodes   = nrow(node_df), n_edges = nrow(edge_df))
}

#' @noRd
.compute_thematic_evolution <- function(
    bibliography,
    keyword_col = "DE",
    eras        = NULL,
    top_n       = 20L,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.string(keyword_col))
  if (is.null(eras)) {
    yr_range <- range(bibliography$publication_year, na.rm = TRUE)
    eras     <- make_eras(yr_range[1L], yr_range[2L])
  }
  assertthat::assert_that(is.list(eras) && length(eras) >= 2L)
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[THEMATIC] Computing thematic evolution across eras ...", verbose)

  if (!keyword_col %in% names(bibliography)) {
    .log_step(sprintf("[THEMATIC] Column '%s' not found.", keyword_col), verbose)
    return(dplyr::tibble(era=character(), keyword=character(),
                         n_papers=integer(), pct_of_era=numeric()))
  }
  if (!"publication_year" %in% names(bibliography)) {
    .log_step("[THEMATIC] Column 'publication_year' not found.", verbose)
    return(dplyr::tibble(era=character(), keyword=character(),
                         n_papers=integer(), pct_of_era=numeric()))
  }

  purrr::map_dfr(names(eras), function(era_name) {
    yr_range   <- eras[[era_name]]
    era_papers <- bibliography |>
      dplyr::filter(
        .data$publication_year >= yr_range[1L],
        .data$publication_year <= yr_range[2L]
      )

    n_era <- nrow(era_papers)
    if (n_era == 0L) return(dplyr::tibble())

    # Count how many papers in this era contain each keyword.
    # Use paper-level counting (not occurrence counting) so that
    # pct_of_era = "% of era papers that mention this keyword" and
    # the per-era percentages are interpretable (each keyword <= 100%;
    # the sum across all keywords will exceed 100%, which is correct
    # because papers can contain multiple keywords).
    # Previously this used occurrence counts / n_papers which produced
    # the same percentages but the n_papers column was mislabelled.
    kw_paper_counts <- era_papers[[keyword_col]] |>
      purrr::imap(function(kw_str, paper_idx) {
        if (is.na(kw_str) || nchar(trimws(kw_str)) == 0L) return(character(0))
        unique(toupper(trimws(strsplit(kw_str, ";")[[1L]])))
      }) |>
      purrr::map(function(kws) kws[nchar(kws) > 0L])

    # Build keyword -> paper_count mapping
    all_kw_tokens <- unlist(kw_paper_counts)
    if (length(all_kw_tokens) == 0L) return(dplyr::tibble())

    kw_freq_raw <- sort(table(all_kw_tokens), decreasing = TRUE)
    top_kws     <- utils::head(kw_freq_raw, top_n)

    dplyr::tibble(
      era          = era_name,
      keyword      = names(top_kws),
      n_papers     = as.integer(top_kws),   # papers containing this keyword
      pct_of_era   = round(as.integer(top_kws) / n_era * 100, 1L),
      n_era_papers = n_era
    )
  })
}

#' @noRd
.compute_mann_kendall_trend <- function(
    annual_trends_tbl,
    count_col  = "publication_count",
    year_col   = "publication_year",
    min_seg    = 5L,
    alpha      = 0.05,
    verbose    = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends_tbl))
  assertthat::assert_that(assertthat::is.string(count_col))
  assertthat::assert_that(assertthat::is.string(year_col))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  assertthat::assert_that(
    assertthat::is.number(alpha) && alpha > 0 && alpha < 1,
    msg = sprintf("`alpha` must be in (0, 1). Received: %g", alpha)
  )
  assertthat::assert_that(
    count_col %in% names(annual_trends_tbl),
    msg = sprintf(
      paste("Column '%s' not found in `annual_trends_tbl`.",
            "Available: %s.",
            "Pass .compute_annual_publication_trends() output directly",
            "or set count_col to your count column name."),
      count_col, paste(names(annual_trends_tbl), collapse = ", ")
    )
  )
  assertthat::assert_that(
    year_col %in% names(annual_trends_tbl),
    msg = sprintf(
      "Column '%s' not found in `annual_trends_tbl`. Available: %s.",
      year_col, paste(names(annual_trends_tbl), collapse = ", ")
    )
  )
  .log_step("[MK TREND] Computing Mann-Kendall trend + breakpoint ...", verbose)

  tbl <- annual_trends_tbl |>
    dplyr::filter(!is.na(.data[[count_col]]),
                  !is.na(.data[[year_col]])) |>
    dplyr::arrange(.data[[year_col]])

  y <- tbl[[count_col]]
  x <- tbl[[year_col]]
  n <- length(y)

  if (n < 4L) {
    .log_step("[MK TREND] Fewer than 4 observations -- skipping.", verbose)
    return(dplyr::tibble(
      mk_s=NA_real_, mk_tau=NA_real_, mk_z=NA_real_, mk_p=NA_real_,
      trend_direction="insufficient data", breakpoint_year=NA_integer_,
      seg1_slope=NA_real_, seg2_slope=NA_real_,
      seg1_r2=NA_real_, seg2_r2=NA_real_
    ))
  }

  # ---- Mann-Kendall S statistic ----
  pairs_idx <- which(lower.tri(matrix(TRUE, n, n)), arr.ind = TRUE)
  s_vec     <- sign(y[pairs_idx[, 1L]] - y[pairs_idx[, 2L]])
  mk_s      <- sum(s_vec)
  var_s     <- n * (n - 1L) * (2L * n + 5L) / 18L
  mk_z      <- if (mk_s > 0) (mk_s - 1L) / sqrt(var_s) else
               if (mk_s < 0) (mk_s + 1L) / sqrt(var_s) else 0
  mk_p      <- 2 * (1 - stats::pnorm(abs(mk_z)))
  mk_tau    <- mk_s / choose(n, 2L)

  trend_direction <- dplyr::case_when(
    mk_p >= alpha              ~ "No significant trend",
    mk_tau > 0 & mk_p < alpha ~ "Significant increase",
    mk_tau < 0 & mk_p < alpha ~ "Significant decrease",
    TRUE                       ~ "Ambiguous"
  )

  # ---- Breakpoint detection: minimize two-segment RSS ----
  breakpoint_year <- NA_integer_
  seg1_slope      <- NA_real_
  seg2_slope      <- NA_real_
  seg1_r2         <- NA_real_
  seg2_r2         <- NA_real_

  if (n >= 2L * min_seg + 1L) {
    best_rss <- Inf
    best_bp  <- NA_integer_

    for (bp_idx in min_seg:(n - min_seg)) {
      fit1 <- suppressWarnings(stats::lm(y[seq_len(bp_idx)] ~ x[seq_len(bp_idx)]))
      fit2 <- suppressWarnings(stats::lm(y[(bp_idx + 1L):n] ~ x[(bp_idx + 1L):n]))
      rss  <- sum(stats::resid(fit1)^2) + sum(stats::resid(fit2)^2)
      if (rss < best_rss) {
        best_rss <- rss
        best_bp  <- bp_idx
      }
    }

    breakpoint_year <- as.integer(x[best_bp])

    # Theil-Sen slope: non-parametric, robust to non-normal residuals.
    # For a segment of n years: slope = median of all pairwise slopes.
    # More appropriate than OLS for short, count-valued segments.
    .theil_sen_slope <- function(xi, yi) {
      idx <- seq_along(xi)
      pairs <- expand.grid(i = idx, j = idx)
      pairs <- pairs[pairs$j > pairs$i, ]
      if (nrow(pairs) == 0L) return(NA_real_)
      slopes <- (yi[pairs$j] - yi[pairs$i]) / (xi[pairs$j] - xi[pairs$i])
      round(median(slopes, na.rm = TRUE), 2L)
    }

    x1 <- x[seq_len(best_bp)];       y1 <- y[seq_len(best_bp)]
    x2 <- x[(best_bp + 1L):n];       y2 <- y[(best_bp + 1L):n]

    seg1_slope <- .theil_sen_slope(x1, y1)
    seg2_slope <- .theil_sen_slope(x2, y2)

    # OLS R² on log-count for goodness-of-fit reporting only
    # (not for inference — slope inference uses Theil-Sen above)
    ly1 <- log(pmax(y1, 1L)); ly2 <- log(pmax(y2, 1L))
    fit1_best <- suppressWarnings(stats::lm(ly1 ~ x1))
    fit2_best <- suppressWarnings(stats::lm(ly2 ~ x2))
    seg1_r2    <- round(suppressWarnings(summary(fit1_best))$r.squared, 3L)
    seg2_r2    <- round(suppressWarnings(summary(fit2_best))$r.squared, 3L)
  }

  .log_step(sprintf(
    "[MK TREND] tau=%.3f  z=%.3f  p=%s  dir=%s  bp=%s",
    mk_tau, mk_z,
    ifelse(mk_p < 0.001, "<0.001", round(mk_p, 3L)),
    trend_direction,
    ifelse(is.na(breakpoint_year), "NA", as.character(breakpoint_year))
  ), verbose)

  # Flag breakpoints near MeSH vocabulary introduction years (1996-2004)
  # as potentially artifactual when year_start predates 1998.
  mesh_artifact_flag <- !is.na(breakpoint_year) &&
    breakpoint_year >= 1996L && breakpoint_year <= 2004L &&
    as.integer(x[1L]) < 1998L
  if (mesh_artifact_flag) {
    .log_step(sprintf(
      paste(
        "[MK TREND] WARNING: breakpoint at %d may reflect MeSH",
        "indexing changes (1998-2002), not true growth.",
        "Flag in manuscript limitations."
      ),
      breakpoint_year
    ), verbose)
  }

  dplyr::tibble(
    mk_s               = round(mk_s, 1L),
    mk_tau             = round(mk_tau, 3L),
    mk_z               = round(mk_z, 3L),
    mk_p               = round(mk_p, 4L),
    trend_direction    = trend_direction,
    mesh_artifact_flag = mesh_artifact_flag,
    breakpoint_year = breakpoint_year,
    seg1_slope      = seg1_slope,
    seg2_slope      = seg2_slope,
    seg1_r2         = seg1_r2,
    seg2_r2         = seg2_r2
  )
}

#' @noRd
.compute_evidence_evolution <- function(
    bibliography,
    eras    = NULL,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  if (is.null(eras)) {
    yr_range <- range(bibliography$publication_year, na.rm = TRUE)
    eras     <- make_eras(yr_range[1L], yr_range[2L])
  }
  assertthat::assert_that(is.list(eras) && length(eras) >= 2L)
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[EVID EVOL] Computing evidence pyramid by era ...", verbose)

  if (!"PT" %in% names(bibliography) ||
      !"publication_year" %in% names(bibliography)) {
    .log_step("[EVID EVOL] PT or publication_year column missing.", verbose)
    return(dplyr::tibble(
      era=character(), study_type=character(),
      n_papers=integer(), pct_of_era=numeric()
    ))
  }

  purrr::map_dfr(names(eras), function(era_name) {
    yr_range   <- eras[[era_name]]
    era_papers <- bibliography |>
      dplyr::filter(
        .data$publication_year >= yr_range[1L],
        .data$publication_year <= yr_range[2L]
      )
    if (nrow(era_papers) == 0L) return(dplyr::tibble())

    st <- .compute_study_type_metrics(era_papers, verbose = FALSE)
    if (nrow(st) == 0L) return(dplyr::tibble())

    st |>
      dplyr::mutate(
        era          = era_name,
        n_era_papers = nrow(era_papers)
      )
  }) |>
    dplyr::select("era", "study_type", "n_papers", "pct_of_total",
                  "n_era_papers")
}

#' @noRd
.compute_equity_metrics <- function(
    bibliography,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[EQUITY] Computing geographic equity metrics ...", verbose)

  if (!"AU_CO" %in% names(bibliography)) {
    .log_step("[EQUITY] No AU_CO column -- returning NAs.", verbose)
    return(dplyr::tibble(
      pct_high_income    = NA_real_,
      pct_upper_mid      = NA_real_,
      pct_lower_mid      = NA_real_,
      pct_low_income     = NA_real_,
      pct_unmapped       = NA_real_,
      n_papers_with_auco = 0L,
      concentration_hhi  = NA_real_
    ))
  }

  co_col    <- bibliography$AU_CO
  has_data  <- !is.na(co_col) & nchar(trimws(co_col)) > 0L
  co_valid  <- co_col[has_data]
  n_papers  <- length(co_valid)

  if (n_papers == 0L) {
    return(dplyr::tibble(
      pct_high_income    = NA_real_,
      pct_upper_mid      = NA_real_,
      pct_lower_mid      = NA_real_,
      pct_low_income     = NA_real_,
      pct_unmapped       = NA_real_,
      n_papers_with_auco = 0L,
      concentration_hhi  = NA_real_
    ))
  }

  # Lead country = first country listed in AU_CO (lead institution)
  lead_country <- toupper(trimws(
    stringr::str_extract(co_valid, "^[^;]+")
  ))
  lead_country <- purrr::map_chr(lead_country, .normalize_country_string)

  income_tiers  <- purrr::map_chr(lead_country, .wb_income_tier)

  n_high     <- sum(income_tiers == "High",         na.rm = TRUE)
  n_upper_m  <- sum(income_tiers == "Upper-Middle",  na.rm = TRUE)
  n_lower_m  <- sum(income_tiers == "Lower-Middle",  na.rm = TRUE)
  n_low      <- sum(income_tiers == "Low",           na.rm = TRUE)
  n_unmapped <- sum(is.na(income_tiers))

  # Herfindahl-Hirschman Index (country concentration):
  # HHI = sum of squared market shares; 0 = perfectly distributed
  country_counts <- table(lead_country)
  country_shares <- country_counts / n_papers
  hhi <- round(sum(country_shares^2), 3L)

  result <- dplyr::tibble(
    pct_high_income    = round(n_high    / n_papers * 100, 1L),
    pct_upper_mid      = round(n_upper_m / n_papers * 100, 1L),
    pct_lower_mid      = round(n_lower_m / n_papers * 100, 1L),
    pct_low_income     = round(n_low     / n_papers * 100, 1L),
    pct_unmapped       = round(n_unmapped / n_papers * 100, 1L),
    n_papers_with_auco = n_papers,
    concentration_hhi  = hhi
  )

  .log_step(sprintf(
    "[EQUITY] High=%.1f%%  Upper-Mid=%.1f%%  Lower-Mid=%.1f%%  Low=%.1f%%  HHI=%.3f",
    result$pct_high_income, result$pct_upper_mid,
    result$pct_lower_mid, result$pct_low_income, hhi
  ), verbose)

  result
}


# ============================================================
# COMPARISON PLOT FUNCTIONS (exported)
# ============================================================

#' Plot Publication Volume Comparison Across Subspecialties
#'
#' @description
#' Horizontal bar chart of total publication volume for each subspecialty,
#' sorted descending. FPMRS bar is highlighted in a distinct color.
#'
#' @param comparison_summary_table Tibble from
#'   \code{run_subspecialty_comparison()$comparison_table}.
#' @param highlight_subspecialty Character. Subspecialty name to highlight.
#'   Defaults to \code{"FPMRS"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr mutate arrange desc
#' @importFrom ggplot2 ggplot aes geom_col geom_text scale_fill_manual
#'   scale_x_continuous coord_flip labs
#' @importFrom scales label_comma
#' @importFrom rlang .data
#' @export
plot_subspecialty_volume_comparison <- function(
    comparison_summary_table,
    highlight_subspecialty = "FPMRS",
    verbose                = TRUE
) {
  assertthat::assert_that(is.data.frame(comparison_summary_table))
  assertthat::assert_that(assertthat::is.string(highlight_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("subspecialty", "total_documents") %in%
      names(comparison_summary_table)),
    msg = "comparison_summary_table must contain 'subspecialty' and 'total_documents'."
  )

  .log_step("[PLOT] Building volume comparison bar chart ...", verbose)

  plot_data <- comparison_summary_table |>
    dplyr::arrange(dplyr::desc(.data$total_documents)) |>
    dplyr::mutate(
      subspecialty = factor(
        .data$subspecialty,
        levels = rev(.data$subspecialty)
      ),
      bar_fill = dplyr::if_else(
        .data$subspecialty == highlight_subspecialty,
        "highlight", "other"
      )
    )

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x    = .data$subspecialty,
      y    = .data$total_documents,
      fill = .data$bar_fill
    )
  ) +
    ggplot2::geom_col(width = 0.72) +
    ggplot2::geom_text(
      ggplot2::aes(
        label = scales::label_comma()(.data$total_documents)
      ),
      hjust  = -0.1,
      size   = 3.2,
      color  = "grey30"
    ) +
    ggplot2::scale_fill_manual(
      values = c(highlight = "#2C7BB6", other = "#BDBDBD"),
      guide  = "none"
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.18))
    ) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title    = "Total Publication Volume by Subspecialty",
      subtitle = sprintf(
        "%s highlighted | PubMed 2000-2023",
        highlight_subspecialty
      ),
      x        = NULL,
      y        = "Total Publications"
    ) +
    .theme_fpmrs_manuscript()

  .log_step("[PLOT] Volume comparison figure complete.", verbose)
  return(fig)
}


#' Plot Citation Impact Comparison Across Subspecialties
#'
#' @description
#' Two-panel figure: (left) total citations per subspecialty;
#' (right) mean citations per paper. FPMRS highlighted.
#'
#' @param comparison_summary_table Tibble from
#'   \code{run_subspecialty_comparison()$comparison_table}.
#' @param highlight_subspecialty Character. Defaults to \code{"FPMRS"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{patchwork} object.
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr mutate arrange desc if_else
#' @importFrom ggplot2 ggplot aes geom_col coord_flip scale_fill_manual
#'   scale_y_continuous expansion labs
#' @importFrom patchwork wrap_plots plot_annotation
#' @importFrom scales label_comma label_number
#' @importFrom rlang .data
#' @export
plot_subspecialty_citation_comparison <- function(
    comparison_summary_table,
    highlight_subspecialty = "FPMRS",
    verbose                = TRUE
) {
  assertthat::assert_that(is.data.frame(comparison_summary_table))
  assertthat::assert_that(assertthat::is.string(highlight_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("subspecialty", "total_citations", "mean_citations_per_paper")
      %in% names(comparison_summary_table))
  )

  .log_step("[PLOT] Building citation comparison two-panel figure ...", verbose)

  ordered_subspecialties <- comparison_summary_table |>
    dplyr::arrange(dplyr::desc(.data$total_citations)) |>
    dplyr::pull(.data$subspecialty)

  make_bar_data <- function(tbl) {
    tbl |>
      dplyr::mutate(
        subspecialty = factor(
          .data$subspecialty, levels = rev(ordered_subspecialties)
        ),
        bar_fill = dplyr::if_else(
          as.character(.data$subspecialty) == highlight_subspecialty,
          "highlight", "other"
        )
      )
  }

  panel_total <- ggplot2::ggplot(
    make_bar_data(comparison_summary_table),
    ggplot2::aes(
      x    = .data$subspecialty,
      y    = .data$total_citations,
      fill = .data$bar_fill
    )
  ) +
    ggplot2::geom_col(width = 0.72) +
    ggplot2::scale_fill_manual(
      values = c(highlight = "#238B45", other = "#BDBDBD"),
      guide  = "none"
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.1))
    ) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title = "A. Total Citations",
      x     = NULL,
      y     = "Total Citations"
    ) +
    .theme_fpmrs_manuscript()

  panel_mean <- ggplot2::ggplot(
    make_bar_data(comparison_summary_table),
    ggplot2::aes(
      x    = .data$subspecialty,
      y    = .data$mean_citations_per_paper,
      fill = .data$bar_fill
    )
  ) +
    ggplot2::geom_col(width = 0.72) +
    ggplot2::scale_fill_manual(
      values = c(highlight = "#756BB1", other = "#BDBDBD"),
      guide  = "none"
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_number(accuracy = 0.1),
      expand = ggplot2::expansion(mult = c(0, 0.1))
    ) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title = "B. Mean Citations per Paper",
      x     = NULL,
      y     = "Mean Citations / Paper"
    ) +
    .theme_fpmrs_manuscript()

  combined_fig <- patchwork::wrap_plots(
    panel_total, panel_mean, ncol = 2L
  ) +
    patchwork::plot_annotation(
      title    = "Citation Impact by Subspecialty",
      subtitle = sprintf("%s highlighted", highlight_subspecialty),
      theme    = ggplot2::theme(
        plot.title    = ggplot2::element_text(face = "bold", size = 13),
        plot.subtitle = ggplot2::element_text(color = "grey40", size = 10)
      )
    )

  .log_step("[PLOT] Citation comparison figure complete.", verbose)
  return(combined_fig)
}


#' Plot Annual Publication Trends Overlaid for All Subspecialties
#'
#' @description
#' Line chart overlaying annual publication volume for all subspecialties.
#' Useful for visualizing growth timing and relative scale.
#'
#' @param overlaid_annual_trends Tibble from
#'   \code{run_subspecialty_comparison()$overlaid_trends}.
#' @param log_scale Logical. Whether to apply log10 y-axis scale.
#'   Recommended when Urology is included due to scale difference.
#'   Defaults to \code{TRUE}.
#' @param highlight_subspecialty Character. Subspecialty to draw in bold.
#'   Defaults to \code{"FPMRS"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.flag is.string
#' @importFrom dplyr mutate
#' @importFrom ggplot2 ggplot aes geom_line scale_color_viridis_d
#'   scale_y_log10 scale_y_continuous labs
#' @importFrom rlang .data
#' @export
plot_subspecialty_trends_overlay <- function(
    overlaid_annual_trends,
    log_scale              = TRUE,
    highlight_subspecialty = "FPMRS",
    verbose                = TRUE
) {
  assertthat::assert_that(is.data.frame(overlaid_annual_trends))
  assertthat::assert_that(assertthat::is.flag(log_scale))
  assertthat::assert_that(assertthat::is.string(highlight_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("subspecialty", "publication_year", "publication_count")
      %in% names(overlaid_annual_trends))
  )

  .log_step(sprintf(
    "[PLOT] Building trend overlay (log_scale=%s) ...", log_scale
  ), verbose)

  n_subspecialties <- dplyr::n_distinct(
    overlaid_annual_trends$subspecialty
  )

  line_data <- overlaid_annual_trends |>
    dplyr::mutate(
      line_size = dplyr::if_else(
        .data$subspecialty == highlight_subspecialty, 1.6, 0.7
      ),
      line_alpha = dplyr::if_else(
        .data$subspecialty == highlight_subspecialty, 1.0, 0.65
      )
    )

  fig <- ggplot2::ggplot(
    line_data,
    ggplot2::aes(
      x     = .data$publication_year,
      y     = .data$publication_count,
      color = .data$subspecialty,
      linewidth = .data$line_size,
      alpha = .data$line_alpha
    )
  ) +
    ggplot2::geom_line() +
    ggplot2::scale_color_viridis_d(
      option = "turbo",
      name   = "Subspecialty"
    ) +
    ggplot2::scale_linewidth_identity() +
    ggplot2::scale_alpha_identity()

  if (isTRUE(log_scale)) {
    fig <- fig +
      ggplot2::scale_y_log10(
        labels = scales::label_comma()
      ) +
      ggplot2::labs(
        title    = "Annual Publication Volume by Subspecialty (log scale)",
        subtitle = sprintf(
          "%s bolded | log10 y-axis to accommodate Urology scale",
          highlight_subspecialty
        ),
        x = "Publication Year",
        y = "Publications (log10 scale)"
      )
  } else {
    fig <- fig +
      ggplot2::scale_y_continuous(
        labels = scales::label_comma(),
        expand = ggplot2::expansion(mult = c(0, 0.05))
      ) +
      ggplot2::labs(
        title    = "Annual Publication Volume by Subspecialty",
        subtitle = sprintf("%s bolded", highlight_subspecialty),
        x        = "Publication Year",
        y        = "Number of Publications"
      )
  }

  fig <- fig + .theme_fpmrs_manuscript()

  .log_step("[PLOT] Trend overlay figure complete.", verbose)
  return(fig)
}


#' Plot CAGR Comparison Across Subspecialties
#'
#' @description
#' Horizontal bar chart of compound annual growth rate (CAGR) per
#' subspecialty. FPMRS highlighted. Bars colored by whether CAGR is
#' above or below the cohort median.
#'
#' @param comparison_summary_table Tibble from
#'   \code{run_subspecialty_comparison()$comparison_table}.
#' @param highlight_subspecialty Character. Defaults to \code{"FPMRS"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr mutate arrange
#' @importFrom ggplot2 ggplot aes geom_col geom_vline coord_flip
#'   scale_fill_manual scale_x_continuous labs
#' @importFrom rlang .data
#' @export
plot_subspecialty_cagr_comparison <- function(
    comparison_summary_table,
    highlight_subspecialty = "FPMRS",
    verbose                = TRUE
) {
  assertthat::assert_that(is.data.frame(comparison_summary_table))
  assertthat::assert_that(assertthat::is.string(highlight_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("subspecialty", "cagr_pct") %in%
      names(comparison_summary_table))
  )

  .log_step("[PLOT] Building CAGR comparison figure ...", verbose)

  median_cagr <- stats::median(
    comparison_summary_table$cagr_pct, na.rm = TRUE
  )

  plot_data <- comparison_summary_table |>
    dplyr::filter(!is.na(.data$cagr_pct)) |>
    dplyr::arrange(.data$cagr_pct) |>
    dplyr::mutate(
      subspecialty = factor(
        .data$subspecialty, levels = .data$subspecialty
      ),
      bar_fill = dplyr::case_when(
        as.character(.data$subspecialty) == highlight_subspecialty
          ~ "highlight",
        .data$cagr_pct >= median_cagr ~ "above_median",
        TRUE                          ~ "below_median"
      )
    )

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x    = .data$subspecialty,
      y    = .data$cagr_pct,
      fill = .data$bar_fill
    )
  ) +
    ggplot2::geom_col(width = 0.72) +
    ggplot2::geom_hline(
      yintercept = median_cagr,
      linetype   = "dashed",
      color      = "grey50",
      linewidth  = 0.6
    ) +
    ggplot2::geom_text(
      ggplot2::aes(label = sprintf("%.1f%%", .data$cagr_pct)),
      hjust = -0.15,
      size  = 3.2,
      color = "grey30"
    ) +
    ggplot2::scale_fill_manual(
      values = c(
        highlight    = "#D7191C",
        above_median = "#74C476",
        below_median = "#BDBDBD"
      ),
      labels = c(
        highlight    = highlight_subspecialty,
        above_median = "Above median",
        below_median = "Below median"
      ),
      name = NULL
    ) +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      expand = ggplot2::expansion(mult = c(0, 0.18))
    ) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title    = "Compound Annual Growth Rate (CAGR) by Subspecialty",
      subtitle = sprintf(
        "Dashed line = cohort median (%.1f%%) | %s highlighted",
        median_cagr, highlight_subspecialty
      ),
      x        = NULL,
      y        = "CAGR (%)"
    ) +
    .theme_fpmrs_manuscript()

  .log_step("[PLOT] CAGR comparison figure complete.", verbose)
  return(fig)
}


#' Plot Subspecialty Performance Heatmap
#'
#' @description
#' Tile heatmap with subspecialties as rows and normalized metrics as
#' columns. Cell color encodes normalized rank (0 = lowest, 1 = highest).
#' Provides a rapid at-a-glance positioning of each subspecialty across
#' all dimensions.
#'
#' @param comparison_summary_table Tibble from
#'   \code{run_subspecialty_comparison()$comparison_table}.
#' @param metrics_to_display Character vector of column names to include.
#'   Defaults to the five key abstract metrics.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr mutate
#' @importFrom ggplot2 ggplot aes geom_tile geom_text
#'   scale_fill_gradientn scale_x_discrete labs theme element_text
#' @importFrom rlang .data
#' @export
plot_subspecialty_heatmap <- function(
    comparison_summary_table,
    metrics_to_display = c(
      "total_documents",
      "total_citations",
      "mean_citations_per_paper",
      "cagr_pct",
      "unique_countries",
      "mean_authors_per_paper",
      "pct_large_team_10plus"
    ),
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(comparison_summary_table))
  assertthat::assert_that(is.character(metrics_to_display))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step("[PLOT] Building subspecialty heatmap ...", verbose)

  metric_labels <- c(
    total_documents          = "Total\nPublications",
    total_citations          = "Total\nCitations",
    mean_citations_per_paper = "Mean Citations\nper Paper",
    cagr_pct                 = "CAGR (%)",
    unique_countries         = "Unique\nCountries",
    mean_authors_per_paper   = "Mean Authors\nper Paper",
    median_authors_per_paper = "Median Authors\nper Paper",
    pct_large_team_10plus    = "Large Team\n(>=10 Authors, %)"
  )

  heatmap_data <- .build_comparison_heatmap_data(
    comparison_summary_table = comparison_summary_table,
    metrics_to_include       = metrics_to_display,
    verbose                  = verbose
  ) |>
    dplyr::mutate(
      metric_label = dplyr::recode(
        .data$metric, !!!metric_labels, .default = .data$metric
      ),
      display_value = round(.data$raw_value, 1L)
    )

  # Order subspecialties by total_documents for consistent y-axis
  subspecialty_order <- comparison_summary_table |>
    dplyr::arrange(dplyr::desc(.data$total_documents)) |>
    dplyr::pull(.data$subspecialty)

  heatmap_data <- heatmap_data |>
    dplyr::mutate(
      subspecialty = factor(
        .data$subspecialty, levels = rev(subspecialty_order)
      )
    )

  fig <- ggplot2::ggplot(
    heatmap_data,
    ggplot2::aes(
      x    = .data$metric_label,
      y    = .data$subspecialty,
      fill = .data$normalized
    )
  ) +
    ggplot2::geom_tile(color = "white", linewidth = 0.6) +
    ggplot2::geom_text(
      ggplot2::aes(
        label = scales::label_comma(accuracy = 0.1)(
          .data$display_value
        )
      ),
      size  = 2.9,
      color = "grey15"
    ) +
    ggplot2::scale_fill_gradientn(
      colors   = c("#F7FBFF", "#6BAED6", "#08306B"),
      name     = "Normalized\nScore",
      limits   = c(0, 1),
      breaks   = c(0, 0.5, 1),
      labels   = c("Low", "Mid", "High")
    ) +
    ggplot2::scale_x_discrete(position = "top") +
    ggplot2::labs(
      title    = "Subspecialty Performance Heatmap",
      subtitle = "Cell values = raw metric | Color = normalized 0-1 within each column",
      x        = NULL,
      y        = NULL
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      axis.text.x = ggplot2::element_text(size = 8.5, face = "bold"),
      axis.text.y = ggplot2::element_text(size = 9)
    )

  .log_step("[PLOT] Subspecialty heatmap complete.", verbose)
  return(fig)
}


# ============================================================
# ABSTRACT TEXT GENERATOR
# ============================================================

#' @noRd
.expand_abstract_abbrevs <- function(text, subspecialty_labels = NULL) {
  if (is.na(text) || nchar(trimws(text)) == 0L) return(text)
  metric_subs <- list(
    " [BH]-corrected"  = "-corrected",
    " [BH] correction" = " correction",
    " [BH]"            = "",
    " [RCR]"           = "",
    " [IRR]"           = "",
    " [CI]"            = "",
    " [HHI]"           = "",
    " [CAGR]"          = "",
    " (DI)"            = "",
    " (RCTs)"          = "s",
    " (CAGR)"          = "",
    "OB/GYN"           = "obstetrics and gynecology"
  )
  for (pattern in names(metric_subs))
    text <- gsub(pattern, metric_subs[[pattern]], text, fixed = TRUE)
  if (!is.null(subspecialty_labels) && length(subspecialty_labels) > 0L) {
    for (short in names(subspecialty_labels)) {
      full <- subspecialty_labels[[short]]
      text <- gsub(paste0("\\b", short, "\\b"), full, text, perl = TRUE)
    }
  }
  # Capitalise first letter after ". " and "; " (sentence boundaries)
  text <- gsub("([.][[:space:]]+)([a-z])", "\\1\\U\\2", text, perl = TRUE)
  text <- gsub("([;][[:space:]]+)([a-z])", "\\1\\U\\2", text, perl = TRUE)
  # Capitalise very first character of the whole text
  if (nchar(text) > 0L) substr(text, 1L, 1L) <- toupper(substr(text, 1L, 1L))
  text
}

#' Generate Abstract Results Text from Comparison Table
#'
#' @description
#' Auto-fills the Results and Methods sections of a comparative subspecialty
#' bibliometric abstract. Returns a named list of character strings, one per
#' abstract sentence or paragraph. Paste directly into your manuscript or
#' use \code{cat()} to preview all sentences at once.
#'
#' Six improvements over the original generator:
#' \enumerate{
#'   \item Geography sentence split: international breadth reported
#'     separately from US concentration (no longer self-contradicting).
#'   \item Cohort labeling: explicitly names "7 OB/GYN subspecialties
#'     and urology" rather than the vague "comparator specialty."
#'   \item Growth sentence: detects near-ties (gap < 0.5 pp) and
#'     names subspecialties FPMRS meaningfully outpaces (gap > 0.5 pp).
#'   \item Institution sentence: names the most productive institution.
#'   \item Keyword sentence: identifies the top 3 emerging keywords
#'     by growth ratio in the most recent 3 years.
#'   \item Methods sentence: auto-filled search and analysis description.
#' }
#'
#' @param comparison_summary_table Tibble from
#'   \code{run_subspecialty_comparison()$comparison_table}.
#' @param focal_subspecialty Character. The subspecialty the manuscript
#'   is about. Defaults to \code{"FPMRS"}.
#' @param focal_institution_metrics Tibble or \code{NULL}. The
#'   \code{institution_metrics} element from the focal subspecialty's
#'   pipeline result. When supplied, adds an institution sentence.
#'   Defaults to \code{NULL}.
#' @param focal_keyword_trends Tibble or \code{NULL}. The
#'   \code{keyword_trends} element from the focal subspecialty's
#'   pipeline result. When supplied, adds an emerging-keyword sentence.
#'   Defaults to \code{NULL}.
#' @param data_source Character. Data source label for the methods
#'   sentence. Defaults to \code{"PubMed"}.
#' @param cagr_near_tie_threshold Numeric. Maximum CAGR gap (percentage
#'   points) between focal subspecialty and the leader before the growth
#'   sentence describes it as a near-tie rather than a deficit. Defaults
#'   to \code{0.5}.
#' @param year_start Integer. First year of analysis window.
#' @param year_end Integer. Last year of analysis window.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A named list of character strings:
#' \describe{
#'   \item{\code{methods}}{Search and analysis methods sentence.}
#'   \item{\code{corpus}}{Total documents, year span, rank vs cohort.}
#'   \item{\code{citations}}{Total and mean citations, cohort ranks.}
#'   \item{\code{growth}}{CAGR, cohort rank, near-tie or outpacing note.}
#'   \item{\code{geography}}{International breadth then US concentration
#'     as two separate clauses.}
#'   \item{\code{institution}}{Most productive institution (if supplied).}
#'   \item{\code{output}}{Most prolific author and highest-cited journal.}
#'   \item{\code{keywords}}{Top 3 emerging keyword themes (if supplied).}
#'   \item{\code{urology}}{Volume and growth comparison to urology.}
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag is.number
#' @importFrom dplyr filter pull arrange desc mutate group_by summarise
#'   inner_join slice_head n_distinct case_when if_else
#' @importFrom purrr walk2
#' @export
generate_abstract_results_text <- function(
    comparison_summary_table,
    focal_subspecialty        = "FPMRS",
    focal_institution_metrics = NULL,
    focal_keyword_trends      = NULL,
    focal_mk_trend            = NULL,
    focal_disruption_index    = NULL,
    focal_equity_metrics      = NULL,
    focal_evidence_quality    = NULL,
    data_source               = "PubMed",
    cagr_near_tie_threshold   = 0.5,
    collab_high_threshold     = 8.0,
    collab_low_threshold      = 4.5,
    di_pct_disruptive_threshold  = 95.0,  # sentence fires when pct_disruptive >= this
    di_pct_consolidating_threshold = 40.0,  # sentence fires when pct_consolidating >= this
    di_direction_threshold       = 60.0,  # label "predominantly disruptive" when pct_dis >= this
    statistics_results        = NULL,
    equity_hhi_threshold         = 0.25,  # HHI above which authorship is "concentrated"
    equity_hic_threshold         = 70.0,  # % high-income above which sentence fires
    equity_lmic_threshold        = 15.0,  # % LMIC below which sentence fires
    year_start                = 1975L,
    year_end                  = 2023L,
    statistics_alpha          = 0.05,
    subspecialty_labels       = NULL,
    verbose                   = TRUE
) {
  # ---- Validate ----
  assertthat::assert_that(is.data.frame(comparison_summary_table))
  if (!is.null(statistics_results)) {
    assertthat::assert_that(
      is.list(statistics_results),
      msg = "`statistics_results` must be a list from compute_comparison_statistics() or NULL."
    )
  }
  assertthat::assert_that(assertthat::is.string(focal_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    assertthat::is.number(statistics_alpha) &&
      statistics_alpha > 0 && statistics_alpha < 1,
    msg = sprintf("`statistics_alpha` must be in (0, 1). Received: %g",
                  statistics_alpha)
  )
  assertthat::assert_that(assertthat::is.string(data_source))
  assertthat::assert_that(assertthat::is.number(cagr_near_tie_threshold))
  assertthat::assert_that(assertthat::is.number(collab_high_threshold))
  assertthat::assert_that(assertthat::is.number(collab_low_threshold))
  assertthat::assert_that(
    assertthat::is.number(equity_hhi_threshold) &&
      equity_hhi_threshold >= 0 && equity_hhi_threshold <= 1,
    msg = "`equity_hhi_threshold` must be in [0, 1]."
  )
  assertthat::assert_that(
    assertthat::is.number(equity_hic_threshold) &&
      equity_hic_threshold >= 0 && equity_hic_threshold <= 100,
    msg = "`equity_hic_threshold` must be in [0, 100]."
  )
  assertthat::assert_that(
    assertthat::is.number(equity_lmic_threshold) &&
      equity_lmic_threshold >= 0 && equity_lmic_threshold <= 100,
    msg = "`equity_lmic_threshold` must be in [0, 100]."
  )
  assertthat::assert_that(
    collab_low_threshold < collab_high_threshold,
    msg = "`collab_low_threshold` must be less than `collab_high_threshold`."
  )
  assertthat::assert_that(
    focal_subspecialty %in% comparison_summary_table$subspecialty,
    msg = sprintf(
      "'%s' not found in comparison_summary_table$subspecialty. Available: %s",
      focal_subspecialty,
      paste(comparison_summary_table$subspecialty, collapse = ", ")
    )
  )
  if (!is.null(focal_institution_metrics)) {
    assertthat::assert_that(
      is.data.frame(focal_institution_metrics),
      msg = "`focal_institution_metrics` must be a data frame or NULL."
    )
  }
  if (!is.null(focal_keyword_trends)) {
    assertthat::assert_that(
      is.data.frame(focal_keyword_trends),
      msg = "`focal_keyword_trends` must be a data frame or NULL."
    )
  }

  # ---- Helpers ----
  fmt_n   <- function(x) scales::label_comma()(x)
  fmt_pct <- function(x) sprintf("%.1f%%", x)
  correction_method <- if (
    !is.null(statistics_results) &&
    !is.null(statistics_results$pairwise_citations)
  ) "BH" else "BH"
  `%||%` <- function(a, b) if (!is.null(a)) a else b

  focal <- dplyr::filter(
    comparison_summary_table,
    .data$subspecialty == focal_subspecialty
  )

  # Detect urology as external comparator; remaining = OB/GYN subspecialties
  urology_row <- dplyr::filter(
    comparison_summary_table,
    grepl("urology", tolower(.data$subspecialty))
  )
  n_obgyn_subspecialties <- nrow(comparison_summary_table) -
    nrow(urology_row)

  cohort_label <- if (nrow(urology_row) > 0L) {
    sprintf(
      "%d OB/GYN subspecialt%s and urology",
      n_obgyn_subspecialties,
      ifelse(n_obgyn_subspecialties == 1L, "y", "ies")
    )
  } else {
    n_sp_cohort <- nrow(comparison_summary_table)
    sprintf("%d OB/GYN subspecialt%s", n_sp_cohort,
            ifelse(n_sp_cohort == 1L, "y", "ies"))
  }

  # ---- Manuscript title ----
  n_comparators_title <- nrow(comparison_summary_table) - 1L
  has_urology_title   <- nrow(urology_row) > 0L
  comparator_desc_title <- if (has_urology_title) {
    sprintf("%d obstetrics and gynecology subspecialt%s and urology",
            n_comparators_title,
            ifelse(n_comparators_title == 1L, "y", "ies"))
  } else {
    sprintf("%d obstetrics and gynecology subspecialt%s",
            n_comparators_title,
            ifelse(n_comparators_title == 1L, "y", "ies"))
  }
  s_title <- sprintf(
    paste(
      "%s Research Output, Evidence Quality, and Global Authorship,",
      "%d\u2013%d: A Comparative Bibliometric Analysis Among %s."
    ),
    focal_subspecialty,
    year_start,
    year_end,
    comparator_desc_title
  )

  # ---- Sentence: Introduction ----
  # Frames the clinical burden, knowledge gap, and study purpose.
  s_introduction <- sprintf(
    paste(
      "%s is a surgical subspecialty of obstetrics and gynecology",
      "that addresses pelvic floor disorders affecting an estimated",
      "one in four women; despite documented workforce shortages and",
      "growing patient demand, the research trajectory, evidence",
      "maturity, and global scholarly footprint of %s relative to",
      "comparator subspecialties have not been systematically",
      "characterised.",
      "We conducted a comprehensive bibliometric analysis of %s",
      "peer-reviewed literature published from %d to %d and",
      "benchmarked key output metrics against %s to identify",
      "growth patterns, evidence quality gaps, and opportunities",
      "to prioritise future research investment."
    ),
    focal_subspecialty,
    focal_subspecialty,
    focal_subspecialty,
    year_start,
    year_end,
    cohort_label
  )

  # ---- Sentence: Methods ----
  s_methods <- sprintf(
    paste(
      "A systematic search of the %s database was conducted",
      "using validated Medical Subject Headings (MeSH) terms and title/abstract keywords",
      "for %s and %s comparator specialties; records published",
      "between %d and %d were retrieved and analyzed using the",
      "bibliometrix R package (v4.x); author name variants were",
      "analyzed as indexed without manual deduplication."
    ),
    data_source,
    focal_subspecialty,
    nrow(comparison_summary_table) - 1L,
    year_start,
    year_end
  )

  # ---- Sentence: Corpus ----
  s_corpus <- sprintf(
    paste(
      "This analysis identified %s %s publications spanning",
      "%d-%d, representing the %s largest corpus among the",
      "%s analyzed."
    ),
    fmt_n(focal$total_documents),
    focal_subspecialty,
    year_start, year_end,
    ordinal_label(focal$rank_by_volume),
    cohort_label
  )

  # ---- Sentence: Citations (with optional KW p-value) ----
  kw_clause <- if (
    !is.null(statistics_results) &&
    !is.null(statistics_results$kruskal_wallis)
  ) {
    sprintf(
      " (Kruskal-Wallis H=%.1f, df=%d, p%s across cohort)",
      statistics_results$kruskal_wallis$H,
      statistics_results$kruskal_wallis$df,
      statistics_results$kruskal_wallis$p_fmt
    )
  } else {
    ""
  }

  # Pairwise p-value vs Urology for citation sentence
  urol_pair_clause <- ""
  if (!is.null(statistics_results) &&
      !is.null(statistics_results$pairwise_citations)) {
    urol_pair <- dplyr::filter(
      statistics_results$pairwise_citations,
      grepl("urology", tolower(.data$subspecialty))
    )
    if (nrow(urol_pair) > 0L) {
      urol_pair_clause <- sprintf(
        "; p%s vs. Urology (Benjamini-Hochberg [BH] correction)",
        urol_pair$p_adj_fmt[[1L]]
      )
    }
  }

  rcr_clause <- if (
    "rcr_median" %in% names(focal) &&
    !is.na(focal[["rcr_median"]]) &&
    "rcr_above_1_pct" %in% names(focal) &&
    !is.na(focal[["rcr_above_1_pct"]])
  ) {
    sprintf(
      "; median Relative Citation Ratio [RCR]=%.2f (%.0f%% of papers above field-average impact)",
      focal[["rcr_median"]], focal[["rcr_above_1_pct"]]
    )
  } else ""

  s_citations <- sprintf(
    paste(
      "%s accumulated %s total citations (mean %.1f per paper),",
      "ranking %s in total citations and %s in mean citation",
      "impact per paper among the cohort%s%s%s."
    ),
    focal_subspecialty,
    fmt_n(focal$total_citations),
    focal$mean_citations_per_paper,
    ordinal_label(focal$rank_by_citations),
    ordinal_label(focal$rank_by_impact),
    kw_clause,
    urol_pair_clause,
    rcr_clause
  )

  # ---- Sentence: Growth -- detect near-tie and meaningful outpacing ----
  valid_cagr_tbl <- comparison_summary_table |>
    dplyr::filter(!is.na(.data$cagr_pct))

  top_cagr_row <- valid_cagr_tbl |>
    dplyr::arrange(dplyr::desc(.data$cagr_pct)) |>
    dplyr::slice_head(n = 1L)

  focal_cagr <- focal$cagr_pct
  top_cagr   <- top_cagr_row$cagr_pct
  cagr_gap   <- top_cagr - focal_cagr

  # Subspecialties FPMRS meaningfully outpaces
  outpaced_subspecialties <- valid_cagr_tbl |>
    dplyr::filter(
      .data$subspecialty != focal_subspecialty,
      !is.na(focal_cagr),
      (focal_cagr - .data$cagr_pct) > cagr_near_tie_threshold
    ) |>
    dplyr::arrange(dplyr::desc(.data$cagr_pct)) |>
    dplyr::pull(.data$subspecialty)

  s_growth <- if (is.na(focal_cagr)) {
    sprintf(
      "%s growth rate data were unavailable for this analysis.",
      focal_subspecialty
    )
  } else if (focal_subspecialty == top_cagr_row$subspecialty) {
    # FPMRS IS the fastest grower
    sprintf(
      paste(
        "%s demonstrated the highest compound annual growth rate",
        "in the cohort (%.1f%%), surpassing all %s comparators."
      ),
      focal_subspecialty, focal_cagr, cohort_label
    )
  } else if (cagr_gap <= cagr_near_tie_threshold) {
    # Near-tie with the leader
    outpacing_clause <- if (length(outpaced_subspecialties) > 0L) {
      sprintf(
        "; %s meaningfully outpaced %s",
        focal_subspecialty,
        paste(outpaced_subspecialties, collapse = ", ")
      )
    } else {
      ""
    }
    sprintf(
      paste(
        "%s demonstrated a compound annual growth rate of %.1f%%,",
        "effectively matching %s (%.1f%%) for the highest growth",
        "velocity in the cohort%s."
      ),
      focal_subspecialty, focal_cagr,
      top_cagr_row$subspecialty, top_cagr,
      outpacing_clause
    )
  } else {
    # Clear ranking
    outpacing_clause <- if (length(outpaced_subspecialties) > 0L) {
      sprintf(
        ", while meaningfully outpacing %s",
        paste(outpaced_subspecialties, collapse = ", ")
      )
    } else {
      ""
    }
    # Poisson p-value + growth heterogeneity F-test for focal growth trend
    poisson_p_clause <- if (
      !is.null(statistics_results) &&
      !is.null(statistics_results$growth_trends)
    ) {
      focal_growth <- dplyr::filter(
        statistics_results$growth_trends,
        .data$subspecialty == focal_subspecialty
      )
      if (nrow(focal_growth) > 0L && !is.na(focal_growth$p_value)) {
        # Add heterogeneity F-test if available
        hetero_clause <- if (
          !is.null(statistics_results$growth_heterogeneity) &&
          !is.na(statistics_results$growth_heterogeneity$F_statistic)
        ) {
          gh <- statistics_results$growth_heterogeneity
          sprintf(
            "; growth slopes differed %sacross subspecialties (analysis of variance F-test: F(%d,%d)=%.2f, p%s)",
            ifelse(gh$significant, "significantly ", "non-significantly "),
            gh$df_numerator, gh$df_denominator,
            gh$F_statistic, gh$p_fmt
          )
        } else ""
        sprintf(
          " (Poisson incidence rate ratio [IRR]=%.4f, 95%% confidence interval [CI] %.4f–%.4f, p%s%s)",
          focal_growth$irr,
          focal_growth$irr_lo95,
          focal_growth$irr_hi95,
          focal_growth$p_fmt,
          hetero_clause
        )
      } else ""
    } else ""

    sprintf(
      paste(
        "%s demonstrated a compound annual growth rate of %.1f%%%s,",
        "ranking %s in growth velocity among the cohort",
        "(%s led at %.1f%%)%s."
      ),
      focal_subspecialty, focal_cagr,
      poisson_p_clause,
      ordinal_label(focal$rank_by_cagr),
      top_cagr_row$subspecialty, top_cagr,
      outpacing_clause
    )
  }

  if (!is.null(focal_mk_trend)) {
    assertthat::assert_that(
      is.data.frame(focal_mk_trend),
      msg = paste(
        "`focal_mk_trend` must be a 1-row data frame from",
        ".compute_mann_kendall_trend(), or NULL."
      )
    )
    assertthat::assert_that(
      all(c("mk_tau","mk_p","trend_direction","breakpoint_year",
            "seg1_slope","seg2_slope") %in% names(focal_mk_trend)),
      msg = paste(
        "`focal_mk_trend` is missing expected columns.",
        "Pass the output of .compute_mann_kendall_trend() directly."
      )
    )
  }

  # ---- Sentence: Mann-Kendall trend + breakpoint (conditional) ----
  # Fires when:
  #   (a) A significant monotonic trend exists, OR
  #   (b) A structural breakpoint was detected.
  # Suppressed when trend is non-significant and no breakpoint found,
  # to keep the abstract within the ~300-word target.
  s_mk_trend <- if (is.null(focal_mk_trend)) {
    NULL
  } else {
    mk_tau  <- focal_mk_trend$mk_tau[[1L]]
    mk_p    <- focal_mk_trend$mk_p[[1L]]
    mk_dir  <- focal_mk_trend$trend_direction[[1L]]
    bp_year <- focal_mk_trend$breakpoint_year[[1L]]
    s1_slope <- focal_mk_trend$seg1_slope[[1L]]
    s2_slope <- focal_mk_trend$seg2_slope[[1L]]

    has_sig_trend  <- !is.na(mk_p) && mk_p < statistics_alpha
    has_breakpoint <- !is.na(bp_year)

    # Build the monotonic trend clause
    trend_clause <- if (!is.na(mk_tau) && !is.na(mk_p)) {
      # OB-journal P format: capital P, no leading zero
      mk_p_fmt <- dplyr::case_when(
        mk_p < 0.001 ~ "P<.001",
        mk_p < 0.10  ~ sub("^0\\.", ".", sprintf("P=%.3f", mk_p)),
        TRUE         ~ sub("^0\\.", ".", sprintf("P=%.2f", mk_p))
      )
      direction_word <- dplyr::case_when(
        mk_tau > 0  ~ "a significant monotonic increase",
        mk_tau < 0  ~ "a significant monotonic decrease",
        TRUE        ~ "no significant directional trend"
      )
      sprintf(
        "%s publication output demonstrated %s over %d\u2013%d (Mann-Kendall \u03c4=%.3f, %s)",
        focal_subspecialty,
        direction_word,
        year_start, year_end,
        mk_tau, mk_p_fmt
      )
    } else {
      NULL
    }

    # Build the breakpoint clause
    bp_clause <- if (has_breakpoint && !is.na(s1_slope) && !is.na(s2_slope)) {
      accel_word <- dplyr::case_when(
        s2_slope > s1_slope  ~ "accelerated",
        s2_slope < s1_slope  ~ "decelerated",
        TRUE                 ~ "remained stable"
      )
      sprintf(
        paste(
          "a structural inflection was identified in %d,",
          "after which annual growth %s (%.1f vs. %.1f",
          "publications/year before and after the breakpoint)"
        ),
        bp_year, accel_word,
        s1_slope, s2_slope
      )
    } else if (has_breakpoint) {
      sprintf("a structural inflection was identified in %d", bp_year)
    } else {
      NULL
    }

    # Assemble: fire when significant trend OR breakpoint
    if (!has_sig_trend && !has_breakpoint) {
      NULL                          # suppress non-notable trend
    } else if (!is.null(trend_clause) && !is.null(bp_clause)) {
      paste0(trend_clause, "; ", bp_clause, ".")
    } else if (!is.null(trend_clause)) {
      paste0(trend_clause, ".")
    } else if (!is.null(bp_clause)) {
      paste0(
        stringr::str_to_sentence(bp_clause), " in ",
        focal_subspecialty, " literature."
      )
    } else {
      NULL
    }
  }
  broadest_sp      <- comparison_summary_table$subspecialty[
    which.max(comparison_summary_table$unique_countries)
  ]
  broadest_n       <- max(comparison_summary_table$unique_countries,
                          na.rm = TRUE)
  focal_countries  <- focal$unique_countries
  focal_country_n  <- if (!is.na(focal$top_country_pct))
    focal$top_country_pct else NA_real_

  breadth_clause <- if (broadest_sp == focal_subspecialty) {
    sprintf(
      "%s had the broadest international reach in the cohort,",
      focal_subspecialty
    )
  } else {
    sprintf(
      "%s spanned %d countries (compared with %d for %s, which had the widest reach in the cohort);",
      focal_subspecialty, focal_countries,
      broadest_n, broadest_sp
    )
  }

  concentration_clause <- if (!is.na(focal_country_n)) {
    top_country_display <- dplyr::case_when(
      focal$top_country == "USA"            ~ "the United States",
      focal$top_country == "UNITED KINGDOM" ~ "the United Kingdom",
      focal$top_country == "UK"             ~ "the United Kingdom",
      TRUE ~ stringr::str_to_title(focal$top_country)
    )
    sprintf(
      "%s of all %s publications originated from %s.",
      fmt_pct(focal_country_n),
      focal_subspecialty,
      top_country_display
    )
  } else {
    sprintf("country-level data were unavailable for %s.",
            focal_subspecialty)
  }

  # Country proportion p-value summary (from statistics_results)
  country_p_clause <- ""
  if (!is.null(statistics_results) &&
      !is.null(statistics_results$country_proportions)) {
    cp_tbl <- statistics_results$country_proportions
    n_sig_cp <- sum(cp_tbl$significant, na.rm = TRUE)
    n_tot_cp <- nrow(cp_tbl)
    # Most extreme p-value (smallest) vs any comparator
    min_p_cp <- min(cp_tbl$p_value, na.rm = TRUE)
    if (n_sig_cp > 0L && is.finite(min_p_cp)) {
      country_p_clause <- sprintf(
        " (p%s vs. %d of %d comparators; Benjamini-Hochberg [BH] correction)",
        .fmt_pvalue(min_p_cp),
        n_sig_cp, n_tot_cp
      )
    }
  }

  s_geography <- if (nchar(country_p_clause) > 0L) {
    # Insert p-value clause before the period at end of concentration_clause
    paste0(
      breadth_clause, " ",
      sub("\\.$", country_p_clause, concentration_clause),
      "."
    )
  } else {
    paste(breadth_clause, concentration_clause)
  }

  # ---- Sentence: Institution (optional) ----
  s_institution <- if (
    !is.null(focal_institution_metrics) &&
    nrow(focal_institution_metrics) > 0L &&
    "institution" %in% names(focal_institution_metrics)
  ) {
    top_inst <- focal_institution_metrics$institution[[1L]]
    top_inst_pubs <- focal_institution_metrics$publication_count[[1L]]
    sprintf(
      paste(
        "%s was the most productive institution in %s",
        "literature, contributing %s publications."
      ),
      stringr::str_to_title(top_inst),
      focal_subspecialty,
      fmt_n(top_inst_pubs)
    )
  } else {
    NULL
  }

  # ---- Sentence: Author + Journal ----
  # str_to_title("BARBER MD") -> "Barber Md" which is wrong.
  # .title_case_name() preserves all-uppercase tokens of <= 2 characters
  # (degree abbreviations, initials) while title-casing the surname.
  # Allowlist of tokens to preserve in ALL-CAPS: degree abbreviations,
  # Roman numerals, and common name suffixes. Anything not on this list
  # is title-cased. This avoids incorrectly preserving "VAN", "DER",
  # "VON" (Dutch/German particles) while correctly keeping "MD", "III".
  .preserve_caps_tokens <- c(
    "MD", "DO", "PHD", "RN", "NP", "PA", "MBA", "JD", "MS", "BS", "BA",
    "MPH", "MSC", "MSPH", "DRPH", "MA", "MHA", "FACOG", "FACS", "FACP",
    "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI", "XII",
    "JR", "SR"
  )
  .title_case_name <- function(x) {
    if (is.na(x) || nchar(trimws(x)) == 0L) return(x)
    tokens <- strsplit(trimws(x), "\\s+")[[1L]]
    titled <- vapply(tokens, function(tok) {
      tok_up <- toupper(tok)
      if (tok_up %in% .preserve_caps_tokens)
        tok_up
      else
        paste0(toupper(substr(tok, 1L, 1L)),
               tolower(substr(tok, 2L, nchar(tok))))
    }, character(1L), USE.NAMES = FALSE)
    paste(titled, collapse = " ")
  }

  s_output <- sprintf(
    paste(
      "The most prolific %s author was %s (%s publications);",
      "the highest-cited outlet was %s."
    ),
    focal_subspecialty,
    .title_case_name(focal$top_author),
    fmt_n(focal$top_author_pubs),
    stringr::str_to_title(focal$top_journal_by_citations)
  )

  # ---- Sentence: Emerging Keywords (optional) ----
  s_keywords <- if (
    !is.null(focal_keyword_trends) &&
    nrow(focal_keyword_trends) > 0L &&
    all(c("publication_year", "keyword", "keyword_count") %in%
          names(focal_keyword_trends))
  ) {
    yr_max <- max(focal_keyword_trends$publication_year, na.rm = TRUE)

    recent_kw <- focal_keyword_trends |>
      dplyr::filter(.data$publication_year >= yr_max - 2L) |>
      dplyr::group_by(.data$keyword) |>
      dplyr::summarise(
        recent_count = sum(.data$keyword_count), .groups = "drop"
      )

    earlier_kw <- focal_keyword_trends |>
      dplyr::filter(
        .data$publication_year >= yr_max - 5L,
        .data$publication_year <  yr_max - 2L
      ) |>
      dplyr::group_by(.data$keyword) |>
      dplyr::summarise(
        earlier_count = sum(.data$keyword_count), .groups = "drop"
      )

    emerging_kw <- recent_kw |>
      dplyr::left_join(earlier_kw, by = "keyword") |>
      dplyr::mutate(
        earlier_count = dplyr::coalesce(.data$earlier_count, 0L),
        growth_ratio  = .data$recent_count /
          pmax(.data$earlier_count, 1L)
      ) |>
      dplyr::filter(.data$recent_count >= 3L) |>
      dplyr::arrange(dplyr::desc(.data$growth_ratio),
                     dplyr::desc(.data$recent_count)) |>
      dplyr::slice_head(n = 3L) |>
      dplyr::pull(.data$keyword)

    if (length(emerging_kw) >= 2L) {
      kw_formatted <- paste(
        paste0(
          "\"",
          stringr::str_to_lower(
            stringr::str_to_title(emerging_kw)
          ),
          "\""
        ),
        collapse = ", "
      )
      sprintf(
        paste(
          "Emerging research themes identified through keyword",
          "analysis included %s."
        ),
        kw_formatted
      )
    } else {
      NULL
    }
  } else {
    NULL
  }

  # ---- Sentence: Urology comparison ----
  s_urology <- if (nrow(urology_row) > 0L) {
    volume_ratio <- round(
      urology_row$total_documents / max(focal$total_documents, 1L), 0L
    )
    urology_cagr <- urology_row$cagr_pct

    faster_grower <- dplyr::case_when(
      is.na(focal_cagr) || is.na(urology_cagr) ~ "unknown",
      focal_cagr > urology_cagr                ~ focal_subspecialty,
      urology_cagr > focal_cagr                ~ "Urology",
      TRUE                                     ~ "both at equal rates"
    )

    growth_clause <- dplyr::case_when(
      faster_grower == focal_subspecialty ~ sprintf(
        "%s demonstrated faster growth (compound annual growth rate [CAGR] %.1f%% vs. %.1f%%)",
        focal_subspecialty, focal_cagr, urology_cagr
      ),
      faster_grower == "Urology" ~ sprintf(
        "Urology demonstrated faster growth (compound annual growth rate [CAGR] %.1f%% vs. %.1f%% for %s)",
        urology_cagr, focal_cagr, focal_subspecialty
      ),
      faster_grower == "both at equal rates" ~ sprintf(
        "both demonstrated similar growth rates (compound annual growth rate [CAGR] %.1f%%)",
        focal_cagr
      ),
      TRUE ~ "growth rates were unavailable for comparison"
    )

    sprintf(
      paste(
        "Urology produced approximately %dx the publication",
        "volume of %s (%s vs. %s documents); %s."
      ),
      volume_ratio,
      focal_subspecialty,
      fmt_n(urology_row$total_documents),
      fmt_n(focal$total_documents),
      growth_clause
    )
  } else {
    NULL
  }

  # ---- Sentence: Collaboration / team size (conditional) ----
  # Fires only when focal subspecialty is notably high or low vs. cohort.
  s_collaboration <- {
    focal_mean_au <- if ("mean_authors_per_paper" %in% names(focal))
      focal$mean_authors_per_paper else NA_real_

    cohort_mean_au_vals <- comparison_summary_table |>
      dplyr::filter(!is.na(.data[[
        if ("mean_authors_per_paper" %in% names(comparison_summary_table))
          "mean_authors_per_paper"
        else
          "total_documents"   # fallback column that always exists
      ]])) |>
      dplyr::pull(
        if ("mean_authors_per_paper" %in% names(comparison_summary_table))
          "mean_authors_per_paper"
        else
          "total_documents"
      )

    has_collab_data <- "mean_authors_per_paper" %in%
      names(comparison_summary_table) &&
      !is.na(focal_mean_au)

    if (!has_collab_data) {
      NULL
    } else {
      cohort_median_au <- median(
        comparison_summary_table$mean_authors_per_paper, na.rm = TRUE
      )
      focal_rank_collab <- if ("rank_by_collaboration" %in% names(focal))
        focal$rank_by_collaboration else NA_integer_
      n_cohort <- nrow(comparison_summary_table)

      collab_high <- focal_mean_au >= collab_high_threshold
      collab_low  <- focal_mean_au <= collab_low_threshold

      if (collab_high) {
        # Above threshold — distinctly consortium-driven
        sprintf(
          paste(
            "%s demonstrated a high degree of multi-investigator",
            "collaboration (mean %.1f authors per paper, ranking %s",
            "in the cohort), consistent with multicenter network",
            "research activity."
          ),
          focal_subspecialty,
          focal_mean_au,
          ordinal_label(focal_rank_collab)
        )
      } else if (collab_low) {
        # Below threshold — predominantly single-center / small-team
        sprintf(
          paste(
            "%s publications had a lower mean authorship count than",
            "most comparators (mean %.1f authors per paper, ranking",
            "%s in the cohort), reflecting a higher proportion of",
            "single-center or small-team studies."
          ),
          focal_subspecialty,
          focal_mean_au,
          ordinal_label(focal_rank_collab)
        )
      } else {
        # Within normal range — report descriptively with cohort context.
        # Always fires: suppression was removed because the focal subspecialty
        # (urogynecology) should always report its authorship pattern.
        rank_clause_collab <- if (!is.na(focal_rank_collab))
          sprintf(", ranking %s in the cohort",
                  ordinal_label(focal_rank_collab))
        else ""

        above_below <- if (!is.na(cohort_median_au)) {
          if (focal_mean_au > cohort_median_au)
            " above the cohort median"
          else if (focal_mean_au < cohort_median_au)
            " below the cohort median"
          else " at the cohort median"
        } else ""

        sprintf(
          paste(
            "%s publications had a mean of %.1f authors per paper%s%s,",
            "reflecting a mix of single-center and collaborative studies."
          ),
          focal_subspecialty,
          focal_mean_au,
          rank_clause_collab,
          above_below
        )
      }
    }
  }

  if (!is.null(focal_disruption_index)) {
    assertthat::assert_that(
      is.data.frame(focal_disruption_index),
      msg = "`focal_disruption_index` must be a data frame from .compute_disruption_index() or NULL."
    )
    assertthat::assert_that(
      all(c("median_di","pct_disruptive","pct_consolidating",
            "n_papers_with_di") %in% names(focal_disruption_index)),
      msg = paste(
        "`focal_disruption_index` is missing expected columns.",
        "Pass the output of .compute_disruption_index() directly."
      )
    )
  }
  if (!is.null(focal_equity_metrics)) {
    assertthat::assert_that(
      is.data.frame(focal_equity_metrics),
      msg = "`focal_equity_metrics` must be a data frame from .compute_equity_metrics() or NULL."
    )
    assertthat::assert_that(
      all(c("pct_high_income","pct_lower_mid","pct_low_income",
            "concentration_hhi") %in% names(focal_equity_metrics)),
      msg = paste(
        "`focal_equity_metrics` is missing expected columns.",
        "Pass the output of .compute_equity_metrics() directly."
      )
    )
  }
  if (!is.null(focal_evidence_quality)) {
    assertthat::assert_that(
      is.data.frame(focal_evidence_quality),
      msg = "`focal_evidence_quality` must be a data frame from .compute_evidence_quality_score() or NULL."
    )
    assertthat::assert_that(
      all(c("evidence_quality_score","pct_high_evidence") %in%
            names(focal_evidence_quality)),
      msg = paste(
        "`focal_evidence_quality` is missing expected columns.",
        "Pass the output of .compute_evidence_quality_score() directly."
      )
    )
  }
  assertthat::assert_that(
    assertthat::is.number(di_pct_disruptive_threshold) &&
      di_pct_disruptive_threshold >= 0 && di_pct_disruptive_threshold <= 100,
    msg = "`di_pct_disruptive_threshold` must be in [0, 100]."
  )
  assertthat::assert_that(
    assertthat::is.number(di_pct_consolidating_threshold) &&
      di_pct_consolidating_threshold >= 0 &&
      di_pct_consolidating_threshold <= 100,
    msg = "`di_pct_consolidating_threshold` must be in [0, 100]."
  )
  assertthat::assert_that(
    assertthat::is.number(di_direction_threshold) &&
      di_direction_threshold >= 0 && di_direction_threshold <= 100,
    msg = "`di_direction_threshold` must be in [0, 100]."
  )

  # ---- Sentence: Disruption Index (conditional) ----
  # Fires when:
  #   focal median_di is available AND
  #   the focal subspecialty ranks notably high or low vs the cohort
  #   (rank 1 = most disruptive; rank = last = most consolidating),
  #   OR when pct_disruptive is remarkably extreme (>95% or <20%).
  # Suppressed when DI is unavailable or ranks in the middle third.
  s_disruption <- if (is.null(focal_disruption_index)) {
    NULL
  } else {
    di_median   <- focal_disruption_index$median_di[[1L]]
    pct_dis     <- focal_disruption_index$pct_disruptive[[1L]]
    pct_con     <- focal_disruption_index$pct_consolidating[[1L]]
    n_di        <- focal_disruption_index$n_papers_with_di[[1L]]

    # Also use comparison-table columns if available
    focal_di_rank <- if ("rank_by_disruption" %in% names(focal))
      focal$rank_by_disruption else NA_integer_
    n_cohort <- nrow(comparison_summary_table)
    top_third <- ceiling(n_cohort / 3L)
    bot_third <- n_cohort - top_third + 1L

    is_top_disruptor  <- !is.na(focal_di_rank) && focal_di_rank <= top_third
    is_consolidating  <- !is.na(focal_di_rank) && focal_di_rank >= bot_third
    is_extreme_pct    <- !is.na(pct_dis) &&
      (pct_dis >= di_pct_disruptive_threshold ||
       (!is.na(pct_con) && pct_con >= di_pct_consolidating_threshold))
    di_available      <- !is.na(di_median) && n_di >= 10L

    if (!di_available || (!is_top_disruptor && !is_consolidating &&
                          !is_extreme_pct)) {
      NULL
    } else {
      direction_word <- if (!is.na(pct_dis) &&
                            pct_dis >= di_direction_threshold) {
        "predominantly disruptive"
      } else if (!is.na(pct_con) &&
                 pct_con >= di_pct_consolidating_threshold) {
        "predominantly consolidating"
      } else {
        "mixed disruptive-consolidating"
      }

      rank_clause <- if (!is.na(focal_di_rank) && n_cohort > 1L) {
        sprintf(
          ", ranking %s for disruption among the cohort",
          ordinal_label(focal_di_rank)
        )
      } else ""

      sprintf(
        paste(
          "Disruption Index (DI) analysis showed %s %s contributions",
          "(median DI=%.3f; %.1f%% of papers with DI > 0)%s;",
          "local DI was computed from within-corpus citation patterns."
        ),
        focal_subspecialty,
        direction_word,
        dplyr::coalesce(di_median, 0),
        dplyr::coalesce(pct_dis,   0),
        rank_clause
      )
    }
  }

  # ---- Sentence: Geographic equity / LMIC (conditional) ----
  # Fires when equity data is provided AND the corpus shows notable
  # concentration (high HHI) or notable LMIC under-representation
  # (pct_low_income + pct_lower_mid < 15%).
  s_equity <- if (is.null(focal_equity_metrics)) {
    NULL
  } else {
    pct_high   <- focal_equity_metrics$pct_high_income[[1L]]
    pct_up_mid <- if ("pct_upper_mid" %in% names(focal_equity_metrics))
      focal_equity_metrics$pct_upper_mid[[1L]] else NA_real_
    pct_lo_mid <- focal_equity_metrics$pct_lower_mid[[1L]]
    pct_low    <- focal_equity_metrics$pct_low_income[[1L]]
    hhi        <- focal_equity_metrics$concentration_hhi[[1L]]
    pct_lmic   <- dplyr::coalesce(pct_lo_mid, 0) +
      dplyr::coalesce(pct_low, 0)

    is_concentrated  <- !is.na(hhi)   && hhi > equity_hhi_threshold
    is_lmic_low      <- !is.na(pct_lmic) && pct_lmic < equity_lmic_threshold
    is_hic_dominant  <- !is.na(pct_high) && pct_high > equity_hic_threshold

    if (!is_concentrated && !is_lmic_low && !is_hic_dominant) {
      NULL
    } else {
      hhi_clause <- if (!is.na(hhi)) {
        sprintf("(Herfindahl-Hirschman Index [HHI]=%.3f)", hhi)
      } else ""

      lmic_clause <- if (!is.na(pct_lmic)) {
        sprintf(
          "only %.1f%% of publications originated from lower- or low-income countries",
          pct_lmic
        )
      } else NULL

      hic_clause <- if (!is.na(pct_high)) {
        sprintf("%.1f%% from high-income countries", pct_high)
      } else NULL

      distribution_detail <- paste(
        c(hic_clause, lmic_clause), collapse = "; "
      )

      sprintf(
        paste(
          "Geographic authorship was heavily concentrated %s,",
          "with %s, suggesting persistent under-representation",
          "of low- and middle-income country researchers."
        ),
        hhi_clause,
        distribution_detail
      )
    }
  }

  # ---- Sentence: Evidence quality (conditional) ----
  # Fires when evidence quality data is provided AND the focal
  # subspecialty has a notably high or low evidence score relative
  # to the comparison table's evidence_quality_score column.
  s_evidence_quality <- if (is.null(focal_evidence_quality)) {
    NULL
  } else {
    eq_score    <- focal_evidence_quality$evidence_quality_score[[1L]]
    pct_high_ev <- focal_evidence_quality$pct_high_evidence[[1L]]

    # Always fire when data is present — no rank threshold
    if (is.na(eq_score)) {
      NULL
    } else {
      # Rank clause
      focal_eq_rank <- if ("rank_by_evidence" %in% names(focal))
        focal$rank_by_evidence else NA_integer_

      rank_clause_ev <- if (!is.na(focal_eq_rank)) {
        sprintf(", ranking %s among the cohort",
                ordinal_label(focal_eq_rank))
      } else ""

      # Cohort comparison: highest and lowest pct_high_evidence
      ev_col <- "pct_high_evidence"
      cohort_high_ev <- if (ev_col %in% names(comparison_summary_table)) {
        comparison_summary_table |>
          dplyr::filter(!is.na(.data[[ev_col]]),
                        .data$subspecialty != focal_subspecialty) |>
          dplyr::arrange(dplyr::desc(.data[[ev_col]])) |>
          dplyr::slice_head(n = 1L)
      } else NULL

      cohort_low_ev <- if (ev_col %in% names(comparison_summary_table)) {
        comparison_summary_table |>
          dplyr::filter(!is.na(.data[[ev_col]]),
                        .data$subspecialty != focal_subspecialty) |>
          dplyr::arrange(.data[[ev_col]]) |>
          dplyr::slice_head(n = 1L)
      } else NULL

      # Cohort context clause: focal vs highest and lowest comparators
      cohort_context <- if (
        !is.null(cohort_high_ev) && nrow(cohort_high_ev) > 0L &&
        !is.null(cohort_low_ev)  && nrow(cohort_low_ev)  > 0L &&
        !is.na(pct_high_ev)
      ) {
        sprintf(
          " — below %s (%.1f%%) but above %s (%.1f%%)",
          cohort_high_ev$subspecialty[[1L]],
          cohort_high_ev[[ev_col]][[1L]],
          cohort_low_ev$subspecialty[[1L]],
          cohort_low_ev[[ev_col]][[1L]]
        )
      } else ""

      # ---- p-value: inline prop.test from comparison table ----
      # Compute pairwise two-proportion chi-square (continuity-corrected)
      # directly from pct_high_evidence + total_documents in the comparison
      # table.  This fires even when statistics_results is NULL so the
      # abstract always includes significance statements.
      # If statistics_results already contains pre-computed evidence_quality
      # p-values (with BH correction), those take precedence.
      ev_p_clause <- {
        # ---- Path A: pre-computed results (BH-corrected) ----
        if (
          !is.null(statistics_results) &&
          !is.null(statistics_results$evidence_quality) &&
          nrow(statistics_results$evidence_quality) > 0L
        ) {
          ev_tbl  <- statistics_results$evidence_quality
          n_sig   <- sum(ev_tbl$significant, na.rm = TRUE)
          n_tot   <- nrow(ev_tbl)

          top_comp_row <- ev_tbl |>
            dplyr::filter(!is.na(.data$comparator_pct_high)) |>
            dplyr::arrange(dplyr::desc(.data$comparator_pct_high)) |>
            dplyr::slice_head(n = 1L)

          if (nrow(top_comp_row) > 0L && !is.na(top_comp_row$p_value)) {
            sprintf(
              " (P%s vs. %s; %d of %d pairwise comparisons significant, Benjamini-Hochberg [BH]-corrected)",
              top_comp_row$p_fmt[[1L]],
              top_comp_row$subspecialty[[1L]],
              n_sig, n_tot
            )
          } else ""

        # ---- Path B: inline computation from comparison table ----
        } else if (
          !is.na(pct_high_ev) &&
          !is.na(focal$total_documents) &&
          "pct_high_evidence" %in% names(comparison_summary_table) &&
          "total_documents"   %in% names(comparison_summary_table)
        ) {
          focal_hi_n <- round(pct_high_ev * focal$total_documents / 100)

          # Run prop.test vs each comparator; apply BH correction
          comparators <- comparison_summary_table |>
            dplyr::filter(
              .data$subspecialty != focal_subspecialty,
              !is.na(.data$pct_high_evidence),
              !is.na(.data$total_documents)
            )

          if (nrow(comparators) == 0L || focal_hi_n <= 0L) {
            ""
          } else {
            raw_p <- vapply(seq_len(nrow(comparators)), function(i) {
              comp_hi_n <- round(
                comparators$pct_high_evidence[[i]] *
                  comparators$total_documents[[i]] / 100
              )
              if (is.na(comp_hi_n) || comp_hi_n <= 0L) return(NA_real_)
              pt <- tryCatch(
                stats::prop.test(
                  x = c(focal_hi_n, comp_hi_n),
                  n = c(focal$total_documents,
                        comparators$total_documents[[i]]),
                  correct = TRUE
                ),
                error = function(e) NULL
              )
              if (is.null(pt)) NA_real_ else pt$p.value
            }, numeric(1L))

            # BH correction
            valid_idx <- which(!is.na(raw_p))
            adj_p     <- rep(NA_real_, length(raw_p))
            if (length(valid_idx) > 0L) {
              adj_p[valid_idx] <- stats::p.adjust(
                raw_p[valid_idx],
                method = if (!is.null(statistics_results$correction_method))
                  statistics_results$correction_method else "BH"
              )
            }

            n_sig_inline <- sum(adj_p < statistics_alpha, na.rm = TRUE)
            n_tot_inline <- sum(!is.na(raw_p))

            # Report p-value vs the highest-evidence comparator
            top_idx <- which.max(comparators$pct_high_evidence)
            top_p   <- adj_p[top_idx]
            top_nm  <- comparators$subspecialty[[top_idx]]

            if (!is.na(top_p)) {
              top_p_fmt <- dplyr::case_when(
                top_p < 0.001 ~ "P<.001",
                top_p < 0.10  ~ sub("^0\\.", ".", sprintf("P=%.3f", top_p)),
                TRUE          ~ sub("^0\\.", ".", sprintf("P=%.2f", top_p))
              )
              sprintf(
                " (%s vs. %s; %d of %d pairwise comparisons significant, Benjamini-Hochberg [BH]-corrected)",
                top_p_fmt, top_nm, n_sig_inline, n_tot_inline
              )
            } else ""
          }
        } else ""
      }

      # High-evidence breakdown clause
      high_ev_clause <- if (!is.na(pct_high_ev)) {
        sprintf(
          "; %.1f%% of publications were randomized controlled trials (RCTs), meta-analyses, or multicenter studies%s",
          pct_high_ev, cohort_context
        )
      } else ""

      sprintf(
        paste(
          "Evidence quality analysis showed a weighted pyramid score of",
          "%.2f / 5.0 for %s%s%s%s."
        ),
        eq_score,
        focal_subspecialty,
        rank_clause_ev,
        high_ev_clause,
        ev_p_clause
      )
    }
  }

  # ---- Sentence: Conclusion ----
  # Four-clause structure, each data-driven and actionable:
  #   1. Growth + volume rank -> workforce/demand signal
  #   2. Evidence quality rank + pct RCT -> trial gap and priority
  #   3. Geographic equity -> international partnership call
  #   4. Closing call-to-action for fellowship/grant planning
  s_conclusion <- {

    # Clause 1: growth + volume rank as workforce signal
    growth_clause <- if (!is.na(focal_cagr)) {
      vol_rank_word <- if ("rank_by_volume" %in% names(focal) &&
                           !is.na(focal$rank_by_volume))
        ordinal_label(focal$rank_by_volume) else NULL
      if (!is.null(vol_rank_word)) {
        sprintf(
          paste(
            "%s produced the %s largest publication corpus in the cohort",
            "with a %.1f%% compound annual growth rate (CAGR), consistent",
            "with expanding clinical demand and documented workforce shortages"
          ),
          focal_subspecialty, vol_rank_word, focal_cagr
        )
      } else {
        sprintf(
          paste(
            "%s demonstrated a %.1f%% compound annual growth rate (CAGR),",
            "consistent with expanding clinical demand"
          ),
          focal_subspecialty, focal_cagr
        )
      }
    } else {
      sprintf("%s publication output reflects expanding clinical activity",
              focal_subspecialty)
    }

    # Clause 2: evidence quality as actionable gap
    ev_clause <- if (
      !is.null(focal_evidence_quality) &&
      !is.na(focal_evidence_quality$evidence_quality_score[[1L]])
    ) {
      eq  <- focal_evidence_quality$evidence_quality_score[[1L]]
      ph  <- focal_evidence_quality$pct_high_evidence[[1L]]
      eq_rank <- if ("rank_by_evidence" %in% names(focal) &&
                     !is.na(focal$rank_by_evidence))
        ordinal_label(focal$rank_by_evidence) else NULL
      n_sp <- nrow(comparison_summary_table)
      if (!is.null(eq_rank)) {
        sprintf(
          paste(
            "evidence quality ranked %s among %d subspecialties",
            "(%.1f%% randomized controlled trials [RCTs] or",
            "meta-analyses), indicating that multicentre trial",
            "infrastructure is the highest-yield investment to",
            "advance the evidence base"
          ),
          eq_rank, n_sp, ph
        )
      } else {
        sprintf(
          paste(
            "%.1f%% of publications were high-evidence designs;",
            "multicentre trial investment is a priority"
          ),
          ph
        )
      }
    } else NULL

    # Clause 3: geographic equity as structural gap
    geo_clause <- if (
      !is.null(focal_equity_metrics) &&
      !is.na(focal_equity_metrics$pct_high_income[[1L]])
    ) {
      pct_hic  <- focal_equity_metrics$pct_high_income[[1L]]
      pct_lmic <- dplyr::coalesce(focal_equity_metrics$pct_lower_mid[[1L]], 0) +
                  dplyr::coalesce(focal_equity_metrics$pct_low_income[[1L]],  0)
      if (pct_hic > equity_hic_threshold || pct_lmic < equity_lmic_threshold) {
        sprintf(
          paste(
            "%.1f%% of authorship originated from high-income countries",
            "and only %.1f%% from low- and middle-income countries,",
            "underscoring the need for targeted international fellowships",
            "and research partnerships to broaden the global %s",
            "evidence base"
          ),
          pct_hic, pct_lmic, focal_subspecialty
        )
      } else NULL
    } else NULL

    # Assemble with actionable closing sentence
    clauses <- Filter(Negate(is.null), list(growth_clause, ev_clause, geo_clause))

    closing <- sprintf(
      paste(
        "These findings provide a quantitative foundation for %s",
        "fellowship programme planning, grant prioritisation, and",
        "evidence-based advocacy for subspecialty resources."
      ),
      focal_subspecialty
    )

    if (length(clauses) >= 2L) {
      body <- paste(clauses, collapse = "; ")
      paste0(body, ". ", closing)
    } else if (length(clauses) == 1L) {
      paste0(clauses[[1L]], ". ", closing)
    } else {
      sprintf(
        paste(
          "This bibliometric profile of %s literature from %d to %d",
          "provides a quantitative foundation for fellowship planning,",
          "grant prioritisation, and evidence-based advocacy."
        ),
        focal_subspecialty, year_start, year_end
      )
    }
  }


  # ---- Assemble ----
  abstract_sentences <- list(
    title            = s_title,
    introduction     = s_introduction,
    methods          = s_methods,
    corpus           = s_corpus,
    citations        = s_citations,
    growth           = s_growth,
    mk_trend         = s_mk_trend,
    geography        = s_geography,
    institution      = s_institution,
    output           = s_output,
    keywords         = s_keywords,
    disruption       = s_disruption,
    evidence_quality = s_evidence_quality,
    equity           = s_equity,
    collaboration    = s_collaboration,
    urology          = s_urology,
    conclusion       = s_conclusion
  )

  # ---- Formatted abstract (section headings) ----
  # Groups sentences under INTRODUCTION / METHODS / RESULTS / CONCLUSION
  # for direct copy-paste into a manuscript.  NULL sentences are silently
  # omitted; each section is separated by a blank line.
  results_keys <- c("corpus","citations","growth","mk_trend","geography",
                    "institution","output","keywords","disruption",
                    "evidence_quality","equity","collaboration","urology")

  .join_sentences <- function(keys, lst) {
    sents <- Filter(Negate(is.null), lst[keys])
    if (length(sents) == 0L) return("")
    paste(unlist(sents), collapse=" ")
  }

  formatted_abstract <- paste(
    "TITLE",
    s_title,
    "",
    "INTRODUCTION",
    s_introduction,
    "",
    "METHODS",
    s_methods,
    "",
    "RESULTS",
    .join_sentences(results_keys, abstract_sentences),
    "",
    "CONCLUSION",
    s_conclusion,
    sep = "
"
  )
  # Post-process: expand all abbreviations in formatted text
  formatted_abstract <- .expand_abstract_abbrevs(
    formatted_abstract,
    subspecialty_labels = subspecialty_labels
  )
  abstract_sentences$formatted <- formatted_abstract

  if (isTRUE(verbose)) {
    message("\n=== ABSTRACT RESULTS TEXT ===")
    purrr::walk2(
      names(abstract_sentences),
      abstract_sentences,
      function(label, sentence) {
        if (!is.null(sentence)) {
          message(sprintf("\n[%s]\n%s", toupper(label), sentence))
        }
      }
    )
    message("=============================\n")
  }

  # Expand abbreviations in every individual sentence too
  abstract_sentences <- lapply(abstract_sentences, function(s) {
    if (is.character(s) && length(s) == 1L && !is.na(s))
      .expand_abstract_abbrevs(s, subspecialty_labels)
    else s
  })
  return(abstract_sentences)
}

#' @noRd
ordinal_label <- function(n) {
  assertthat::assert_that(
    is.numeric(n) && length(n) == 1L,
    msg = "`n` must be a single numeric value."
  )
  assertthat::assert_that(
    !is.na(n),
    msg = "`n` must not be NA in ordinal_label()."
  )
  n <- as.integer(n)
  suffix <- dplyr::case_when(
    n %% 100 %in% 11:13 ~ "th",
    n %% 10 == 1        ~ "st",
    n %% 10 == 2        ~ "nd",
    n %% 10 == 3        ~ "rd",
    TRUE                ~ "th"
  )
  paste0(n, suffix)
}


# ============================================================
# STATISTICAL TESTING MODULE
# ============================================================

#' Compute Comparative Statistics Across Subspecialties
#'
#' @description
#' Performs five statistical tests on the subspecialty comparison data
#' and returns structured results suitable for direct insertion into
#' manuscript Results text. All tests are pre-specified and non-redundant:
#'
#' \enumerate{
#'   \item \strong{Kruskal-Wallis H-test}: Are per-paper citation counts
#'     heterogeneous across all subspecialties? (Overall test, 1 p-value.)
#'   \item \strong{Pairwise Wilcoxon rank-sum + multiple comparison
#'     correction}: Does the focal subspecialty differ from each comparator
#'     in per-paper citations? Reports raw and adjusted p-values and
#'     rank-biserial correlation effect size (r).
#'   \item \strong{Poisson regression on annual counts}: Is the growth
#'     trend within each subspecialty statistically significant? Reports
#'     incidence rate ratio (IRR) and p-value per subspecialty.
#'   \item \strong{Growth slope heterogeneity F-test}: Do annual growth
#'     slopes differ significantly across subspecialties?
#'   \item \strong{Chi-square proportion test}: Is the focal subspecialty's
#'     top-country concentration proportion significantly different from
#'     each comparator's?
#' }
#'
#' @param subspecialty_results_list Named list. Same structure as passed
#'   to \code{run_subspecialty_comparison()}.
#' @param focal_subspecialty Character. Focal subspecialty name. Defaults
#'   to \code{"FPMRS"}.
#' @param alpha Numeric. Significance threshold. Defaults to \code{0.05}.
#' @param correction_method Character. Method passed to
#'   \code{p.adjust()}. One of \code{"BH"} (Benjamini-Hochberg),
#'   \code{"bonferroni"}, \code{"holm"}, etc. Defaults to \code{"BH"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A named list with elements:
#' \describe{
#'   \item{\code{kruskal_wallis}}{List: H statistic, df, p-value, and
#'     formatted string.}
#'   \item{\code{pairwise_citations}}{Tibble: one row per comparator with
#'     W statistic, raw p, adjusted p, effect size r, and significance
#'     flag.}
#'   \item{\code{growth_trends}}{Tibble: one row per subspecialty with
#'     Poisson IRR, p-value, and 95\% CI.}
#'   \item{\code{growth_heterogeneity}}{List: F statistic, df, p-value.}
#'   \item{\code{country_proportions}}{Tibble: focal vs each comparator
#'     chi-square and p-value for top-country concentration.}
#'   \item{\code{summary_sentences}}{Named list of ready-to-paste
#'     sentences incorporating p-values.}
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag is.number
#' @importFrom dplyr tibble filter pull mutate bind_rows arrange desc
#'   case_when
#' @importFrom purrr map_dfr map_dbl map_chr imap_dfr
#' @importFrom stats kruskal.test wilcox.test p.adjust glm poisson
#'   prop.test lm anova qnorm
#' @export
compute_comparison_statistics <- function(
    subspecialty_results_list,
    focal_subspecialty = "FPMRS",
    alpha              = 0.05,
    correction_method  = "BH",
    verbose            = TRUE
) {
  # ---- Validate ----
  assertthat::assert_that(is.list(subspecialty_results_list))
  assertthat::assert_that(
    length(subspecialty_results_list) >= 2L,
    msg = "Need at least 2 subspecialties to compute statistics."
  )
  assertthat::assert_that(assertthat::is.string(focal_subspecialty))
  assertthat::assert_that(assertthat::is.number(alpha))
  assertthat::assert_that(alpha > 0 && alpha < 1,
    msg = "`alpha` must be between 0 and 1.")
  assertthat::assert_that(assertthat::is.string(correction_method))
  assertthat::assert_that(
    correction_method %in% c("BH","bonferroni","holm","hochberg",
                              "hommel","BY","fdr","none"),
    msg = paste("`correction_method` must be a valid p.adjust method.",
                "Received:", correction_method)
  )
  assertthat::assert_that(
    isTRUE(verbose) || isFALSE(verbose),
    msg = "`verbose` must be TRUE or FALSE."
  )

  # Verify focal subspecialty exists
  sp_names <- purrr::map_chr(subspecialty_results_list, "subspecialty")
  assertthat::assert_that(
    focal_subspecialty %in% sp_names,
    msg = sprintf(
      "'%s' not found in subspecialty_results_list. Available: %s",
      focal_subspecialty, paste(sp_names, collapse = ", ")
    )
  )

  .log_step("\n--- STATISTICS: Building long-format citation table ---",
            verbose)

  # ---- Build long-format citation table ----
  citation_long_table <- purrr::map_dfr(
    subspecialty_results_list,
    function(sp_result) {
      tc_values <- suppressWarnings(
        as.numeric(sp_result$bibliography$TC)
      )
      dplyr::tibble(
        subspecialty   = sp_result$subspecialty,
        citation_count = tc_values
      )
    }
  ) |>
    dplyr::filter(!is.na(.data$citation_count))

  assertthat::assert_that(
    nrow(citation_long_table) > 0,
    msg = "No valid citation counts found. Check TC column in bibliography."
  )

  # ---- 1. Kruskal-Wallis overall ----
  .log_step("[STATS] Running Kruskal-Wallis H-test ...", verbose)
  kw_result <- kruskal.test(
    citation_count ~ subspecialty,
    data = citation_long_table
  )
  kw_list <- list(
    H          = round(kw_result$statistic, 2L),
    df         = kw_result$parameter,
    p_value    = kw_result$p.value,
    p_fmt      = .fmt_pvalue(kw_result$p.value),
    significant = kw_result$p.value < alpha
  )
  .log_step(sprintf(
    "[STATS] Kruskal-Wallis: H=%.2f df=%d p=%s",
    kw_list$H, kw_list$df, kw_list$p_fmt
  ), verbose)

  # ---- 2. Pairwise Wilcoxon: focal vs each comparator ----
  .log_step("[STATS] Running pairwise Wilcoxon rank-sum tests ...", verbose)
  focal_citations <- citation_long_table$citation_count[
    citation_long_table$subspecialty == focal_subspecialty
  ]
  comparator_names <- setdiff(sp_names, focal_subspecialty)

  pairwise_raw <- purrr::map_dfr(comparator_names, function(comp_name) {
    comp_tc <- citation_long_table$citation_count[
      citation_long_table$subspecialty == comp_name
    ]
    wt <- wilcox.test(focal_citations, comp_tc, exact = FALSE)

    # Rank-biserial correlation effect size: r = Z / sqrt(N)
    # Capped at [0, 1] -- extremely small p-values hit machine precision
    # and produce Inf from qnorm(); cap prevents nonsense in output.
    n_focal <- length(focal_citations)
    n_comp  <- length(comp_tc)
    z_score <- if (wt$p.value > 0 && is.finite(wt$p.value)) {
      qnorm(wt$p.value / 2) * sign(mean(focal_citations) - mean(comp_tc))
    } else {
      # p underflows to 0: maximum effect, use z=8.2 (p~2e-16)
      8.2 * sign(mean(focal_citations) - mean(comp_tc))
    }
    r_effect <- min(abs(z_score / sqrt(n_focal + n_comp)), 1.0)

    dplyr::tibble(
      subspecialty    = comp_name,
      W_statistic     = wt$statistic,
      p_raw           = wt$p.value,
      effect_r        = round(r_effect, 3L),
      effect_label    = dplyr::case_when(
        r_effect < 0.1 ~ "negligible",
        r_effect < 0.3 ~ "small",
        r_effect < 0.5 ~ "medium",
        TRUE           ~ "large"
      ),
      focal_median    = median(focal_citations),
      comparator_median = median(comp_tc)
    )
  })

  # Apply multiple comparison correction
  pairwise_citations_table <- pairwise_raw |>
    dplyr::mutate(
      p_adjusted  = p.adjust(.data$p_raw, method = correction_method),
      p_raw_fmt   = purrr::map_chr(.data$p_raw, .fmt_pvalue),
      p_adj_fmt   = purrr::map_chr(.data$p_adjusted, .fmt_pvalue),
      significant = .data$p_adjusted < alpha
    )

  n_significant <- sum(pairwise_citations_table$significant)
  .log_step(sprintf(
    "[STATS] Pairwise Wilcoxon (%s correction): %d/%d comparisons p<%g",
    correction_method, n_significant, nrow(pairwise_citations_table), alpha
  ), verbose)

  # ---- 3. Poisson growth trend per subspecialty ----
  .log_step("[STATS] Fitting Poisson growth models ...", verbose)
  growth_trends_table <- purrr::map_dfr(
    subspecialty_results_list,
    function(sp_result) {
      ann <- sp_result$annual_trends
      if (nrow(ann) < 5L) {
        .log_step(sprintf(
          "[STATS]   Skipping %s: fewer than 5 year-rows",
          sp_result$subspecialty
        ), verbose)
        return(dplyr::tibble(
          subspecialty = sp_result$subspecialty,
          irr          = NA_real_,
          irr_lo95     = NA_real_,
          irr_hi95     = NA_real_,
          p_value      = NA_real_,
          p_fmt        = "N/A",
          significant  = FALSE
        ))
      }

      # Annual publication counts are overdispersed (variance >> mean
      # over multi-decade windows). Negative binomial GLM (glm.nb) adds
      # a dispersion parameter theta, giving more honest standard errors
      # and wider confidence intervals than standard Poisson.
      # Falls back to Poisson GLM if MASS is unavailable or glm.nb fails
      # (e.g. insufficient data to estimate theta).
      fit <- tryCatch({
        MASS::glm.nb(publication_count ~ publication_year, data = ann)
      }, error = function(e_nb) {
        .log_step(sprintf(
          "[STATS] glm.nb failed for %s (%s), falling back to Poisson",
          sp_result$subspecialty, e_nb$message
        ), verbose)
        tryCatch(
          glm(publication_count ~ publication_year,
              data = ann, family = poisson()),
          error = function(e) NULL
        )
      })

      if (is.null(fit)) {
        return(dplyr::tibble(
          subspecialty = sp_result$subspecialty,
          irr = NA_real_, irr_lo95 = NA_real_, irr_hi95 = NA_real_,
          p_value = NA_real_, p_fmt = "N/A", significant = FALSE,
          dispersion_theta = NA_real_,
          model_family     = NA_character_
        ))
      }

      family_used <- if (inherits(fit, "negbin")) "negative_binomial" else "poisson"
      coefs     <- summary(fit)$coefficients
      ci        <- suppressMessages(confint(fit))
      beta      <- coefs["publication_year", "Estimate"]
      p_val     <- coefs["publication_year", "Pr(>|z|)"]
      irr_val   <- exp(beta)
      irr_lo    <- exp(ci["publication_year", "2.5 %"])
      irr_hi    <- exp(ci["publication_year", "97.5 %"])

      theta_val <- if (inherits(fit, "negbin")) fit$theta else NA_real_
      dplyr::tibble(
        subspecialty     = sp_result$subspecialty,
        irr              = round(irr_val, 4L),
        irr_lo95         = round(irr_lo, 4L),
        irr_hi95         = round(irr_hi, 4L),
        p_value          = p_val,
        dispersion_theta = round(theta_val, 3L),
        model_family     = family_used,
        p_fmt        = .fmt_pvalue(p_val),
        significant  = p_val < alpha
      )
    }
  )

  # Apply BH correction across all k Poisson growth tests.
  # Without correction, testing k=8 subspecialties at alpha=0.05
  # gives an expected 0.4 false positives from noise alone.
  valid_growth_idx <- which(!is.na(growth_trends_table$p_value))
  if (length(valid_growth_idx) > 0L) {
    p_adj_growth <- stats::p.adjust(
      growth_trends_table$p_value[valid_growth_idx],
      method = correction_method
    )
    growth_trends_table$p_value[valid_growth_idx]   <- p_adj_growth
    growth_trends_table$p_fmt[valid_growth_idx]     <-
      purrr::map_chr(p_adj_growth, .fmt_pvalue)
    growth_trends_table$significant[valid_growth_idx] <-
      p_adj_growth < alpha
    .log_step(sprintf(
      "[STATS] Growth trends (%s-corrected): %d/%d subspecialties significant",
      correction_method,
      sum(growth_trends_table$significant, na.rm = TRUE),
      nrow(growth_trends_table)
    ), verbose)
  } else {
    .log_step("[STATS] Growth trends: no valid p-values to correct.", verbose)
  }

  # ---- 4. Growth slope heterogeneity F-test ----
  .log_step("[STATS] Testing growth slope heterogeneity ...", verbose)
  overlaid_trends <- purrr::map_dfr(
    subspecialty_results_list,
    function(sp_result) {
      sp_result$annual_trends |>
        dplyr::mutate(subspecialty = sp_result$subspecialty)
    }
  ) |>
    dplyr::mutate(
      log_count = log(.data$publication_count + 1L)
    )

  # F-test on log-transformed counts. The log transformation
  # stabilises variance across subspecialties of very different sizes,
  # making the normality assumption more defensible than on raw counts.
  # The null model forces equal slopes; the full model allows each
  # subspecialty to have its own slope.
  hetero_null <- lm(
    log_count ~ publication_year + subspecialty,
    data = overlaid_trends
  )
  hetero_full <- lm(
    log_count ~ publication_year * subspecialty,
    data = overlaid_trends
  )
  hetero_anova  <- anova(hetero_null, hetero_full)
  hetero_f      <- hetero_anova$F[[2L]]
  hetero_df_num <- hetero_anova$Df[[2L]]
  hetero_df_den <- hetero_anova$Res.Df[[2L]]
  hetero_p      <- hetero_anova[["Pr(>F)"]][[2L]]

  growth_heterogeneity_list <- list(
    F_statistic = round(hetero_f, 3L),
    df_numerator   = hetero_df_num,
    df_denominator = hetero_df_den,
    p_value        = hetero_p,
    p_fmt          = .fmt_pvalue(hetero_p),
    significant    = hetero_p < alpha
  )
  .log_step(sprintf(
    "[STATS] Growth heterogeneity: F(%d,%d)=%.3f p=%s",
    hetero_df_num, hetero_df_den,
    hetero_f, growth_heterogeneity_list$p_fmt
  ), verbose)

  # ---- 5. Country proportion tests ----
  .log_step("[STATS] Running country concentration proportion tests ...",
            verbose)
  comp_tbl <- .build_comparison_summary_table(
    subspecialty_results_list, verbose = FALSE
  )
  focal_row   <- dplyr::filter(comp_tbl,
    .data$subspecialty == focal_subspecialty)
  focal_us_n  <- round(
    focal_row$top_country_pct * focal_row$total_documents / 100
  )

  country_prop_table <- purrr::map_dfr(
    setdiff(comp_tbl$subspecialty, focal_subspecialty),
    function(comp_name) {
      other_row  <- dplyr::filter(comp_tbl,
        .data$subspecialty == comp_name)
      other_us_n <- round(
        other_row$top_country_pct * other_row$total_documents / 100
      )

      if (is.na(other_us_n) || other_us_n <= 0 ||
          is.na(focal_us_n) || focal_us_n <= 0) {
        return(dplyr::tibble(
          subspecialty       = comp_name,
          focal_pct          = focal_row$top_country_pct,
          comparator_pct     = other_row$top_country_pct,
          chi2               = NA_real_,
          p_value            = NA_real_,
          p_fmt              = "N/A",
          significant        = FALSE
        ))
      }

      pt <- tryCatch(
        prop.test(
          x = c(focal_us_n, other_us_n),
          n = c(focal_row$total_documents, other_row$total_documents),
          correct = TRUE
        ),
        error = function(e) NULL
      )

      if (is.null(pt)) {
        return(dplyr::tibble(
          subspecialty = comp_name, focal_pct = focal_row$top_country_pct,
          comparator_pct = other_row$top_country_pct,
          chi2 = NA_real_, p_value = NA_real_,
          p_fmt = "N/A", significant = FALSE
        ))
      }

      dplyr::tibble(
        subspecialty   = comp_name,
        focal_pct      = focal_row$top_country_pct,
        comparator_pct = other_row$top_country_pct,
        chi2           = round(pt$statistic, 3L),
        p_value        = pt$p.value,
        p_fmt          = .fmt_pvalue(pt$p.value),
        significant    = pt$p.value < alpha
      )
    }
  )
  # Apply BH correction across all k−1 country proportion tests.
  valid_country_idx <- which(!is.na(country_prop_table$p_value))
  if (length(valid_country_idx) > 0L) {
    p_adj_country <- stats::p.adjust(
      country_prop_table$p_value[valid_country_idx],
      method = correction_method
    )
    country_prop_table$p_value[valid_country_idx]   <- p_adj_country
    country_prop_table$p_fmt[valid_country_idx]     <-
      purrr::map_chr(p_adj_country, .fmt_pvalue)
    country_prop_table$significant[valid_country_idx] <-
      p_adj_country < alpha
  }
  .log_step(sprintf(
    "[STATS] Country proportions (%s-corrected): %d/%d comparisons p<%g",
    correction_method,
    sum(country_prop_table$significant, na.rm = TRUE),
    nrow(country_prop_table), alpha
  ), verbose)

  # ---- 6. Summary sentences with p-values ----
  focal_growth_row <- dplyr::filter(growth_trends_table,
    .data$subspecialty == focal_subspecialty)

  s_stats_citations <- sprintf(
    paste(
      "Citation counts per paper differed significantly across",
      "the %d subspecialties (Kruskal-Wallis H=%s, df=%d, p%s).",
      "%s demonstrated a median of %s citations per paper",
      "(vs. %s for urology%s)."
    ),
    length(subspecialty_results_list),
    round(kw_list$H, 1L),
    kw_list$df,
    kw_list$p_fmt,
    focal_subspecialty,
    round(median(focal_citations), 0L),
    {
      ur <- pairwise_citations_table |>
        dplyr::filter(grepl("urology",
          tolower(.data$subspecialty)))
      if (nrow(ur) > 0) round(ur$comparator_median[[1L]], 0L) else "N/A"
    },
    {
      ur <- pairwise_citations_table |>
        dplyr::filter(grepl("urology",
          tolower(.data$subspecialty)))
      if (nrow(ur) > 0)
        sprintf("; p%s after %s correction",
          ur$p_adj_fmt[[1L]], correction_method)
      else ""
    }
  )

  s_stats_growth <- if (!is.na(focal_growth_row$p_value)) {
    sprintf(
      paste(
        "Annual publication growth in %s was %s",
        "(Poisson IRR=%.4f, 95%% CI %.4f-%.4f, p%s);",
        "growth slopes differed %sacross subspecialties",
        "(F(%d,%d)=%.2f, p%s)."
      ),
      focal_subspecialty,
      ifelse(focal_growth_row$significant,
        "statistically significant", "not statistically significant"),
      focal_growth_row$irr,
      focal_growth_row$irr_lo95,
      focal_growth_row$irr_hi95,
      focal_growth_row$p_fmt,
      ifelse(growth_heterogeneity_list$significant,
        "significantly ", "non-significantly "),
      growth_heterogeneity_list$df_numerator,
      growth_heterogeneity_list$df_denominator,
      growth_heterogeneity_list$F_statistic,
      growth_heterogeneity_list$p_fmt
    )
  } else {
    sprintf(
      "Growth trend data were insufficient for %s to compute statistics.",
      focal_subspecialty
    )
  }

  summary_sentences_list <- list(
    stats_citations = s_stats_citations,
    stats_growth    = s_stats_growth
  )

  # ---- 7. Evidence quality: pairwise prop.test on pct_high_evidence ----
  # Tests whether the proportion of high-evidence papers (RCT + meta-analysis
  # + multicenter) in the focal subspecialty differs significantly from each
  # comparator.  Uses the same two-proportion continuity-corrected z-test
  # as country_proportions. Requires pct_high_evidence and total_documents
  # in the comparison table (populated by .compute_evidence_quality_score()).
  evidence_quality_table <- NULL
  if ("pct_high_evidence" %in% names(comp_tbl) &&
      "total_documents"   %in% names(comp_tbl) &&
      !is.na(focal_row$pct_high_evidence)) {

    focal_hi_n <- round(
      focal_row$pct_high_evidence * focal_row$total_documents / 100
    )

    evidence_quality_table <- purrr::map_dfr(
      setdiff(comp_tbl$subspecialty, focal_subspecialty),
      function(comp_name) {
        other_row  <- dplyr::filter(comp_tbl,
          .data$subspecialty == comp_name)
        other_hi_n <- round(
          other_row$pct_high_evidence * other_row$total_documents / 100
        )

        na_row <- dplyr::tibble(
          subspecialty       = comp_name,
          focal_pct_high     = focal_row$pct_high_evidence,
          comparator_pct_high = other_row$pct_high_evidence,
          chi2               = NA_real_,
          p_value            = NA_real_,
          p_fmt              = "N/A",
          significant        = FALSE
        )

        if (is.na(other_hi_n) || other_hi_n <= 0L ||
            is.na(focal_hi_n) || focal_hi_n <= 0L) {
          return(na_row)
        }

        pt <- tryCatch(
          stats::prop.test(
            x = c(focal_hi_n,  other_hi_n),
            n = c(focal_row$total_documents,
                  other_row$total_documents),
            correct = TRUE
          ),
          error = function(e) NULL
        )

        if (is.null(pt)) return(na_row)

        dplyr::tibble(
          subspecialty        = comp_name,
          focal_pct_high      = focal_row$pct_high_evidence,
          comparator_pct_high = other_row$pct_high_evidence,
          chi2                = round(pt$statistic, 3L),
          p_value             = pt$p.value,
          p_fmt               = .fmt_pvalue(pt$p.value),
          significant         = pt$p.value < alpha
        )
      }
    )

    # Apply BH correction across all k−1 evidence quality proportion tests.
    valid_ev_idx <- which(!is.na(evidence_quality_table$p_value))
    if (length(valid_ev_idx) > 0L) {
      p_adj_ev <- stats::p.adjust(
        evidence_quality_table$p_value[valid_ev_idx],
        method = correction_method
      )
      evidence_quality_table$p_value[valid_ev_idx]   <- p_adj_ev
      evidence_quality_table$p_fmt[valid_ev_idx]     <-
        purrr::map_chr(p_adj_ev, .fmt_pvalue)
      evidence_quality_table$significant[valid_ev_idx] <-
        p_adj_ev < alpha
    }
    .log_step(sprintf(
      "[STATS] Evidence quality proportions (%s-corrected): %d/%d p<%g",
      correction_method,
      sum(evidence_quality_table$significant, na.rm = TRUE),
      nrow(evidence_quality_table), alpha
    ), verbose)
  }

  if (isTRUE(verbose)) {
    message("\n=== STATISTICAL RESULTS ===")
    message(sprintf("\n[CITATIONS]\n%s", s_stats_citations))
    message(sprintf("\n[GROWTH]\n%s", s_stats_growth))
    message("===========================\n")
  }

  return(list(
    kruskal_wallis         = kw_list,
    pairwise_citations     = pairwise_citations_table,
    growth_trends          = growth_trends_table,
    growth_heterogeneity   = growth_heterogeneity_list,
    country_proportions    = country_prop_table,
    evidence_quality       = evidence_quality_table,
    summary_sentences      = summary_sentences_list,
    correction_method      = correction_method,   # propagate for abstract
    alpha                  = alpha                # propagate for abstract
  ))
}

#' @noRd
.fmt_pvalue <- function(p) {
  assertthat::assert_that(
    is.numeric(p) && length(p) == 1L,
    msg = "p must be a single numeric value in .fmt_pvalue()"
  )
  if (is.na(p))  return("=NA")
  if (p <= 0.001) return("<0.001")  # boundary: p=0.001 included
  if (p < 0.01)  return(sprintf("=%.3f", p))
  if (p < 0.10)  return(sprintf("=%.2f", p))
  return(sprintf("=%.2f", p))
}



# ============================================================
# FEATURE 1: FPMRS → URPS NOMENCLATURE TRANSITION ANALYSIS
# The subspecialty was officially renamed to Urogynecology and
# Reconstructive Pelvic Surgery (URPS) on January 1, 2024.
# ============================================================

#' Analyse the FPMRS-to-URPS Nomenclature Transition in the Literature
#'
#' @description
#' Tracks how the literature has shifted from "FPMRS" to "URPS" and
#' "urogynecology" terminology over time. Counts annual title/abstract
#' mentions of each term to quantify when the field's self-description
#' changed. Produces a tidy tibble and an optional ggplot2 figure.
#'
#' @param bibliography Data frame with \code{TI} (title), \code{AB}
#'   (abstract), \code{PY} (publication year), and
#'   \code{publication_year} columns.
#' @param focal_year Integer. Year the official rename occurred.
#'   Defaults to \code{2024L}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A list with:
#' \describe{
#'   \item{\code{annual_counts}}{Tibble: year × term × n_mentions.}
#'   \item{\code{transition_year}}{Integer year when URPS first exceeded
#'     FPMRS mentions.}
#'   \item{\code{pct_urps_post_rename}}{Numeric \% of post-2024 papers
#'     mentioning URPS.}
#'   \item{\code{figure}}{ggplot2 object or NULL.}
#' }
#'
#' @examples
#' \dontrun{
#' result <- run_fpmrs_bibliometric_pipeline(...)
#' nom <- compute_nomenclature_transition(
#'   result$bibliography, focal_year = 2024L
#' )
#' nom$figure
#' }
#'
#' @importFrom assertthat assert_that is.flag is.count
#' @importFrom dplyr tibble filter mutate bind_rows group_by summarise
#'   arrange case_when if_else
#' @importFrom purrr map_dfr
#' @importFrom stringr str_detect regex
#' @importFrom ggplot2 ggplot aes geom_line geom_vline geom_point
#'   scale_color_manual labs theme_minimal annotate
#' @importFrom rlang .data
#' @export
compute_nomenclature_transition <- function(
    bibliography,
    focal_year  = 2024L,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    "publication_year" %in% names(bibliography),
    msg = "`bibliography` must contain 'publication_year'."
  )

  .log_step("[NOMENCLATURE] Analysing FPMRS-to-URPS terminology shift ...",
            verbose)

  # ---- Build free-text search field: title + abstract ----
  ti <- if ("TI" %in% names(bibliography)) bibliography$TI else ""
  ab <- if ("AB" %in% names(bibliography)) bibliography$AB else ""
  full_text <- paste(
    toupper(dplyr::coalesce(as.character(ti), "")),
    toupper(dplyr::coalesce(as.character(ab), ""))
  )

  # ---- Term detection patterns ----
  terms <- list(
    FPMRS        = "\\bFPMRS\\b",
    URPS         = "\\bURPS\\b",
    urogynecology = "UROGYNECOL",    # catches urogynecology/urogynecological
    pelvic_floor  = "PELVIC FLOOR",
    reconstructive_pelvic = "RECONSTRUCTIVE PELVIC SURG"
  )

  # Guard: empty bibliography returns an empty-but-valid result structure
  if (nrow(bibliography) == 0L || all(is.na(bibliography$publication_year))) {
    .log_step("[NOMENCLATURE] Empty bibliography -- returning zero-count result.",
              verbose)
    empty_counts <- dplyr::tibble(
      publication_year = integer(),
      term             = character(),
      n_papers         = integer(),
      n_mentions       = integer()
    )
    return(list(
      annual_counts        = empty_counts,
      transition_year      = NA_integer_,
      pct_urps_post_rename = NA_real_,
      figure               = NULL
    ))
  }
  yr_range <- range(bibliography$publication_year, na.rm = TRUE)
  all_years <- seq(yr_range[1L], yr_range[2L])

  annual_counts <- purrr::map_dfr(names(terms), function(term_name) {
    pattern <- terms[[term_name]]
    hits    <- stringr::str_detect(full_text, stringr::regex(pattern))
    purrr::map_dfr(all_years, function(yr) {
      mask <- bibliography$publication_year == yr & !is.na(hits)
      dplyr::tibble(
        publication_year = yr,
        term             = term_name,
        n_papers         = sum(mask),
        n_mentions       = sum(hits[mask], na.rm = TRUE)
      )
    })
  })

  # ---- Transition year: first year URPS > FPMRS ----
  urps_wider <- annual_counts |>
    dplyr::filter(.data$term %in% c("FPMRS", "URPS")) |>
    tidyr::pivot_wider(names_from = "term",
                       values_from = "n_mentions",
                       values_fill = 0L) |>
    dplyr::arrange(.data$publication_year)

  # tidyr may not always be available; use dplyr fallback
  transition_year <- tryCatch({
    cross <- urps_wider[
      !is.na(urps_wider$URPS) &
      !is.na(urps_wider$FPMRS) &
      urps_wider$URPS > urps_wider$FPMRS, ]
    if (nrow(cross) > 0L) min(cross$publication_year) else NA_integer_
  }, error = function(e) NA_integer_)

  # ---- Post-rename URPS adoption rate ----
  post_rename <- annual_counts |>
    dplyr::filter(
      .data$publication_year >= focal_year,
      .data$term == "URPS"
    )
  total_post <- sum(
    annual_counts$n_papers[annual_counts$publication_year >= focal_year &
                           annual_counts$term == "FPMRS"]
  )
  pct_urps_post <- if (total_post > 0L) {
    round(sum(post_rename$n_mentions) / total_post * 100, 1L)
  } else NA_real_

  .log_step(sprintf(
    "[NOMENCLATURE] URPS first exceeds FPMRS in: %s | Post-%d URPS adoption: %.1f%%",
    if (is.na(transition_year)) "not yet" else as.character(transition_year),
    focal_year, dplyr::coalesce(pct_urps_post, 0)
  ), verbose)

  # ---- Figure ----
  fig <- tryCatch({
    plot_data <- annual_counts |>
      dplyr::filter(.data$term %in% c("FPMRS","URPS","urogynecology")) |>
      dplyr::mutate(term = dplyr::case_when(
        .data$term == "urogynecology" ~ "Urogynecology",
        TRUE ~ .data$term
      ))

    ggplot2::ggplot(
      plot_data,
      ggplot2::aes(x = .data$publication_year,
                   y = .data$n_mentions,
                   color = .data$term)
    ) +
      ggplot2::geom_line(linewidth = 1.1) +
      ggplot2::geom_point(size = 2.0) +
      ggplot2::geom_vline(xintercept = focal_year - 0.5,
        linetype = "dashed", color = "grey40", linewidth = 0.7) +
      ggplot2::annotate("text", x = focal_year + 0.3, y = Inf,
        label = sprintf("URPS rename\n(%d)", focal_year),
        vjust = 1.4, hjust = 0, size = 3, color = "grey30") +
      ggplot2::scale_color_manual(
        values = c("FPMRS"="#2166AC", "URPS"="#D73027",
                   "Urogynecology"="#4DAF4A"),
        name = NULL
      ) +
      ggplot2::labs(
        title    = "FPMRS \u2192 URPS Nomenclature Transition",
        subtitle = sprintf(
          "Annual title/abstract mentions | Official rename: Jan 1, %d",
          focal_year),
        x = "Year", y = "Papers mentioning term"
      ) +
      ggplot2::theme_minimal(base_size = 10) +
      ggplot2::theme(legend.position = "top")
  }, error = function(e) NULL)

  list(
    annual_counts       = annual_counts,
    transition_year     = transition_year,
    pct_urps_post_rename = pct_urps_post,
    figure              = fig
  )
}


#' Run Mesh Regulatory Interrupted Time-Series Analysis
#'
#' @description
#' Wrapper around \code{\link{compute_mesh_its}} that runs the full
#' mesh regulatory ITS analysis, produces a comparison of FPMRS vs
#' comparator subspecialties, and saves results. The analysis tests
#' whether FDA mesh safety notifications (2008, 2011, 2016, 2018,
#' 2019) produced statistically significant slope changes in
#' mesh-related, sling-related, and prolapse-related publication
#' subcorpora across subspecialties.
#'
#' @param subspecialty_results_list Named list of subspecialty results
#'   from \code{run_fpmrs_bibliometric_pipeline()} or
#'   \code{run_subspecialty_comparison()}.
#' @param focal_subspecialty Character. Defaults to \code{"FPMRS"}.
#' @param breakpoints Named integer vector of regulatory event years.
#' @param output_dir Character. Directory for saved outputs.
#' @param alpha Numeric significance threshold. Defaults to
#'   \code{0.05}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A list with one element per subspecialty, each containing
#'   the output of \code{compute_mesh_its()}.
#'
#' @examples
#' \dontrun{
#' its <- run_mesh_its_analysis(
#'   subspecialty_results_list = sp_list,
#'   focal_subspecialty        = "FPMRS",
#'   output_dir                = "output/mesh_its/"
#' )
#' its$FPMRS$segment_fits
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom purrr map
#' @export
run_mesh_its_analysis <- function(
    subspecialty_results_list,
    focal_subspecialty = "FPMRS",
    breakpoints = c(
      "2008 FDA notification"           = 2008L,
      "2011 FDA safety update"          = 2011L,
      "2016 FDA safety communication"   = 2016L,
      "2018 UK mesh pause"              = 2018L,
      "2019 FDA market withdrawal"      = 2019L
    ),
    output_dir = "output/mesh_its/",
    alpha      = 0.05,
    verbose    = TRUE
) {
  assertthat::assert_that(is.list(subspecialty_results_list))
  assertthat::assert_that(assertthat::is.string(focal_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    focal_subspecialty %in% names(subspecialty_results_list),
    msg = sprintf("'%s' not found in subspecialty_results_list.",
                  focal_subspecialty)
  )

  if (!dir.exists(output_dir))
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  .log_step(sprintf(
    "[MESH ITS] Running ITS for %d subspecialties, %d breakpoints ...",
    length(subspecialty_results_list), length(breakpoints)
  ), verbose)

  its_results <- purrr::map(
    subspecialty_results_list,
    function(sp) {
      nm <- sp$subspecialty
      .log_step(sprintf("[MESH ITS]   Processing: %s", nm), verbose)
      tryCatch(
        compute_mesh_its(sp$bibliography, breakpoints = breakpoints,
                         alpha = alpha, verbose = FALSE),
        error = function(e) {
          .log_step(sprintf("[MESH ITS]   Error for %s: %s", nm, e$message),
                    verbose)
          NULL
        }
      )
    }
  )
  names(its_results) <- names(subspecialty_results_list)

  # ---- Build focal vs comparator comparison table ----
  focal_fits <- its_results[[focal_subspecialty]]$segment_fits
  if (!is.null(focal_fits) && nrow(focal_fits) > 0L) {
    csv_path <- file.path(output_dir,
      sprintf("mesh_its_%s_segments.csv", focal_subspecialty))
    utils::write.csv(focal_fits, csv_path, row.names = FALSE)
    .log_step(sprintf("[MESH ITS] Focal segment fits -> %s", csv_path), verbose)
  }

  .log_step(sprintf("[MESH ITS] Complete. %d subspecialties analysed.",
    sum(!sapply(its_results, is.null))), verbose)

  its_results
}


#' Compare First-Author Female Representation Across Subspecialties
#'
#' @description
#' Computes first-author female representation per era for every
#' subspecialty in the comparison list, then runs pairwise chi-square
#' tests (focal vs each comparator) and BH-corrects across the family.
#' Produces a tidy summary table and an optional time-series figure.
#'
#' FPMRS is unusual: it is a surgical subspecialty that primarily
#' treats women, trained by a workforce that is majority female at
#' fellowship level. This function tests whether the female first-author
#' trajectory differs statistically from comparator subspecialties.
#'
#' @param subspecialty_results_list Named list of subspecialty results.
#' @param focal_subspecialty Character. Defaults to \code{"FPMRS"}.
#' @param eras Named list of era year ranges. \code{NULL} auto-derives
#'   from bibliography year range.
#' @param correction_method Character p.adjust method. Defaults to
#'   \code{"BH"}.
#' @param alpha Numeric significance threshold. Defaults to
#'   \code{0.05}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A list with \code{by_era_table}, \code{pairwise_tests},
#'   and \code{figure}.
#'
#' @examples
#' \dontrun{
#' gender_result <- compute_comparative_gender_trends(
#'   subspecialty_results_list,
#'   focal_subspecialty = "FPMRS"
#' )
#' gender_result$pairwise_tests
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr bind_rows filter mutate arrange
#' @importFrom purrr map_dfr
#' @importFrom stats chisq.test p.adjust
#' @importFrom ggplot2 ggplot aes geom_line geom_point scale_color_manual
#'   labs theme_minimal
#' @importFrom rlang .data
#' @export
compute_comparative_gender_trends <- function(
    subspecialty_results_list,
    focal_subspecialty = "FPMRS",
    eras               = NULL,
    correction_method  = "BH",
    alpha              = 0.05,
    verbose            = TRUE
) {
  assertthat::assert_that(is.list(subspecialty_results_list) &&
                          length(subspecialty_results_list) >= 2L)
  assertthat::assert_that(assertthat::is.string(focal_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[GENDER COMP] Computing female first-authorship across %d subspecialties ...",
    length(subspecialty_results_list)
  ), verbose)

  # ---- Per-subspecialty era gender table ----
  all_gender <- purrr::map_dfr(
    subspecialty_results_list,
    function(sp) {
      sp_eras <- if (!is.null(eras)) eras else {
        yr <- range(sp$bibliography$publication_year, na.rm = TRUE)
        make_eras(yr[1L], yr[2L])
      }
      gt <- tryCatch(
        .compute_gender_by_era(sp$bibliography, sp_eras, verbose = FALSE),
        error = function(e) NULL
      )
      if (is.null(gt) || nrow(gt) == 0L) return(dplyr::tibble())
      dplyr::mutate(gt, subspecialty = sp$subspecialty)
    }
  )

  if (nrow(all_gender) == 0L) {
    .log_step("[GENDER COMP] No gender data available.", verbose)
    return(list(by_era_table = dplyr::tibble(),
                pairwise_tests = dplyr::tibble(),
                figure = NULL))
  }

  # ---- Pairwise chi-square: focal vs each comparator (pooled across eras) ----
  focal_g  <- dplyr::filter(all_gender, .data$subspecialty == focal_subspecialty)
  comp_sps <- setdiff(unique(all_gender$subspecialty), focal_subspecialty)

  focal_female <- sum(focal_g$n_female,      na.rm = TRUE)
  focal_male   <- sum(focal_g$n_classified -
                        focal_g$n_female,    na.rm = TRUE)

  pairwise_raw <- purrr::map_dfr(comp_sps, function(comp_nm) {
    comp_g       <- dplyr::filter(all_gender, .data$subspecialty == comp_nm)
    comp_female  <- sum(comp_g$n_female,     na.rm = TRUE)
    comp_male    <- sum(comp_g$n_classified -
                          comp_g$n_female,   na.rm = TRUE)

    na_row <- dplyr::tibble(
      comparator    = comp_nm,
      focal_pct_f   = round(focal_female / max(focal_female+focal_male,1L)*100, 1L),
      comp_pct_f    = NA_real_,
      chi2          = NA_real_,
      p_value       = NA_real_,
      p_fmt         = "N/A",
      significant   = FALSE
    )

    if (comp_female + comp_male < 10L || focal_female + focal_male < 10L)
      return(na_row)

    ct <- tryCatch(
      stats::chisq.test(
        matrix(c(focal_female, focal_male, comp_female, comp_male),
               nrow = 2L),
        correct = TRUE
      ),
      error = function(e) NULL
    )
    if (is.null(ct)) return(na_row)

    dplyr::tibble(
      comparator  = comp_nm,
      focal_pct_f = round(focal_female/(focal_female+focal_male)*100, 1L),
      comp_pct_f  = round(comp_female /(comp_female +comp_male )*100, 1L),
      chi2        = round(ct$statistic, 3L),
      p_value     = ct$p.value,
      p_fmt       = .fmt_pvalue(ct$p.value),
      significant = ct$p.value < alpha
    )
  })

  # BH correction
  valid_idx <- which(!is.na(pairwise_raw$p_value))
  if (length(valid_idx) > 0L) {
    p_adj <- stats::p.adjust(pairwise_raw$p_value[valid_idx],
                             method = correction_method)
    pairwise_raw$p_value[valid_idx]   <- p_adj
    pairwise_raw$p_fmt[valid_idx]     <- purrr::map_chr(p_adj, .fmt_pvalue)
    pairwise_raw$significant[valid_idx] <- p_adj < alpha
  }

  .log_step(sprintf(
    "[GENDER COMP] Focal %s: %.1f%% female first-author | %d/%d comparisons significant (%s)",
    focal_subspecialty,
    round(focal_female/(focal_female+focal_male)*100, 1L),
    sum(pairwise_raw$significant, na.rm=TRUE),
    nrow(pairwise_raw), correction_method
  ), verbose)

  # ---- Figure: female % by era, all subspecialties ----
  fig <- tryCatch({
    plot_gender <- all_gender |>
      dplyr::filter(!is.na(.data$pct_female)) |>
      dplyr::mutate(is_focal = .data$subspecialty == focal_subspecialty)

    ggplot2::ggplot(
      plot_gender,
      ggplot2::aes(x = .data$era, y = .data$pct_female,
                   group = .data$subspecialty, color = .data$is_focal,
                   linewidth = .data$is_focal)
    ) +
      ggplot2::geom_line() +
      ggplot2::geom_point(size = 2.5) +
      ggplot2::scale_color_manual(
        values = c("TRUE" = "#2166AC", "FALSE" = "#BDBDBD"),
        labels = c("TRUE" = focal_subspecialty, "FALSE" = "Comparators"),
        name   = NULL
      ) +
      ggplot2::scale_discrete_manual("linewidth",
        values = c("TRUE" = 1.3, "FALSE" = 0.5)) +
      ggplot2::labs(
        title    = "Female First-Authorship by Era",
        subtitle = sprintf("%s vs. comparator subspecialties", focal_subspecialty),
        x = "Era", y = "Female first authors (%)"
      ) +
      ggplot2::theme_minimal(base_size = 10) +
      ggplot2::theme(legend.position = "top",
                     axis.text.x = ggplot2::element_text(angle=30, hjust=1))
  }, error = function(e) NULL)

  list(
    by_era_table   = all_gender,
    pairwise_tests = pairwise_raw,
    figure         = fig
  )
}


#' Compare NIH Funding Density Across Subspecialties
#'
#' @description
#' For each subspecialty in \code{subspecialty_results_list}, computes
#' NIH funding density from the WoS \code{FU} (Funding Acknowledgements)
#' field. Metrics include overall NIH-funded percentage, grant-mechanism
#' breakdown (R01/R21/U01/P01/other), era-by-era trend, and a
#' publications-per-grant ratio when \code{n_grants} is supplied.
#'
#' \strong{Data requirement}: the \code{FU} field is only populated in
#' Web of Science exports; PubMed-only corpora will return
#' \code{NA} for all funding columns with an explanatory note.
#'
#' @param subspecialty_results_list Named list. Each element must have
#'   a \code{bibliography} data frame and a \code{subspecialty}
#'   character field.
#' @param focal_subspecialty Character. Defaults to \code{"FPMRS"}.
#' @param eras Named list of era year ranges. \code{NULL} auto-derives
#'   from bibliography year range.
#' @param correction_method Character. p.adjust method for pairwise
#'   proportion tests. Defaults to \code{"BH"}.
#' @param alpha Numeric significance threshold. Defaults to
#'   \code{0.05}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A list with:
#' \describe{
#'   \item{\code{summary_table}}{One row per subspecialty: total papers,
#'     NIH-funded count, pct, coverage, grant-type counts.}
#'   \item{\code{era_table}}{NIH-funded percentage per era per
#'     subspecialty.}
#'   \item{\code{grant_type_table}}{R01/R21/U01/P01/other breakdown per
#'     subspecialty.}
#'   \item{\code{pairwise_tests}}{BH-corrected two-proportion z-tests,
#'     focal vs each comparator.}
#'   \item{\code{coverage_warning}}{Character note when FU coverage is
#'     below 50\% in the focal subspecialty.}
#'   \item{\code{figure}}{ggplot2 object comparing NIH-funded \% by era,
#'     or \code{NULL} if insufficient data.}
#' }
#'
#' @examples
#' \dontrun{
#' # Requires Web of Science export (FU field)
#' funding <- compute_comparative_funding(
#'   subspecialty_results_list = sp_list,
#'   focal_subspecialty        = "FPMRS",
#'   correction_method         = "BH",
#'   verbose                   = TRUE
#' )
#' funding$summary_table
#' funding$grant_type_table
#' funding$pairwise_tests
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag is.number
#' @importFrom dplyr tibble bind_rows filter mutate summarise group_by
#'   arrange coalesce
#' @importFrom purrr map_dfr map_chr
#' @importFrom stats prop.test p.adjust
#' @importFrom ggplot2 ggplot aes geom_col geom_errorbar position_dodge
#'   scale_fill_manual labs theme_minimal theme element_text
#' @importFrom rlang .data
#' @export
compute_comparative_funding <- function(
    subspecialty_results_list,
    focal_subspecialty = "FPMRS",
    eras               = NULL,
    correction_method  = "BH",
    alpha              = 0.05,
    verbose            = TRUE
) {
  assertthat::assert_that(
    is.list(subspecialty_results_list) &&
      length(subspecialty_results_list) >= 2L,
    msg = "`subspecialty_results_list` must be a list of >= 2 elements."
  )
  assertthat::assert_that(assertthat::is.string(focal_subspecialty))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    assertthat::is.number(alpha) && alpha > 0 && alpha < 1,
    msg = "`alpha` must be in (0, 1)."
  )
  assertthat::assert_that(
    focal_subspecialty %in% names(subspecialty_results_list),
    msg = sprintf("'%s' not found in subspecialty_results_list.",
                  focal_subspecialty)
  )

  .log_step(sprintf(
    "[FUNDING COMP] Comparing NIH funding density across %d subspecialties ...",
    length(subspecialty_results_list)
  ), verbose)

  # ---- Grant-type regex patterns ----
  # Patterns match WoS FU field format: "National Institutes of Health [R01 CA123456]"
  # or abbreviated forms like "NIH R01" or "R01-CA123456"
  GRANT_PATTERNS <- list(
    R01   = "R01[-\\s]?[A-Z]{2}[0-9]|\\bR01\\b",
    R21   = "R21[-\\s]?[A-Z]{2}[0-9]|\\bR21\\b",
    U01   = "U01[-\\s]?[A-Z]{2}[0-9]|\\bU01\\b",
    P01   = "P01[-\\s]?[A-Z]{2}[0-9]|\\bP01\\b",
    K_mech = "\\bK[0-9]{2}\\b",   # K08, K23, K99 etc.
    T_mech = "\\bT[0-9]{2}\\b"    # T32 training grants
  )

  # NIH detection pattern.
  # Covers: abbreviated agency names, spelled-out full names (as
  # they appear in WoS FU field: "National Institutes of Health"),
  # and bare NIH grant mechanism numbers (R01-CA..., R21 HD...) which
  # are often listed without an agency name in acknowledgement fields.
  NIH_PATTERN <- paste(
    # Abbreviated NIH agency names
    "\\bNIH\\b", "\\bNICHD\\b", "\\bNCI\\b", "\\bNIDDK\\b",
    "\\bNHLBI\\b", "\\bNIMH\\b", "\\bNIAID\\b", "\\bNINDS\\b",
    "\\bNIA\\b",  "\\bNIMHD\\b", "\\bNCATS\\b",
    # Spelled-out forms used in WoS FU field
    "NATIONAL INSTITUTES OF HEALTH",
    "NATIONAL INSTITUTE OF",
    # Bare NIH mechanism numbers (R01, R21, U01, P01) with institute code
    "\\bR0[12][-\\s]?[A-Z]{2}[0-9]",
    "\\bU01[-\\s]?[A-Z]{2}[0-9]",
    "\\bP01[-\\s]?[A-Z]{2}[0-9]",
    sep = "|"
  )

  # ---- Helper: analyse one bibliography ----
  .analyse_funding <- function(bib, sp_nm, sp_eras) {
    has_fu <- "FU" %in% names(bib)
    n_total <- nrow(bib)

    if (!has_fu || n_total == 0L) {
      .log_step(sprintf(
        "[FUNDING COMP]   %s: FU field absent -- WoS required.", sp_nm
      ), verbose)
      return(list(
        summary    = dplyr::tibble(
          subspecialty      = sp_nm,
          n_total           = n_total,
          n_fu_populated    = 0L,
          fu_coverage_pct   = 0,
          n_nih_funded      = 0L,
          pct_nih_funded    = NA_real_,
          n_r01             = 0L, n_r21 = 0L,
          n_u01             = 0L, n_p01 = 0L,
          n_k_mech          = 0L, n_t_mech = 0L,
          data_source_note  = "FU absent: requires WoS export"
        ),
        era_rows   = dplyr::tibble(),
        grant_rows = dplyr::tibble()
      ))
    }

    fu_upper   <- toupper(dplyr::coalesce(as.character(bib$FU), ""))
    has_data   <- nchar(trimws(fu_upper)) > 0
    is_nih     <- grepl(NIH_PATTERN, fu_upper, perl = TRUE)
    n_fu       <- sum(has_data)
    n_nih      <- sum(is_nih)
    fu_cov     <- round(n_fu / n_total * 100, 1L)
    pct_nih    <- if (n_fu > 0L) round(n_nih / n_fu * 100, 1L) else NA_real_

    # Grant type counts (among ALL papers, not just NIH-funded)
    grant_counts <- purrr::map(GRANT_PATTERNS, function(pat) {
      sum(grepl(pat, fu_upper, perl = TRUE))
    })

    # Era breakdown
    era_rows <- purrr::map_dfr(names(sp_eras), function(era_nm) {
      yr      <- sp_eras[[era_nm]]
      mask    <- bib$publication_year >= yr[1L] &
                 bib$publication_year <= yr[2L]
      fu_mask <- has_data & mask
      nih_mask <- is_nih & mask
      dplyr::tibble(
        subspecialty    = sp_nm,
        era             = era_nm,
        n_papers        = sum(mask, na.rm = TRUE),
        n_fu_populated  = sum(fu_mask, na.rm = TRUE),
        n_nih_funded    = sum(nih_mask, na.rm = TRUE),
        pct_nih_funded  = if (sum(fu_mask) > 0L)
          round(sum(nih_mask) / sum(fu_mask) * 100, 1L)
          else NA_real_
      )
    })

    .log_step(sprintf(
      "[FUNDING COMP]   %s: NIH-funded %.1f%% (%d/%d, FU coverage %.0f%%)",
      sp_nm, dplyr::coalesce(pct_nih, 0),
      n_nih, n_fu, fu_cov
    ), verbose)

    list(
      summary = dplyr::tibble(
        subspecialty     = sp_nm,
        n_total          = n_total,
        n_fu_populated   = n_fu,
        fu_coverage_pct  = fu_cov,
        n_nih_funded     = n_nih,
        pct_nih_funded   = pct_nih,
        n_r01            = grant_counts$R01,
        n_r21            = grant_counts$R21,
        n_u01            = grant_counts$U01,
        n_p01            = grant_counts$P01,
        n_k_mech         = grant_counts$K_mech,
        n_t_mech         = grant_counts$T_mech,
        data_source_note = if (fu_cov < 50)
          sprintf("FU coverage %.0f%% -- interpret cautiously.", fu_cov)
          else "OK"
      ),
      era_rows = era_rows,
      grant_rows = dplyr::tibble(
        subspecialty = sp_nm,
        grant_type   = names(GRANT_PATTERNS),
        n_papers     = unlist(grant_counts)
      )
    )
  }

  # ---- Run for each subspecialty ----
  all_analyses <- lapply(names(subspecialty_results_list), function(nm) {
    sp       <- subspecialty_results_list[[nm]]
    sp_eras  <- if (!is.null(eras)) eras else {
      yr <- range(sp$bibliography$publication_year, na.rm = TRUE)
      make_eras(yr[1L], yr[2L])
    }
    .analyse_funding(sp$bibliography, sp$subspecialty, sp_eras)
  })

  summary_table  <- dplyr::bind_rows(lapply(all_analyses, `[[`, "summary"))
  era_table      <- dplyr::bind_rows(lapply(all_analyses, `[[`, "era_rows"))
  grant_type_table <- dplyr::bind_rows(lapply(all_analyses, `[[`, "grant_rows"))

  # ---- Coverage warning ----
  focal_cov <- summary_table$fu_coverage_pct[
    summary_table$subspecialty == focal_subspecialty]
  coverage_warning <- if (length(focal_cov) > 0L && focal_cov < 50) {
    sprintf(paste(
      "FU field coverage for %s is only %.0f%%.",
      "Funding comparisons require Web of Science exports.",
      "Consider re-running with data_source = 'wos' or 'both'."
    ), focal_subspecialty, focal_cov)
  } else NA_character_

  if (!is.na(coverage_warning))
    .log_step(paste("[FUNDING COMP] WARNING:", coverage_warning), verbose)

  # ---- Pairwise two-proportion z-tests: focal vs comparators ----
  focal_row  <- summary_table[summary_table$subspecialty == focal_subspecialty, ]
  comp_rows  <- summary_table[summary_table$subspecialty != focal_subspecialty, ]

  pairwise_tests <- dplyr::bind_rows(lapply(seq_len(nrow(comp_rows)), function(i) {
    comp <- comp_rows[i, ]
    focal_n  <- focal_row$n_fu_populated
    focal_k  <- focal_row$n_nih_funded
    comp_n   <- comp$n_fu_populated
    comp_k   <- comp$n_nih_funded

    na_row <- dplyr::tibble(
      comparator       = comp$subspecialty,
      focal_pct_funded = focal_row$pct_nih_funded,
      comp_pct_funded  = comp$pct_nih_funded,
      chi2             = NA_real_,
      p_value          = NA_real_,
      p_fmt            = "N/A",
      significant      = FALSE
    )

    if (any(is.na(c(focal_n, focal_k, comp_n, comp_k))) ||
        focal_n < 5L || comp_n < 5L)
      return(na_row)

    pt <- tryCatch(
      stats::prop.test(
        c(focal_k, comp_k),
        c(focal_n, comp_n),
        correct = TRUE
      ),
      error = function(e) NULL
    )
    if (is.null(pt)) return(na_row)

    dplyr::tibble(
      comparator       = comp$subspecialty,
      focal_pct_funded = focal_row$pct_nih_funded,
      comp_pct_funded  = comp$pct_nih_funded,
      chi2             = round(pt$statistic, 3L),
      p_value          = pt$p.value,
      p_fmt            = .fmt_pvalue(pt$p.value),
      significant      = pt$p.value < alpha
    )
  }))

  # BH correction
  valid_idx <- which(!is.na(pairwise_tests$p_value))
  if (length(valid_idx) > 0L) {
    p_adj <- stats::p.adjust(pairwise_tests$p_value[valid_idx],
                             method = correction_method)
    pairwise_tests$p_value[valid_idx]    <- p_adj
    pairwise_tests$p_fmt[valid_idx]      <- purrr::map_chr(p_adj, .fmt_pvalue)
    pairwise_tests$significant[valid_idx] <- p_adj < alpha
  }

  n_sig <- sum(pairwise_tests$significant, na.rm = TRUE)
  .log_step(sprintf(
    "[FUNDING COMP] %d/%d comparisons significant (%s-corrected)",
    n_sig, nrow(pairwise_tests), correction_method
  ), verbose)

  # ---- Figure: NIH funding % by era, all subspecialties ----
  funding_figure <- tryCatch({
    if (nrow(era_table) == 0L || all(is.na(era_table$pct_nih_funded))) {
      NULL
    } else {
      plot_data <- era_table |>
        dplyr::filter(!is.na(.data$pct_nih_funded)) |>
        dplyr::mutate(is_focal = .data$subspecialty == focal_subspecialty)

      ggplot2::ggplot(
        plot_data,
        ggplot2::aes(
          x    = .data$era,
          y    = .data$pct_nih_funded,
          group = .data$subspecialty,
          color = .data$is_focal,
          linewidth = .data$is_focal
        )
      ) +
        ggplot2::geom_line() +
        ggplot2::geom_point(size = 2.5) +
        ggplot2::scale_color_manual(
          values = c("TRUE" = "#2166AC", "FALSE" = "#BDBDBD"),
          labels = c("TRUE" = focal_subspecialty, "FALSE" = "Comparators"),
          name   = NULL
        ) +
        ggplot2::scale_discrete_manual("linewidth",
          values = c("TRUE" = 1.3, "FALSE" = 0.5)) +
        ggplot2::labs(
          title    = "NIH Funding Density by Era",
          subtitle = sprintf(
            "%s vs. comparator subspecialties | Source: WoS FU field",
            focal_subspecialty),
          x = "Era", y = "NIH-funded papers (%)",
          caption  = "Requires Web of Science FU field. NA = insufficient coverage."
        ) +
        ggplot2::theme_minimal(base_size = 10) +
        ggplot2::theme(
          legend.position  = "top",
          axis.text.x      = ggplot2::element_text(angle = 30, hjust = 1)
        )
    }
  }, error = function(e) NULL)

  list(
    summary_table    = summary_table,
    era_table        = era_table,
    grant_type_table = grant_type_table,
    pairwise_tests   = pairwise_tests,
    coverage_warning = coverage_warning,
    figure           = funding_figure
  )
}

# ============================================================
# REPRODUCIBILITY MODULE (Features 1-7)
# ============================================================
# ============================================================
# REPRODUCIBILITY MODULE
# Features 1-7: session snapshot, search provenance, query hash,
# reproducible seed, output manifest, CONSORT flow, renv lockfile
# ============================================================

# ---- Feature 1: Session Snapshot ----
#' @noRd
.write_session_snapshot <- function(output_dir, verbose = TRUE) {
  si        <- utils::sessionInfo()
  r_version <- paste(si$R.version$major, si$R.version$minor, sep = ".")

  key_pkgs  <- c("bibliometrix","pubmedR","dplyr","purrr","stringr",
                 "ggplot2","MASS","Matrix","assertthat","scales")
  pkg_versions <- vapply(key_pkgs, function(pkg) {
    tryCatch(as.character(utils::packageVersion(pkg)),
             error = function(e) "not installed")
  }, character(1L))

  snapshot <- list(
    r_version      = r_version,
    r_version_full = paste(si$R.version$version.string),
    platform       = si$platform,
    timestamp_utc  = format(Sys.time(), tz = "UTC", usetz = TRUE),
    packages       = as.list(pkg_versions),
    loaded_namespaces = sort(loadedNamespaces())
  )

  out_path <- file.path(output_dir, "session_snapshot.json")
  jsonlite::write_json(snapshot, out_path, pretty = TRUE, auto_unbox = TRUE)
  .log_step(sprintf("[REPRO] Session snapshot written: %s", out_path), verbose)
  invisible(snapshot)
}

# ---- Feature 2: Search Provenance ----
#' @noRd
.write_search_provenance <- function(
    output_dir,
    data_source,
    pubmed_query,
    file_path,
    year_start,
    year_end,
    english_only,
    n_retrieved,
    n_after_year_filter,
    n_after_lang_filter,
    n_final,
    verbose = TRUE
) {
  prov <- list(
    search_timestamp_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    data_source          = data_source,
    pubmed_query         = if (!is.null(pubmed_query)) pubmed_query else NA,
    wos_file_path        = if (!is.null(file_path)) file_path else NA,
    year_start           = year_start,
    year_end             = year_end,
    english_only         = english_only,
    records = list(
      retrieved           = n_retrieved,
      after_year_filter   = n_after_year_filter,
      after_lang_filter   = n_after_lang_filter,
      final_analysis_set  = n_final
    )
  )

  out_path <- file.path(output_dir, "search_provenance.json")
  jsonlite::write_json(prov, out_path, pretty = TRUE, auto_unbox = TRUE)
  .log_step(sprintf("[REPRO] Search provenance written: %s", out_path), verbose)
  invisible(prov)
}

# ---- Feature 3: Query Hash ----
#' @noRd
.compute_query_hash <- function(pubmed_query, data_source, year_start, year_end) {
  if (is.null(pubmed_query)) pubmed_query <- ""
  key_string <- paste(pubmed_query, data_source, year_start, year_end, sep = "|")
  digest::digest(key_string, algo = "md5", serialize = FALSE)
}

#' @noRd
.write_query_hash <- function(output_dir, query_hash, verbose = TRUE) {
  hash_path <- file.path(output_dir, "query_hash.txt")
  writeLines(c(
    paste("query_md5:", query_hash),
    paste("written_utc:", format(Sys.time(), tz = "UTC", usetz = TRUE))
  ), hash_path)
  .log_step(sprintf("[REPRO] Query hash: %s -> %s", query_hash, hash_path), verbose)
  invisible(query_hash)
}

# ---- Feature 4: Reproducible seed ----
#' @noRd
.set_pipeline_seed <- function(seed, verbose = TRUE) {
  assertthat::assert_that(
    is.numeric(seed) && length(seed) == 1L && seed == as.integer(seed),
    msg = "`seed` must be a single integer."
  )
  set.seed(as.integer(seed))
  .log_step(sprintf("[REPRO] Random seed set: %d", as.integer(seed)), verbose)
  invisible(seed)
}

# ---- Feature 5: Output manifest ----
#' @noRd
.write_output_manifest <- function(output_dir, verbose = TRUE) {
  all_files <- list.files(output_dir, full.names = TRUE, recursive = TRUE)
  all_files <- all_files[!file.info(all_files)$isdir]

  manifest_entries <- lapply(all_files, function(fp) {
    info <- file.info(fp)
    hash <- tryCatch(
      digest::digest(fp, algo = "sha256", file = TRUE),
      error = function(e) NA_character_
    )
    list(
      file      = basename(fp),
      path      = normalizePath(fp),
      size_kb   = round(info$size / 1024, 2),
      modified  = format(info$mtime, tz = "UTC", usetz = TRUE),
      sha256    = hash
    )
  })

  manifest <- list(
    manifest_timestamp_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    output_dir             = normalizePath(output_dir),
    n_files                = length(all_files),
    files                  = manifest_entries
  )

  manifest_path <- file.path(output_dir, "output_manifest.json")
  jsonlite::write_json(manifest, manifest_path, pretty = TRUE, auto_unbox = TRUE)
  .log_step(sprintf("[REPRO] Output manifest: %d files -> %s",
                    length(all_files), manifest_path), verbose)
  invisible(manifest)
}

# ---- Feature 6: CONSORT-style flow diagram ----
#' @noRd
.write_consort_flow <- function(
    output_dir,
    data_source,
    n_retrieved,
    n_excluded_year,
    n_excluded_language,
    n_excluded_dedup,
    n_final,
    year_start,
    year_end,
    english_only,
    verbose = TRUE
) {
  flow <- list(
    stage_1_retrieved   = list(n = n_retrieved,
      label = sprintf("Records retrieved from %s", data_source)),
    excluded_year       = list(n = n_excluded_year,
      label = sprintf("Excluded: outside %d-%d", year_start, year_end)),
    stage_2_year_filter = list(n = n_retrieved - n_excluded_year,
      label = "Records after year filter"),
    excluded_language   = list(n = n_excluded_language,
      label = if (english_only) "Excluded: non-English" else "Non-English retained"),
    stage_3_lang_filter = list(n = n_retrieved - n_excluded_year - n_excluded_language,
      label = "Records after language filter"),
    excluded_dedup      = list(n = n_excluded_dedup,
      label = "Excluded: duplicates (DOI/title-year)"),
    stage_4_final       = list(n = n_final,
      label = "Records included in analysis")
  )

  # JSON representation
  flow_path <- file.path(output_dir, "consort_flow.json")
  jsonlite::write_json(flow, flow_path, pretty = TRUE, auto_unbox = TRUE)

  # Plain text representation
  txt_path <- file.path(output_dir, "consort_flow.txt")
  lines <- c(
    "CONSORT-STYLE RECORD FLOW",
    paste(rep("=", 40L), collapse = ""),
    sprintf("Records retrieved            : %d", n_retrieved),
    sprintf("  - Excluded (year range)    : %d", n_excluded_year),
    sprintf("After year filter            : %d", n_retrieved - n_excluded_year),
    if (english_only)
      sprintf("  - Excluded (non-English)   : %d", n_excluded_language)
    else
      sprintf("  Non-English retained       : %d", n_excluded_language),
    sprintf("After language filter        : %d",
            n_retrieved - n_excluded_year - n_excluded_language),
    sprintf("  - Excluded (duplicates)    : %d", n_excluded_dedup),
    paste(rep("-", 40L), collapse = ""),
    sprintf("INCLUDED IN ANALYSIS         : %d", n_final),
    "",
    sprintf("Generated: %s UTC",
            format(Sys.time(), tz = "UTC", format = "%Y-%m-%d %H:%M:%S"))
  )
  writeLines(lines, txt_path)

  .log_step(sprintf(
    "[REPRO] CONSORT flow: %d retrieved -> %d final (%d excluded total) -> %s",
    n_retrieved, n_final,
    n_retrieved - n_final,
    txt_path
  ), verbose)

  invisible(flow)
}

# ---- Feature 7: renv lockfile snapshot ----
#' @noRd
.snapshot_renv <- function(output_dir, verbose = TRUE) {
  if (!requireNamespace("renv", quietly = TRUE)) {
    .log_step(
      "[REPRO] renv not installed -- skipping lockfile snapshot.",
      verbose
    )
    return(invisible(NULL))
  }

  lock_path <- file.path(output_dir, "renv_lockfile.json")

  result <- tryCatch({
    # renv::snapshot writes to renv.lock in the project directory by default.
    # We capture the lock content programmatically to write to output_dir.
    lock_content <- renv::lockfile_create(type = "implicit")
    renv::lockfile_write(lock_content, file = lock_path)
    .log_step(sprintf("[REPRO] renv lockfile written: %s", lock_path), verbose)
    lock_path
  }, error = function(e) {
    # Graceful fallback: write package versions manually
    si    <- utils::sessionInfo()
    pkgs  <- c(si$otherPkgs, si$loadedOnly)
    versions <- lapply(pkgs, function(p)
      list(Package = p$Package, Version = p$Version, Source = "CRAN"))
    fallback <- list(
      note     = "renv::lockfile_create failed; package list from sessionInfo()",
      packages = versions
    )
    jsonlite::write_json(fallback, lock_path, pretty = TRUE, auto_unbox = TRUE)
    .log_step(sprintf(
      "[REPRO] renv snapshot fallback (sessionInfo) -> %s: %s",
      lock_path, e$message
    ), verbose)
    lock_path
  })

  invisible(result)
}

# ============================================================
# MAIN COMPARISON RUNNER
# ============================================================

#' Run Comparative Subspecialty Bibliometric Analysis
#'
#' @description
#' Orchestrates a complete comparative bibliometric analysis across
#' multiple OB/GYN subspecialties and/or urology. Accepts a named list
#' of pre-run \code{run_fpmrs_bibliometric_pipeline()} outputs (one per
#' subspecialty) and computes a unified comparison table, overlaid trend
#' data, five comparison figures, and auto-filled abstract text.
#'
#' @param subspecialty_results_list Named list. Each element is the
#'   return value from \code{run_fpmrs_bibliometric_pipeline()} with an
#'   additional \code{subspecialty} character field identifying the
#'   subspecialty name for display (e.g., \code{"FPMRS"},
#'   \code{"Gyn Oncology"}).
#' @param focal_subspecialty Character. The focal subspecialty for
#'   highlighting and abstract generation. Defaults to \code{"FPMRS"}.
#' @param output_dir Character. Directory for saved figures. Defaults
#'   to \code{"figures/comparison/"}.
#' @param year_start Integer. First year of the analysis window, used
#'   in abstract text. Defaults to \code{2000L}.
#' @param year_end Integer. Last year of the analysis window. Defaults
#'   to the current year.
#' @param figure_format Character. One of \code{"pdf"}, \code{"png"},
#'   \code{"svg"}. Defaults to \code{"pdf"}.
#' @param figure_width Numeric. Width in inches. Defaults to \code{9}.
#' @param figure_height Numeric. Height in inches. Defaults to
#'   \code{6}.
#' @param log_scale_trends Logical. Use log10 y-axis for the overlay
#'   trend plot. Recommended when Urology is included. Defaults to
#'   \code{TRUE}.
#' @param verbose Logical. Print progress messages. Defaults to
#'   \code{TRUE}.
#'
#' @return A named list with elements:
#' \describe{
#'   \item{\code{comparison_table}}{17-column tibble with one row per
#'     subspecialty, all metrics and ranks.}
#'   \item{\code{overlaid_trends}}{Long-format annual trends tibble for
#'     all subspecialties combined.}
#'   \item{\code{figures}}{Named list of five \code{ggplot2} objects.}
#'   \item{\code{figure_paths}}{Named character vector of saved figure
#'     paths.}
#'   \item{\code{abstract_text}}{Named list of auto-filled abstract
#'     sentences.}
#' }
#'
#' @examples
#' \dontrun{
#' # Step 1: Run pipeline for each subspecialty
#' fpmrs_result <- run_fpmrs_bibliometric_pipeline(
#'   data_source  = "pubmed",
#'   pubmed_query = get_subspecialty_pubmed_query("fpmrs"),
#'   output_dir   = "figures/fpmrs/",
#'   year_start   = 1975L, year_end = 2023L, verbose = FALSE
#' )
#' fpmrs_result$subspecialty <- "FPMRS"
#'
#' gyn_onc_result <- run_fpmrs_bibliometric_pipeline(
#'   data_source  = "pubmed",
#'   pubmed_query = get_subspecialty_pubmed_query("gyn_onc"),
#'   output_dir   = "figures/gyn_onc/",
#'   year_start   = 1975L, year_end = 2023L, verbose = FALSE
#' )
#' gyn_onc_result$subspecialty <- "Gyn Oncology"
#'
#' # Step 2: Run comparison
#' comparison <- run_subspecialty_comparison(
#'   subspecialty_results_list = list(
#'     fpmrs   = fpmrs_result,
#'     gyn_onc = gyn_onc_result
#'   ),
#'   focal_subspecialty = "FPMRS",
#'   output_dir         = "figures/comparison/",
#'   year_start         = 1975L,
#'   year_end           = 2023L,
#'   verbose            = TRUE
#' )
#'
#' # Step 3: View results
#' print(comparison$comparison_table)
#' comparison$figures$volume_comparison
#' cat(comparison$abstract_text$corpus)
#' cat(comparison$abstract_text$urology)
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag is.number
#' @importFrom purrr map_dfr imap_dfr walk2 iwalk
#' @importFrom dplyr filter pull arrange desc mutate case_when
#'   n_distinct
#' @importFrom scales label_comma
#' @export
run_subspecialty_comparison <- function(
    subspecialty_results_list,
    focal_subspecialty = "FPMRS",
    output_dir         = "figures/comparison/",
    year_start                = 1975L,
    year_end           = as.integer(format(Sys.Date(), "%Y")),
    figure_format      = "pdf",
    figure_width       = 9,
    figure_height      = 6,
    log_scale_trends   = TRUE,
    compute_statistics = TRUE,
    alpha              = 0.05,
    correction_method  = "BH",
    verbose            = TRUE
) {
  # ---- Validate ----
  assertthat::assert_that(is.list(subspecialty_results_list))
  assertthat::assert_that(
    length(subspecialty_results_list) >= 2L,
    msg = "Provide at least 2 subspecialty results."
  )
  assertthat::assert_that(assertthat::is.string(focal_subspecialty))
  assertthat::assert_that(assertthat::is.string(output_dir))
  assertthat::assert_that(
    nchar(stringr::str_trim(output_dir)) > 0,
    msg = "`output_dir` must be non-empty."
  )
  assertthat::assert_that(
    isTRUE(verbose) || isFALSE(verbose),
    msg = "`verbose` must be TRUE or FALSE."
  )
  assertthat::assert_that(
    figure_format %in% c("pdf", "png", "svg"),
    msg = paste("`figure_format` must be 'pdf', 'png', or 'svg'.",
                "Received:", figure_format)
  )
  assertthat::assert_that(
    is.finite(figure_width) && figure_width > 0,
    msg = "`figure_width` must be a finite positive number."
  )
  assertthat::assert_that(
    is.finite(figure_height) && figure_height > 0,
    msg = "`figure_height` must be a finite positive number."
  )
  assertthat::assert_that(
    is.numeric(year_start) && length(year_start) == 1L &&
      is.finite(year_start),
    msg = "`year_start` must be a single finite numeric value."
  )
  assertthat::assert_that(
    is.numeric(year_end) && length(year_end) == 1L &&
      is.finite(year_end),
    msg = "`year_end` must be a single finite numeric value."
  )
  assertthat::assert_that(
    year_start < year_end,
    msg = sprintf(
      "`year_start` (%g) must be less than `year_end` (%g).",
      year_start, year_end
    )
  )
  if (figure_format == "svg") {
    assertthat::assert_that(
      requireNamespace("svglite", quietly = TRUE),
      msg = paste(
        "SVG output requires 'svglite'. Install with:",
        "install.packages('svglite')"
      )
    )
  }

  if (verbose) {
    message("================================================")
    message("  Subspecialty Comparison Pipeline")
    message("================================================")
    message(sprintf(
      "  [INPUT]  Subspecialties      : %d",
      length(subspecialty_results_list)
    ))
    message(sprintf(
      "  [INPUT]  Focal subspecialty  : %s", focal_subspecialty
    ))
    message(sprintf(
      "  [INPUT]  Year range          : %d-%d", year_start, year_end
    ))
  }

  # ---- Output dir ----
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    .log_step(sprintf(
      "[DIR] Created: %s", normalizePath(output_dir)
    ), verbose)
  }

  # ---- Step 1: Comparison table ----
  .log_step("\n--- STEP 1: Build Comparison Table ---", verbose)
  comparison_summary_table <- .build_comparison_summary_table(
    subspecialty_results_list = subspecialty_results_list,
    verbose                   = verbose
  )

  # ---- Step 2: Overlaid trends ----
  .log_step("\n--- STEP 2: Build Overlaid Annual Trends ---", verbose)
  overlaid_annual_trends <- .build_overlaid_annual_trends(
    subspecialty_results_list = subspecialty_results_list,
    verbose                   = verbose
  )

  # ---- Step 3: Figures ----
  .log_step("\n--- STEP 3: Generate Comparison Figures ---", verbose)

  fig_volume <- plot_subspecialty_volume_comparison(
    comparison_summary_table = comparison_summary_table,
    highlight_subspecialty   = focal_subspecialty,
    verbose                  = verbose
  )
  fig_citations <- plot_subspecialty_citation_comparison(
    comparison_summary_table = comparison_summary_table,
    highlight_subspecialty   = focal_subspecialty,
    verbose                  = verbose
  )
  fig_trends <- plot_subspecialty_trends_overlay(
    overlaid_annual_trends = overlaid_annual_trends,
    log_scale              = log_scale_trends,
    highlight_subspecialty = focal_subspecialty,
    verbose                = verbose
  )
  fig_cagr <- plot_subspecialty_cagr_comparison(
    comparison_summary_table = comparison_summary_table,
    highlight_subspecialty   = focal_subspecialty,
    verbose                  = verbose
  )
  fig_heatmap <- plot_subspecialty_heatmap(
    comparison_summary_table = comparison_summary_table,
    verbose                  = verbose
  )

  comparison_figures <- list(
    volume_comparison    = fig_volume,
    citation_comparison  = fig_citations,
    trends_overlay       = fig_trends,
    cagr_comparison      = fig_cagr,
    performance_heatmap  = fig_heatmap
  )

  # ---- Step 4: Save figures ----
  .log_step("\n--- STEP 4: Save Comparison Figures ---", verbose)
  comparison_figure_paths <- .save_all_figures(
    figures_list  = comparison_figures,
    output_dir    = output_dir,
    figure_format = figure_format,
    figure_width  = figure_width,
    figure_height = figure_height,
    verbose       = verbose
  )

  # ---- Step 5: Abstract text ----
  .log_step("\n--- STEP 5: Generate Abstract Text ---", verbose)
  # Extract institution and keyword data for focal subspecialty
  focal_sp_key <- names(subspecialty_results_list)[
    purrr::map_lgl(
      subspecialty_results_list,
      ~ .x$subspecialty == focal_subspecialty
    )
  ]
  focal_inst_metrics <- if (length(focal_sp_key) > 0L) {
    subspecialty_results_list[[focal_sp_key[1L]]]$institution_metrics
  } else {
    NULL
  }
  focal_kw_trends <- if (length(focal_sp_key) > 0L) {
    subspecialty_results_list[[focal_sp_key[1L]]]$keyword_trends
  } else {
    NULL
  }
  focal_mk_trend <- if (length(focal_sp_key) > 0L) {
    subspecialty_results_list[[focal_sp_key[1L]]]$mk_trend
  } else {
    NULL
  }
  focal_disruption_index <- if (length(focal_sp_key) > 0L) {
    subspecialty_results_list[[focal_sp_key[1L]]]$disruption_index
  } else {
    NULL
  }
  focal_equity_metrics <- if (length(focal_sp_key) > 0L) {
    subspecialty_results_list[[focal_sp_key[1L]]]$equity_metrics
  } else {
    NULL
  }
  focal_evidence_quality <- if (length(focal_sp_key) > 0L) {
    subspecialty_results_list[[focal_sp_key[1L]]]$evidence_quality
  } else {
    NULL
  }

  # ---- Step 5b: Statistical tests (optional, before abstract) ----
  statistics_results <- if (isTRUE(compute_statistics)) {
    .log_step(
      "\n--- STEP 5b: Compute Comparison Statistics ---", verbose
    )
    tryCatch(
      compute_comparison_statistics(
        subspecialty_results_list = subspecialty_results_list,
        focal_subspecialty        = focal_subspecialty,
        alpha                     = alpha,
        correction_method         = correction_method,
        verbose                   = verbose
      ),
      error = function(e) {
        .log_step(sprintf(
          "[STATS] Statistics computation failed: %s", e$message
        ), verbose)
        NULL
      }
    )
  } else {
    NULL
  }

  # ---- Step 6: Abstract text (uses statistics if computed) ----
  abstract_text <- generate_abstract_results_text(
    comparison_summary_table  = comparison_summary_table,
    focal_subspecialty        = focal_subspecialty,
    focal_institution_metrics = focal_inst_metrics,
    focal_keyword_trends      = focal_kw_trends,
    focal_mk_trend            = focal_mk_trend,
    focal_disruption_index    = focal_disruption_index,
    focal_equity_metrics      = focal_equity_metrics,
    focal_evidence_quality    = focal_evidence_quality,
    data_source               = "PubMed",
    statistics_results        = statistics_results,
    year_start                = year_start,
    year_end                  = year_end,
    verbose                   = verbose
  )

  # ---- Output log ----
  if (verbose) {
    message("\n================================================")
    message("  Comparison Complete")
    message("================================================")
    purrr::walk(
      seq_len(nrow(comparison_summary_table)),
      function(i) {
        row <- comparison_summary_table[i, ]
        message(sprintf(
          "  %-28s %s docs | rank #%d | CAGR %.1f%%",
          row$subspecialty,
          scales::label_comma()(row$total_documents),
          row$rank_by_volume,
          row$cagr_pct
        ))
      }
    )
    if (!is.null(statistics_results)) {
      message(sprintf(
        "  [OUTPUT] KW p-value           : %s",
        statistics_results$kruskal_wallis$p_fmt
      ))
      n_sig <- sum(
        statistics_results$pairwise_citations$significant, na.rm = TRUE
      )
      message(sprintf(
        "  [OUTPUT] Pairwise sig (p<%g)  : %d/%d vs focal",
        alpha, n_sig,
        nrow(statistics_results$pairwise_citations)
      ))
    }
    message(sprintf(
      "\n  Figures saved to: %s", normalizePath(output_dir)
    ))
    message("================================================")
  }

  return(list(
    comparison_table  = comparison_summary_table,
    overlaid_trends   = overlaid_annual_trends,
    figures           = comparison_figures,
    figure_paths      = comparison_figure_paths,
    abstract_text     = abstract_text,
    statistics        = statistics_results
  ))
}


# ============================================================
# EXPORTED PLOT FUNCTIONS
# ============================================================

#' Plot Annual Publication Volume with LOESS Trend
#'
#' @description
#' Renders a bar chart of publication counts by year overlaid with a LOESS
#' smoothing curve. Intended as Figure 1 in a longitudinal bibliometric
#' manuscript. Produced by \code{run_fpmrs_bibliometric_pipeline()}.
#'
#' @param annual_publication_trends A tibble returned by the pipeline
#'   containing columns \code{publication_year} and
#'   \code{publication_count}.
#' @param year_start Integer. Minimum year for the x-axis.
#' @param year_end Integer. Maximum year for the x-axis.
#' @param bar_color Character. Hex color for bars. Defaults to
#'   \code{"#2C7BB6"}.
#' @param trend_color Character. Hex color for LOESS curve. Defaults to
#'   \code{"#D7191C"}.
#' @param verbose Logical. Print progress messages. Defaults to
#'   \code{TRUE}.
#'
#' @return A \code{ggplot2} plot object.
#'
#' @importFrom assertthat assert_that is.string is.flag is.number
#' @importFrom ggplot2 ggplot aes geom_col geom_smooth
#'   scale_x_continuous scale_y_continuous expansion labs
#' @importFrom rlang .data
#' @export
plot_annual_publications <- function(
    annual_publication_trends,
    year_start  = 1975L,
    year_end    = as.integer(format(Sys.Date(), "%Y")),
    bar_color   = "#2C7BB6",
    trend_color = "#D7191C",
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_publication_trends))
  assertthat::assert_that(
    all(c("publication_year", "publication_count") %in%
      names(annual_publication_trends)),
    msg = paste(
      "annual_publication_trends must contain",
      "'publication_year' and 'publication_count' columns."
    )
  )
  assertthat::assert_that(assertthat::is.string(bar_color))
  assertthat::assert_that(assertthat::is.string(trend_color))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step("[PLOT] Building annual publications bar chart ...", verbose)

  figure_annual <- ggplot2::ggplot(
    data = annual_publication_trends,
    ggplot2::aes(
      x = .data$publication_year,
      y = .data$publication_count
    )
  ) +
    ggplot2::geom_col(
      fill  = bar_color,
      alpha = 0.85,
      width = 0.8
    ) +
    ggplot2::geom_smooth(
      method    = "loess",
      formula   = y ~ x,
      se        = TRUE,
      color     = trend_color,
      fill      = trend_color,
      alpha     = 0.15,
      linewidth = 1.1
    ) +
    ggplot2::scale_x_continuous(
      breaks = seq(year_start, year_end, by = 5),
      limits = c(year_start - 0.6, year_end + 0.6),
      expand = c(0, 0)
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.08))
    ) +
    ggplot2::labs(
      title    = "Annual FPMRS Publication Volume",
      subtitle = sprintf(
        "%d-%d  |  LOESS trend with 95%% CI shown in red",
        year_start, year_end
      ),
      x       = "Publication Year",
      y       = "Number of Publications",
      caption = paste0(
        "Source: Bibliometric analysis of FPMRS literature. ",
        "LOESS = locally estimated scatterplot smoothing."
      )
    ) +
    .theme_fpmrs_manuscript()

  .log_step("[PLOT] Annual publications figure complete.", verbose)
  return(figure_annual)
}


#' Plot Citation Impact Trends Over Time (Two-Panel)
#'
#' @description
#' Renders a two-panel figure: (top) total citations received by year;
#' (bottom) mean citations per paper by year. Requires the
#' \code{patchwork} package for panel assembly.
#'
#' @param annual_publication_trends A tibble from the pipeline containing
#'   \code{publication_year}, \code{total_citations}, and
#'   \code{mean_citations}.
#' @param year_start Integer. Minimum year for the x-axis.
#' @param year_end Integer. Maximum year for the x-axis.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{patchwork} / \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom ggplot2 ggplot aes geom_area geom_line geom_point
#'   scale_x_continuous scale_y_continuous expansion labs theme element_text
#' @importFrom patchwork wrap_plots plot_annotation
#' @importFrom scales label_comma number_format
#' @importFrom rlang .data
#' @export
plot_citation_trends <- function(
    annual_publication_trends,
    year_start = 1975L,
    year_end   = as.integer(format(Sys.Date(), "%Y")),
    verbose    = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_publication_trends))
  assertthat::assert_that(
    all(c("publication_year", "total_citations", "mean_citations") %in%
      names(annual_publication_trends)),
    msg = paste(
      "annual_publication_trends must contain 'publication_year',",
      "'total_citations', and 'mean_citations' columns."
    )
  )
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step("[PLOT] Building citation trends two-panel figure ...", verbose)

  panel_total_citations <- ggplot2::ggplot(
    data = annual_publication_trends,
    ggplot2::aes(
      x = .data$publication_year,
      y = .data$total_citations
    )
  ) +
    ggplot2::geom_area(fill = "#74C476", alpha = 0.45) +
    ggplot2::geom_line(color = "#238B45", linewidth = 1.1) +
    ggplot2::scale_x_continuous(
      breaks = seq(year_start, year_end, by = 5)
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.08))
    ) +
    ggplot2::labs(
      title = "A. Total Citations Received per Year",
      x     = NULL,
      y     = "Total Citations"
    ) +
    .theme_fpmrs_manuscript()

  panel_mean_citations <- ggplot2::ggplot(
    data = annual_publication_trends,
    ggplot2::aes(
      x = .data$publication_year,
      y = .data$mean_citations
    )
  ) +
    ggplot2::geom_line(color = "#756BB1", linewidth = 1.1) +
    ggplot2::geom_point(
      color = "#756BB1",
      size  = 2.4,
      shape = 19
    ) +
    ggplot2::scale_x_continuous(
      breaks = seq(year_start, year_end, by = 5)
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_number(accuracy = 0.1),
      expand = ggplot2::expansion(mult = c(0, 0.08))
    ) +
    ggplot2::labs(
      title   = "B. Mean Citations per Paper by Year",
      x       = "Publication Year",
      y       = "Mean Citations",
      caption = paste0(
        "Note: recent years naturally accumulate fewer citations ",
        "due to shorter citation windows."
      )
    ) +
    .theme_fpmrs_manuscript()

  combined_citation_figure <- patchwork::wrap_plots(
    panel_total_citations,
    panel_mean_citations,
    ncol = 1
  ) +
    patchwork::plot_annotation(
      title    = "FPMRS Citation Impact Over Time",
      subtitle = sprintf("%d-%d", year_start, year_end),
      theme    = ggplot2::theme(
        plot.title    = ggplot2::element_text(
          face = "bold", size = 14
        ),
        plot.subtitle = ggplot2::element_text(
          color = "grey40", size = 10
        )
      )
    )

  .log_step("[PLOT] Citation trends figure complete (2 panels).", verbose)
  return(combined_citation_figure)
}


#' Plot Keyword Evolution Heatmap Over Time
#'
#' @description
#' Renders a tile heatmap showing frequency of the top N author keywords
#' across years. Cell color encodes keyword frequency using the viridis
#' plasma palette.
#'
#' @param keyword_evolution_trends A tibble from the pipeline with columns
#'   \code{publication_year}, \code{keyword}, \code{keyword_count}.
#' @param top_n_keywords Integer. Number of keywords to display ranked by
#'   total frequency. Defaults to \code{20L}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr group_by summarise arrange slice_head pull filter mutate
#' @importFrom ggplot2 ggplot aes geom_tile scale_fill_viridis_c labs
#'   theme element_text unit element_blank
#' @importFrom rlang .data
#' @export
plot_keyword_evolution <- function(
    keyword_evolution_trends,
    top_n_keywords = 20L,
    verbose        = TRUE
) {
  assertthat::assert_that(is.data.frame(keyword_evolution_trends))
  assertthat::assert_that(assertthat::is.count(top_n_keywords))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[PLOT] Building keyword evolution heatmap (top %d) ...",
    top_n_keywords
  ), verbose)

  if (nrow(keyword_evolution_trends) == 0) {
    .log_step("[PLOT] Empty keyword data -- returning placeholder figure.",
      verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(
          title    = "Keyword Evolution Over Time",
          subtitle = "No keyword data available for this query."
        ) +
        .theme_fpmrs_manuscript()
    )
  }

  top_keywords_by_frequency <- keyword_evolution_trends |>
    dplyr::group_by(.data$keyword) |>
    dplyr::summarise(
      total_frequency = sum(.data$keyword_count),
      .groups         = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$total_frequency)) |>
    dplyr::slice_head(n = top_n_keywords) |>
    dplyr::pull(.data$keyword)

  .log_step(sprintf(
    "[PLOT] Top %d keywords (by total freq): %s ...",
    top_n_keywords,
    paste(utils::head(top_keywords_by_frequency, 4), collapse = ", ")
  ), verbose)

  heatmap_data <- keyword_evolution_trends |>
    dplyr::filter(.data$keyword %in% top_keywords_by_frequency) |>
    dplyr::mutate(
      keyword = factor(
        .data$keyword,
        levels = rev(top_keywords_by_frequency)
      )
    )

  figure_keyword_heatmap <- ggplot2::ggplot(
    data = heatmap_data,
    ggplot2::aes(
      x    = .data$publication_year,
      y    = .data$keyword,
      fill = .data$keyword_count
    )
  ) +
    ggplot2::geom_tile(color = "white", linewidth = 0.25) +
    ggplot2::scale_fill_viridis_c(
      option   = "plasma",
      name     = "Frequency",
      na.value = "grey96"
    ) +
    ggplot2::labs(
      title    = "Keyword Evolution Over Time",
      subtitle = sprintf(
        "Top %d author keywords by cumulative frequency | ",
        top_n_keywords
      ),
      x        = "Publication Year",
      y        = NULL,
      caption  = "DE = author-assigned keywords; ID = keyword-plus terms."
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      axis.text.y      = ggplot2::element_text(size = 7.5),
      legend.key.width = ggplot2::unit(1.8, "cm")
    )

  .log_step("[PLOT] Keyword evolution heatmap complete.", verbose)
  return(figure_keyword_heatmap)
}


#' Plot Geographic Distribution of Publications Over Time
#'
#' @description
#' Renders a stacked area chart showing the top N contributing countries'
#' publication output across years. One article contributes one count per
#' unique country regardless of how many authors are from that country.
#'
#' @param country_contribution_trends A tibble from the pipeline with
#'   columns \code{publication_year}, \code{country},
#'   \code{publication_count}.
#' @param top_n_countries Integer. Number of countries to display.
#'   Defaults to \code{10L}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr group_by summarise arrange slice_head pull filter mutate
#' @importFrom ggplot2 ggplot aes geom_area scale_fill_viridis_d
#'   scale_y_continuous expansion labs
#' @importFrom rlang .data
#' @export
plot_country_contributions <- function(
    country_contribution_trends,
    top_n_countries = 10L,
    verbose         = TRUE
) {
  assertthat::assert_that(is.data.frame(country_contribution_trends))
  assertthat::assert_that(assertthat::is.count(top_n_countries))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[PLOT] Building country contributions figure (top %d) ...",
    top_n_countries
  ), verbose)

  if (nrow(country_contribution_trends) == 0) {
    .log_step("[PLOT] Empty country data -- returning placeholder.", verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(
          title    = "Geographic Distribution of FPMRS Publications",
          subtitle = "No country data available (AU_CO column absent)."
        ) +
        .theme_fpmrs_manuscript()
    )
  }

  top_countries_by_volume <- country_contribution_trends |>
    dplyr::group_by(.data$country) |>
    dplyr::summarise(
      total_publications = sum(.data$publication_count),
      .groups            = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$total_publications)) |>
    dplyr::slice_head(n = top_n_countries) |>
    dplyr::pull(.data$country)

  .log_step(sprintf(
    "[PLOT] Top countries: %s",
    paste(top_countries_by_volume, collapse = ", ")
  ), verbose)

  stacked_area_data <- country_contribution_trends |>
    dplyr::filter(.data$country %in% top_countries_by_volume) |>
    dplyr::mutate(
      country = factor(
        .data$country,
        levels = rev(top_countries_by_volume)
      )
    )

  figure_country <- ggplot2::ggplot(
    data = stacked_area_data,
    ggplot2::aes(
      x    = .data$publication_year,
      y    = .data$publication_count,
      fill = .data$country
    )
  ) +
    ggplot2::geom_area(position = "stack", alpha = 0.88) +
    ggplot2::scale_fill_viridis_d(
      option    = "turbo",
      name      = "Country",
      direction = -1
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.05))
    ) +
    ggplot2::labs(
      title    = "Geographic Distribution of FPMRS Publications",
      subtitle = sprintf(
        "Top %d countries (stacked; one count per article per country)",
        top_n_countries
      ),
      x        = "Publication Year",
      y        = "Number of Publications",
      caption  = "AU_CO = country of corresponding/first author affiliation."
    ) +
    .theme_fpmrs_manuscript()

  .log_step("[PLOT] Country contributions figure complete.", verbose)
  return(figure_country)
}


#' Plot Journal Dominance Trends Over Time
#'
#' @description
#' Renders a stacked bar chart showing publication share across the top N
#' journals over time. Useful for tracking which outlets dominate FPMRS
#' dissemination and how that has shifted across decades.
#'
#' @param journal_trends A tibble from the pipeline with columns
#'   \code{publication_year}, \code{journal}, \code{publication_count}.
#' @param top_n_journals Integer. Number of journals to display. Defaults
#'   to \code{8L}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr filter mutate
#' @importFrom ggplot2 ggplot aes geom_col scale_fill_brewer
#'   scale_y_continuous expansion labs
#' @importFrom rlang .data
#' @export
plot_journal_trends <- function(
    journal_trends,
    top_n_journals = 8L,
    verbose        = TRUE
) {
  assertthat::assert_that(is.data.frame(journal_trends))
  assertthat::assert_that(assertthat::is.count(top_n_journals))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[PLOT] Building journal trends figure (top %d) ...", top_n_journals
  ), verbose)

  if (nrow(journal_trends) == 0) {
    .log_step("[PLOT] Empty journal data -- returning placeholder.", verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(
          title    = "Journal Dominance Over Time",
          subtitle = "No journal data available."
        ) +
        .theme_fpmrs_manuscript()
    )
  }

  top_journals_vector <- journal_trends |>
    dplyr::group_by(.data$journal) |>
    dplyr::summarise(
      total_publications = sum(.data$publication_count),
      .groups            = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$total_publications)) |>
    dplyr::slice_head(n = top_n_journals) |>
    dplyr::pull(.data$journal)

  journal_stacked_data <- journal_trends |>
    dplyr::filter(.data$journal %in% top_journals_vector) |>
    dplyr::mutate(
      journal = stringr::str_wrap(.data$journal, width = 35),
      journal = factor(.data$journal)
    )

  figure_journal <- ggplot2::ggplot(
    data = journal_stacked_data,
    ggplot2::aes(
      x    = .data$publication_year,
      y    = .data$publication_count,
      fill = .data$journal
    )
  ) +
    ggplot2::geom_col(position = "stack", width = 0.85) +
    ggplot2::scale_fill_brewer(
      palette = "Set2",
      name    = "Journal"
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.06))
    ) +
    ggplot2::labs(
      title    = "Journal Dominance in FPMRS Literature Over Time",
      subtitle = sprintf(
        "Top %d journals by total publication volume (stacked)",
        top_n_journals
      ),
      x        = "Publication Year",
      y        = "Number of Publications"
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      legend.text = ggplot2::element_text(size = 7)
    )

  .log_step("[PLOT] Journal trends figure complete.", verbose)
  return(figure_journal)
}



# ============================================================
# ERA-BASED AND SCIENCE-MAPPING PLOT FUNCTIONS (exported)
# ============================================================

#' Plot Thematic Evolution Across Publication Eras (Slope/Bump Chart)
#'
#' @description
#' Renders a slope chart showing how the top keywords' rank within each
#' publication era shifted across four time windows
#' (2000--2007, 2008--2013, 2014--2018, 2019+).
#' Each line tracks one keyword; a rise indicates growing prominence.
#' Keywords are labelled on the left (first era) and right (last era)
#' margins. Only keywords present in at least two eras are shown.
#'
#' @param thematic_evolution_data A tibble from
#'   \code{.compute_thematic_evolution()} with columns
#'   \code{era}, \code{keyword}, \code{n_papers}, \code{pct_of_era}.
#' @param top_n Integer. Maximum number of keywords to trace.
#'   Selected by highest average \code{pct_of_era} across all eras.
#'   Defaults to \code{12L}.
#' @param highlight_keywords Character vector of keyword strings to
#'   draw in a saturated accent colour. All others are grey.
#'   Defaults to \code{NULL} (all coloured by viridis).
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr group_by summarise arrange desc slice_head
#'   filter mutate left_join n_distinct
#' @importFrom tidyr complete
#' @importFrom ggplot2 ggplot aes geom_line geom_point geom_text
#'   scale_color_viridis_d scale_y_reverse scale_x_discrete
#'   labs theme element_text element_blank margin expansion
#' @importFrom rlang .data
#' @export
plot_thematic_evolution <- function(
    thematic_evolution_data,
    top_n               = 12L,
    highlight_keywords  = NULL,
    verbose             = TRUE
) {
  assertthat::assert_that(is.data.frame(thematic_evolution_data))
  assertthat::assert_that(assertthat::is.count(top_n))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  assertthat::assert_that(
    all(c("era","keyword","n_papers","pct_of_era") %in%
          names(thematic_evolution_data)),
    msg = paste(
      "`thematic_evolution_data` must contain 'era', 'keyword',",
      "'n_papers', 'pct_of_era'. Pass output of",
      ".compute_thematic_evolution() directly."
    )
  )

  .log_step(
    sprintf("[PLOT] Building thematic evolution slope chart (top %d) ...",
            top_n),
    verbose
  )

  if (nrow(thematic_evolution_data) == 0L) {
    .log_step("[PLOT] Empty thematic evolution data -- placeholder.", verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(title = "Thematic Evolution",
                      subtitle = "No keyword data available.") +
        .theme_fpmrs_manuscript()
    )
  }

  # Derive era order dynamically from the data itself.
  # IMPORTANT: do NOT use a hardcoded intersect list — make_eras()
  # produces different era names depending on year_start (e.g. it
  # generates "2008\u20132018" for pre-1990 corpora, but "2008\u20132012"
  # for 2000-start corpora). A hardcoded list silently drops any era
  # name it does not recognise, causing entire decades to vanish from
  # the figure.
  era_order <- if (nrow(thematic_evolution_data) > 0L) {
    eras_present <- unique(thematic_evolution_data$era)
    # Sort by the four-digit year embedded at the start of each label
    era_start_yr <- suppressWarnings(
      as.integer(stringr::str_extract(eras_present, "^[0-9]{4}"))
    )
    eras_present[order(era_start_yr, na.last = TRUE)]
  } else character(0L)
  era_labels <- era_order


  # Select top_n keywords by mean pct_of_era across all eras they appear in
  top_kws <- thematic_evolution_data |>
    dplyr::group_by(.data$keyword) |>
    dplyr::summarise(
      mean_pct    = mean(.data$pct_of_era, na.rm = TRUE),
      n_eras_seen = dplyr::n_distinct(.data$era),
      .groups     = "drop"
    ) |>
    dplyr::filter(.data$n_eras_seen >= 2L) |>
    dplyr::arrange(dplyr::desc(.data$mean_pct)) |>
    dplyr::slice_head(n = top_n) |>
    dplyr::pull(.data$keyword)

  if (length(top_kws) == 0L) {
    .log_step("[PLOT] No keywords appear in 2+ eras -- placeholder.", verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(title = "Thematic Evolution",
                      subtitle = "Insufficient cross-era keyword overlap.") +
        .theme_fpmrs_manuscript()
    )
  }

  # Build rank within each era (1 = highest pct_of_era)
  ranked <- thematic_evolution_data |>
    dplyr::filter(.data$keyword %in% top_kws) |>
    dplyr::mutate(era = factor(.data$era, levels = era_order)) |>
    dplyr::group_by(.data$era) |>
    dplyr::mutate(
      rank_in_era = rank(-.data$pct_of_era, ties.method = "first")
    ) |>
    dplyr::ungroup()

  # Colour scheme: highlight vs viridis
  all_kws_ordered <- top_kws
  if (!is.null(highlight_keywords)) {
    assertthat::assert_that(
      is.character(highlight_keywords),
      msg = "`highlight_keywords` must be a character vector or NULL."
    )
    kw_colour <- ifelse(
      all_kws_ordered %in% highlight_keywords,
      "#D7191C", "#BDBDBD"
    )
    names(kw_colour) <- all_kws_ordered
    colour_scale <- ggplot2::scale_color_manual(
      values = kw_colour, guide = "none"
    )
  } else {
    colour_scale <- ggplot2::scale_color_viridis_d(
      option = "turbo", guide = "none"
    )
  }

  # Edge labels: first and last era
  # Use match() against ordered levels for x-offset — avoids the fragile
  # as.numeric(factor) assumption that level index == axis position.
  first_era  <- era_order[1L]
  last_era   <- era_order[length(era_order)]
  n_eras     <- length(era_order)

  label_left <- ranked |>
    dplyr::filter(.data$era == first_era) |>
    dplyr::mutate(
      label_text = stringr::str_to_title(.data$keyword),
      era_num    = match(first_era, era_order)
    )
  label_right <- ranked |>
    dplyr::filter(.data$era == last_era) |>
    dplyr::mutate(
      label_text = stringr::str_to_title(.data$keyword),
      era_num    = match(last_era, era_order)
    )

  fig <- ggplot2::ggplot(
    ranked,
    ggplot2::aes(
      x     = .data$era,
      y     = .data$rank_in_era,
      group = .data$keyword,
      color = .data$keyword
    )
  ) +
    ggplot2::geom_line(linewidth = 1.1, alpha = 0.75) +
    ggplot2::geom_point(size = 3.2, alpha = 0.9) +
    # Left labels — offset left of first era tick
    ggplot2::geom_text(
      data  = label_left,
      ggplot2::aes(
        x     = .data$era_num - 0.08,
        y     = .data$rank_in_era,
        label = .data$label_text,
        color = .data$keyword
      ),
      hjust = 1, size = 2.8, show.legend = FALSE
    ) +
    # Right labels — offset right of last era tick
    ggplot2::geom_text(
      data  = label_right,
      ggplot2::aes(
        x     = .data$era_num + 0.08,
        y     = .data$rank_in_era,
        label = .data$label_text,
        color = .data$keyword
      ),
      hjust = 0, size = 2.8, show.legend = FALSE
    ) +
    colour_scale +
    ggplot2::scale_y_reverse(
      breaks = seq_len(top_n),
      expand = ggplot2::expansion(add = 1.5)
    ) +
    ggplot2::scale_x_discrete(
      labels = era_labels,
      expand = ggplot2::expansion(add = c(1.6, 1.6))
    ) +
    ggplot2::labs(
      title    = "Thematic Evolution of FPMRS Literature",
      subtitle = sprintf(
        "Top %d keywords ranked by prevalence within each era | Rank 1 = most frequent",
        top_n
      ),
      x        = "Publication Era",
      y        = "Rank Within Era (1 = most frequent)",
      caption  = paste(
        "Keywords present in fewer than 2 eras excluded.",
        "Rank computed within each era independently."
      )
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      panel.grid.major.x = ggplot2::element_blank(),
      axis.text.x        = ggplot2::element_text(face = "bold", size = 10)
    )

  .log_step("[PLOT] Thematic evolution slope chart complete.", verbose)
  return(fig)
}


#' Plot Evidence Pyramid by Publication Era
#'
#' @description
#' Stacked 100\\% bar chart showing the proportion of each study type
#' across four publication eras. Study types are ordered by the evidence
#' hierarchy (RCT and meta-analyses at top, editorials at bottom).
#' Useful for detecting whether a field's evidence base is maturing
#' toward higher-quality designs over time.
#'
#' @param evidence_evolution_data A tibble from
#'   \code{.compute_evidence_evolution()} with columns
#'   \code{era}, \code{study_type}, \code{n_papers},
#'   \code{pct_of_total}, \code{n_era_papers}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr mutate filter left_join
#' @importFrom ggplot2 ggplot aes geom_col geom_text position_stack
#'   scale_fill_manual scale_y_continuous expansion coord_flip labs
#' @importFrom rlang .data
#' @export
plot_evidence_evolution <- function(
    evidence_evolution_data,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(evidence_evolution_data))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  assertthat::assert_that(
    all(c("era","study_type","n_papers","pct_of_total") %in%
          names(evidence_evolution_data)),
    msg = paste(
      "`evidence_evolution_data` must contain 'era', 'study_type',",
      "'n_papers', 'pct_of_total'."
    )
  )

  .log_step("[PLOT] Building evidence pyramid by era ...", verbose)

  if (nrow(evidence_evolution_data) == 0L) {
    .log_step("[PLOT] Empty evidence evolution data -- placeholder.", verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(title = "Evidence Pyramid by Era",
                      subtitle = "No study type data available.") +
        .theme_fpmrs_manuscript()
    )
  }

  # Evidence hierarchy: higher rank = higher quality = plotted at top
  evidence_order <- c(
    "RCT",
    "Meta-Analysis / Systematic Review",
    "Multicenter Study",
    "Guideline",
    "Clinical Trial (Non-RCT)",
    "Observational / Comparative",
    "Narrative Review",
    "Original Article",
    "Case Report",
    "Editorial / Comment / Letter",
    "Other",
    "Unknown"
  )
  evidence_colours <- c(
    "RCT"                               = "#005A32",
    "Meta-Analysis / Systematic Review" = "#238B45",
    "Multicenter Study"                 = "#41AB5D",
    "Guideline"                         = "#74C476",
    "Clinical Trial (Non-RCT)"          = "#A1D99B",
    "Observational / Comparative"       = "#C7E9C0",
    "Narrative Review"                  = "#FDCC8A",
    "Original Article"                  = "#FC8D59",
    "Case Report"                       = "#D7301F",
    "Editorial / Comment / Letter"      = "#7F0000",
    "Other"                             = "#BDBDBD",
    "Unknown"                           = "#969696"
  )

  # Derive era order dynamically from the data itself.
  # IMPORTANT: do NOT use a hardcoded intersect list — make_eras()
  # produces different era names depending on year_start (e.g. it
  # generates "2008\u20132018" for pre-1990 corpora, but "2008\u20132012"
  # for 2000-start corpora). A hardcoded list silently drops any era
  # name it does not recognise, causing entire decades to vanish from
  # the figure.
  era_order <- if (nrow(evidence_evolution_data) > 0L) {
    eras_present <- unique(evidence_evolution_data$era)
    # Sort by the four-digit year embedded at the start of each label
    era_start_yr <- suppressWarnings(
      as.integer(stringr::str_extract(eras_present, "^[0-9]{4}"))
    )
    eras_present[order(era_start_yr, na.last = TRUE)]
  } else character(0L)

  plot_data <- evidence_evolution_data |>
    dplyr::mutate(
      era        = factor(.data$era, levels = era_order),
      study_type = factor(
        .data$study_type,
        levels = rev(evidence_order)   # rev so RCT plots on top
      )
    ) |>
    dplyr::filter(!is.na(.data$study_type))

  # Label text: show pct only if >= 8% to avoid overplotting
  plot_data <- plot_data |>
    dplyr::mutate(
      label_txt = dplyr::if_else(
        .data$pct_of_total >= 8,
        sprintf("%.0f%%", .data$pct_of_total),
        ""
      )
    )

  # Build n_era_papers annotation for subtitle (column may be absent
  # if caller constructed the table manually rather than via
  # .compute_evidence_evolution()).
  era_n_label <- if ("n_era_papers" %in% names(evidence_evolution_data)) {
    era_ns <- evidence_evolution_data |>
      dplyr::group_by(.data$era) |>
      dplyr::summarise(
        n = dplyr::first(.data$n_era_papers), .groups = "drop"
      ) |>
      dplyr::arrange(match(.data$era, era_order))
    paste(
      paste0(era_ns$era, " (n=", scales::label_comma()(era_ns$n), ")"),
      collapse = "; "
    )
  } else {
    paste(unique(as.character(evidence_evolution_data$era)), collapse = " | ")
  }

  present_types <- intersect(
    levels(plot_data$study_type),
    unique(as.character(plot_data$study_type))
  )

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x    = .data$era,
      y    = .data$pct_of_total,
      fill = .data$study_type
    )
  ) +
    ggplot2::geom_col(
      position = ggplot2::position_stack(reverse = FALSE),
      width    = 0.72
    ) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$label_txt),
      position = ggplot2::position_stack(vjust = 0.5, reverse = FALSE),
      size     = 2.9,
      color    = "white",
      fontface = "bold"
    ) +
    ggplot2::scale_fill_manual(
      values = evidence_colours[present_types],
      name   = "Study Type",
      breaks = rev(present_types)   # legend top = highest evidence
    ) +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      expand = ggplot2::expansion(mult = c(0, 0.04))
    ) +
    ggplot2::labs(
      title    = "Evidence Pyramid by Publication Era",
      subtitle = era_n_label,
      x        = "Publication Era",
      y        = "Percentage of Publications",
      caption  = paste(
        "Study type classified from PubMed PT field; titles with",
        "'multicenter' upgraded from 'Original Article' where applicable."
      )
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      legend.key.size = ggplot2::unit(0.45, "cm"),
      legend.text     = ggplot2::element_text(size = 7.5)
    )

  .log_step("[PLOT] Evidence pyramid by era complete.", verbose)
  return(fig)
}


#' Plot Geographic Equity: World Bank Income Tier Breakdown
#'
#' @description
#' Horizontal stacked-bar chart showing the share of publications
#' attributed to countries in each World Bank income tier (High,
#' Upper-Middle, Lower-Middle, Low). The Herfindahl-Hirschman Index
#' (HHI) is annotated to show country-level concentration.
#'
#' Accepts either a single-row \code{equity_metrics} tibble from one
#' subspecialty, or a named list of such tibbles (one per subspecialty)
#' for a multi-bar comparison.
#'
#' @param equity_metrics_input Either a 1-row data frame from
#'   \code{.compute_equity_metrics()}, or a named list of such data
#'   frames (one per subspecialty). Must contain
#'   \code{pct_high_income}, \code{pct_upper_mid},
#'   \code{pct_lower_mid}, \code{pct_low_income},
#'   \code{pct_unmapped}, \code{concentration_hhi}.
#' @param subspecialty_label Character. Used as the y-axis label when
#'   \code{equity_metrics_input} is a single data frame.
#'   Defaults to \code{"FPMRS"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr mutate bind_rows tibble
#' @importFrom tidyr pivot_longer
#' @importFrom ggplot2 ggplot aes geom_col geom_text position_stack
#'   scale_fill_manual scale_x_continuous expansion annotate labs
#' @importFrom rlang .data
#' @export
plot_equity_breakdown <- function(
    equity_metrics_input,
    subspecialty_label = "FPMRS",
    verbose            = TRUE
) {
  assertthat::assert_that(
    is.data.frame(equity_metrics_input) || is.list(equity_metrics_input),
    msg = "`equity_metrics_input` must be a data frame or named list."
  )
  assertthat::assert_that(assertthat::is.string(subspecialty_label))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  .log_step("[PLOT] Building geographic equity breakdown ...", verbose)

  required_cols <- c("pct_high_income","pct_upper_mid","pct_lower_mid",
                     "pct_low_income","pct_unmapped","concentration_hhi")

  # Normalise to a data frame with a subspecialty column
  if (is.data.frame(equity_metrics_input)) {
    assertthat::assert_that(
      all(required_cols %in% names(equity_metrics_input)),
      msg = paste("Single data frame missing columns:",
                  paste(setdiff(required_cols,
                                names(equity_metrics_input)),
                        collapse = ", "))
    )
    equity_tbl <- equity_metrics_input |>
      dplyr::mutate(subspecialty = subspecialty_label)
  } else {
    # Named list of data frames
    assertthat::assert_that(
      length(equity_metrics_input) > 0L,
      msg = "`equity_metrics_input` is an empty list. Pass at least one entry."
    )
    assertthat::assert_that(
      !is.null(names(equity_metrics_input)) &&
        all(nchar(trimws(names(equity_metrics_input))) > 0L),
      msg = "List input must be a **named** list with non-empty names."
    )
    equity_tbl <- purrr::imap_dfr(equity_metrics_input, function(df, nm) {
      assertthat::assert_that(
        is.data.frame(df),
        msg = sprintf("Entry '%s' is not a data frame.", nm)
      )
      assertthat::assert_that(
        all(required_cols %in% names(df)),
        msg = sprintf("Entry '%s' missing columns: %s", nm,
                      paste(setdiff(required_cols, names(df)),
                            collapse = ", "))
      )
      dplyr::mutate(df, subspecialty = nm)
    })
  }

  assertthat::assert_that(
    nrow(equity_tbl) > 0L,
    msg = "No rows remain after constructing equity table."
  )

  # Clamp each pct column to [0, 100] to absorb floating-point rounding
  # errors (e.g. 82.4 + 5.8 + 11.8 + 0.0 + 0.1 = 100.1) that would
  # otherwise push bars beyond the y-axis limit and trigger ggplot2 clipping.
  pct_cols <- c("pct_high_income","pct_upper_mid","pct_lower_mid",
                "pct_low_income","pct_unmapped")
  equity_tbl <- equity_tbl |>
    dplyr::mutate(dplyr::across(
      dplyr::all_of(pct_cols),
      ~ pmin(pmax(.x, 0), 100)
    ))

  tier_colours <- c(
    "High income"         = "#2166AC",
    "Upper-middle income" = "#74ADD1",
    "Lower-middle income" = "#FEE090",
    "Low income"          = "#D73027",
    "Unmapped"            = "#BDBDBD"
  )

  # Pivot to long for stacked bar
  plot_data <- equity_tbl |>
    dplyr::select(
      "subspecialty",
      "High income"         = "pct_high_income",
      "Upper-middle income" = "pct_upper_mid",
      "Lower-middle income" = "pct_lower_mid",
      "Low income"          = "pct_low_income",
      "Unmapped"            = "pct_unmapped",
      "concentration_hhi"
    ) |>
    tidyr::pivot_longer(
      cols      = c("High income","Upper-middle income",
                    "Lower-middle income","Low income","Unmapped"),
      names_to  = "income_tier",
      values_to = "pct"
    ) |>
    dplyr::mutate(
      income_tier  = factor(
        .data$income_tier,
        levels = c("High income","Upper-middle income",
                   "Lower-middle income","Low income","Unmapped")
      ),
      label_txt = dplyr::if_else(
        .data$pct >= 6,
        sprintf("%.0f%%", .data$pct),
        ""
      )
    )

  # HHI annotation data (one label per subspecialty bar)
  hhi_labels <- equity_tbl |>
    dplyr::mutate(
      hhi_label = sprintf("HHI=%.3f", .data$concentration_hhi)
    )

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x    = .data$subspecialty,
      y    = .data$pct,
      fill = .data$income_tier
    )
  ) +
    ggplot2::geom_col(
      position = ggplot2::position_stack(reverse = TRUE),
      width    = 0.65
    ) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$label_txt),
      position = ggplot2::position_stack(vjust = 0.5, reverse = TRUE),
      size     = 3.0,
      color    = "white",
      fontface = "bold"
    ) +
    ggplot2::geom_text(
      data = hhi_labels,
      ggplot2::aes(
        x     = .data$subspecialty,
        y     = 102,
        label = .data$hhi_label
      ),
      inherit.aes = FALSE,
      size        = 2.8,
      color       = "grey30",
      hjust       = 0.5
    ) +
    ggplot2::scale_fill_manual(
      values = tier_colours,
      name   = "World Bank\nIncome Tier"
    ) +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      limits = c(0, 108),
      expand = ggplot2::expansion(mult = c(0, 0))
    ) +
    ggplot2::coord_flip() +
    ggplot2::labs(
      title    = "Geographic Equity: Publication Distribution by Income Tier",
      subtitle = paste(
        "Lead country per paper classified by World Bank FY2024 income tier.",
        "HHI = Herfindahl-Hirschman Index (0=dispersed, 1=monopoly)."
      ),
      x        = NULL,
      y        = "Percentage of Publications",
      caption  = paste(
        "Source: AU_CO lead-country field from bibliometrix export.",
        "World Bank FY2024 classification applied."
      )
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      legend.key.size = ggplot2::unit(0.5, "cm"),
      legend.text     = ggplot2::element_text(size = 8)
    )

  .log_step("[PLOT] Geographic equity breakdown complete.", verbose)
  return(fig)
}


#' Plot Disruption Index Distribution
#'
#' @description
#' Histogram with density overlay showing the distribution of local
#' Disruption Index (DI) values across all papers in the corpus.
#' A vertical dashed line marks DI = 0 (neutral boundary between
#' disruptive and consolidating). A solid line marks the median DI.
#' An annotation box reports the percentage disruptive (DI > 0),
#' percentage consolidating (DI < 0), and the median value.
#'
#' @param disruption_index_data A 1-row tibble from
#'   \code{.compute_disruption_index()} containing the
#'   \code{di_values} list column, \code{median_di},
#'   \code{pct_disruptive}, and \code{pct_consolidating}.
#' @param subspecialty_label Character. Used in the title.
#'   Defaults to \code{"FPMRS"}.
#' @param n_bins Integer. Number of histogram bins.
#'   Defaults to \code{40L}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object.
#'
#' @importFrom assertthat assert_that is.string is.count is.flag
#' @importFrom dplyr tibble
#' @importFrom ggplot2 ggplot aes geom_histogram geom_density
#'   geom_vline annotate scale_x_continuous scale_y_continuous
#'   expansion labs after_stat
#' @importFrom rlang .data
#' @export
plot_disruption_index_distribution <- function(
    disruption_index_data,
    subspecialty_label = "FPMRS",
    n_bins             = 40L,
    verbose            = TRUE
) {
  assertthat::assert_that(is.data.frame(disruption_index_data))
  assertthat::assert_that(assertthat::is.string(subspecialty_label))
  assertthat::assert_that(assertthat::is.count(n_bins))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  assertthat::assert_that(
    "di_values" %in% names(disruption_index_data),
    msg = paste(
      "`disruption_index_data` must contain a 'di_values' list column.",
      "Pass the output of .compute_disruption_index() directly."
    )
  )
  assertthat::assert_that(
    all(c("median_di","pct_disruptive","pct_consolidating",
          "n_papers_total") %in% names(disruption_index_data)),
    msg = paste(
      "`disruption_index_data` is missing required columns.",
      "Expected: median_di, pct_disruptive, pct_consolidating, n_papers_total.",
      "Pass the output of .compute_disruption_index() directly."
    )
  )
  assertthat::assert_that(
    nrow(disruption_index_data) > 0L,
    msg = paste(
      "`disruption_index_data` has zero rows.",
      "The DI compute function always returns at least 1 row."
    )
  )
  assertthat::assert_that(
    is.list(disruption_index_data$di_values),
    msg = paste(
      "Column 'di_values' must be a list column.",
      "It may have been coerced to a plain vector during data manipulation.",
      "Pass the output of .compute_disruption_index() without modification."
    )
  )

  .log_step(
    sprintf("[PLOT] Building DI distribution histogram (%s) ...",
            subspecialty_label),
    verbose
  )

  di_vec     <- disruption_index_data$di_values[[1L]]
  di_valid   <- di_vec[!is.na(di_vec) & is.finite(di_vec)]
  n_valid    <- length(di_valid)
  median_val <- disruption_index_data$median_di[[1L]]

  if (n_valid < 5L) {
    .log_step("[PLOT] Fewer than 5 valid DI values -- placeholder.", verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(
          title    = sprintf("Disruption Index: %s", subspecialty_label),
          subtitle = "Insufficient within-corpus citations to estimate DI."
        ) +
        .theme_fpmrs_manuscript()
    )
  }

  pct_dis  <- disruption_index_data$pct_disruptive[[1L]]
  pct_con  <- disruption_index_data$pct_consolidating[[1L]]
  n_papers <- disruption_index_data$n_papers_total[[1L]]

  di_df <- dplyr::tibble(di = di_valid)

  # Annotation text box
  annot_text <- sprintf(
    "Disruptive (>0): %.1f%%\nConsolidating (<0): %.1f%%\nMedian DI: %.3f\nn = %s papers",
    dplyr::coalesce(pct_dis,  0),
    dplyr::coalesce(pct_con,  0),
    dplyr::coalesce(median_val, 0),
    scales::label_comma()(n_valid)
  )

  fig <- ggplot2::ggplot(di_df, ggplot2::aes(x = .data$di)) +
    ggplot2::geom_histogram(
      ggplot2::aes(y = ggplot2::after_stat(density)),
      bins     = n_bins,
      fill     = "#6BAED6",
      color    = "white",
      alpha    = 0.80,
      linewidth = 0.25
    ) +
    ggplot2::geom_density(
      color     = "#08519C",
      linewidth = 1.1,
      adjust    = 1.2
    ) +
    # DI = 0 boundary
    ggplot2::geom_vline(
      xintercept = 0,
      linetype   = "dashed",
      color      = "#636363",
      linewidth  = 0.9
    ) +
    # Median DI
    ggplot2::geom_vline(
      xintercept = dplyr::coalesce(median_val, 0),
      linetype   = "solid",
      color      = "#D7191C",
      linewidth  = 1.1
    ) +
    ggplot2::annotate(
      "text",
      x     = 0.98,
      y     = Inf,
      label = annot_text,
      hjust = 1, vjust = 1.15,
      size  = 3.0,
      color = "grey20",
      family = "sans"
    ) +
    ggplot2::annotate(
      "text",
      x = 0.03, y = Inf,
      label = "Disruptive \u2192",
      hjust = 0, vjust = 1.8,
      size = 3.0, color = "#08519C"
    ) +
    ggplot2::annotate(
      "text",
      x = -0.03, y = Inf,
      label = "\u2190 Consolidating",
      hjust = 1, vjust = 1.8,
      size = 3.0, color = "#6a3d9a"
    ) +
    ggplot2::scale_x_continuous(
      limits = c(-1, 1),
      breaks = seq(-1, 1, by = 0.25),
      expand = ggplot2::expansion(mult = c(0.01, 0.01))
    ) +
    ggplot2::scale_y_continuous(
      expand = ggplot2::expansion(mult = c(0, 0.15))
    ) +
    ggplot2::labs(
      title    = sprintf("Disruption Index Distribution: %s Literature",
                         subspecialty_label),
      subtitle = sprintf(
        "Local DI (within-corpus) | Red line = median (%.3f) | Dashed = DI 0",
        dplyr::coalesce(median_val, 0)
      ),
      x        = "Disruption Index (DI)",
      y        = "Density",
      caption  = paste(
        "Local DI computed from within-corpus CR field citations only.",
        "Positive DI = subsequent papers ignore focal paper's references (disruptive).",
        "Negative DI = subsequent papers also cite focal paper's references (consolidating)."
      )
    ) +
    .theme_fpmrs_manuscript()

  .log_step("[PLOT] DI distribution histogram complete.", verbose)
  return(fig)
}


#' Plot Citation Impact Metrics by Publication Era
#'
#' @description
#' Three-panel connected dot plot showing how the h-index, median
#' citation velocity (citations/year), and citation Gini coefficient
#' evolved across four publication eras. Papers in each era are
#' evaluated as a self-contained cohort: velocity reflects the age
#' of papers published in that window, so the 2019+ cohort will
#' naturally show higher velocity due to shorter citation windows.
#'
#' @param bibliography A data frame containing at minimum \code{TC}
#'   (times cited, character or numeric) and
#'   \code{publication_year} (integer). The same tibble returned
#'   by \code{run_fpmrs_bibliometric_pipeline()$bibliography}.
#' @param eras A named list of integer vectors \code{c(start, end)}.
#'   Defaults to the standard four-era breakdown.
#' @param subspecialty_label Character. Used in the title.
#'   Defaults to \code{"FPMRS"}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{patchwork} object (three vertically stacked panels).
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr filter tibble mutate
#' @importFrom purrr map_dfr
#' @importFrom ggplot2 ggplot aes geom_line geom_point geom_text
#'   scale_x_discrete scale_y_continuous expansion labs
#' @importFrom patchwork wrap_plots plot_annotation
#' @importFrom rlang .data
#' @export
plot_citation_metrics_by_era <- function(
    bibliography,
    eras = list(
      "2000\u20132007" = c(2000L, 2007L),
      "2008\u20132013" = c(2008L, 2013L),
      "2014\u20132018" = c(2014L, 2018L),
      "2019+"          = c(2019L, 2100L)
    ),
    subspecialty_label = "FPMRS",
    verbose            = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(
    is.list(eras) && length(eras) >= 2L,
    msg = "`eras` must be a named list of at least 2 elements."
  )
  assertthat::assert_that(
    !is.null(names(eras)) &&
      all(nchar(trimws(names(eras))) > 0L),
    msg = paste(
      "`eras` must be a **named** list.",
      "Each name becomes the x-axis label for that era.",
      "Example: list('2000-2007'=c(2000L,2007L), '2008-2013'=c(2008L,2013L))"
    )
  )
  purrr::walk2(names(eras), eras, function(nm, yr) {
    assertthat::assert_that(
      is.numeric(yr) && length(yr) == 2L,
      msg = sprintf(
        "Era '%s' must be a numeric vector of length 2 (start, end). Got length %d.",
        nm, length(yr)
      )
    )
    assertthat::assert_that(
      is.finite(yr[1L]) && is.finite(yr[2L]) && yr[1L] < yr[2L],
      msg = sprintf(
        "Era '%s': start year (%g) must be strictly less than end year (%g).",
        nm, yr[1L], yr[2L]
      )
    )
  })
  assertthat::assert_that(assertthat::is.string(subspecialty_label))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  assertthat::assert_that(
    "publication_year" %in% names(bibliography),
    msg = "`bibliography` must contain 'publication_year'."
  )

  .log_step(
    sprintf("[PLOT] Building citation metrics by era (%s) ...",
            subspecialty_label),
    verbose
  )

  era_names <- names(eras)

  era_metrics <- purrr::map_dfr(era_names, function(era_nm) {
    yr      <- eras[[era_nm]]
    era_bib <- dplyr::filter(
      bibliography,
      .data$publication_year >= yr[1L],
      .data$publication_year <= yr[2L]
    )
    n_papers <- nrow(era_bib)
    if (n_papers == 0L) {
      return(dplyr::tibble(
        era              = era_nm,
        n_papers         = 0L,
        h_index          = NA_integer_,
        velocity_median  = NA_real_,
        citation_gini    = NA_real_
      ))
    }
    cm <- .compute_citation_metrics(era_bib, verbose = FALSE)
    dplyr::tibble(
      era             = era_nm,
      n_papers        = n_papers,
      h_index         = cm$h_index,
      velocity_median = cm$citation_velocity_median,
      citation_gini   = cm$citation_gini
    )
  }) |>
    dplyr::mutate(
      # Pin factor levels to era_names order (insertion order of the list).
      # Do NOT use unique(.data$era_label) — that would honour arrival order
      # from map_dfr which is already correct, but is fragile if a caller
      # passes an unsorted list. Explicit levelling on era_names is safe.
      era = factor(.data$era, levels = era_names),
      era_label = paste0(
        .data$era, "\n(n=",
        scales::label_comma()(.data$n_papers), ")"
      ),
      era_label = factor(
        .data$era_label,
        levels = paste0(
          era_names, "\n(n=",
          scales::label_comma()(.data$n_papers), ")"
        )
      )
    )

  .log_step(sprintf(
    "[PLOT] Era metrics: h=(%s)  vel=(%s)",
    paste(era_metrics$h_index, collapse=","),
    paste(round(era_metrics$velocity_median, 1), collapse=",")
  ), verbose)

  .make_era_panel <- function(data, y_col, y_label,
                               point_color, caption_txt = NULL) {
    ggplot2::ggplot(
      data,
      ggplot2::aes(x = .data$era_label, y = .data[[y_col]],
                   group = 1)
    ) +
      ggplot2::geom_line(
        color     = point_color,
        linewidth = 1.1,
        na.rm     = TRUE
      ) +
      ggplot2::geom_point(
        color = point_color,
        size  = 4.0,
        shape = 19,
        na.rm = TRUE
      ) +
      ggplot2::geom_text(
        ggplot2::aes(
          label = dplyr::if_else(
            is.na(.data[[y_col]]),
            "NA",
            as.character(round(.data[[y_col]], 2))
          )
        ),
        vjust = -1.1,
        size  = 3.2,
        color = "grey30",
        na.rm = FALSE
      ) +
      ggplot2::scale_x_discrete(drop = FALSE) +
      ggplot2::scale_y_continuous(
        expand = ggplot2::expansion(mult = c(0.15, 0.25))
      ) +
      ggplot2::labs(
        x       = NULL,
        y       = y_label,
        caption = caption_txt
      ) +
      .theme_fpmrs_manuscript() +
      ggplot2::theme(
        axis.text.x = ggplot2::element_text(size = 8.5)
      )
  }

  panel_h <- .make_era_panel(
    era_metrics, "h_index", "h-index",
    "#2C7BB6"
  )
  panel_vel <- .make_era_panel(
    era_metrics, "velocity_median", "Median Citations/Year",
    "#1A9641",
    paste(
      "Velocity = TC / years since publication.",
      "2019+ cohort shows higher velocity due to shorter citation window."
    )
  )
  panel_gini <- .make_era_panel(
    era_metrics, "citation_gini", "Citation Gini",
    "#7B2D8B"
  )

  combined <- patchwork::wrap_plots(
    panel_h, panel_vel, panel_gini,
    ncol = 1
  ) +
    patchwork::plot_annotation(
      title    = sprintf(
        "Citation Impact Metrics by Era: %s Literature",
        subspecialty_label
      ),
      subtitle = paste(
        "Each era evaluated independently.",
        "h-index = Hirsch index | Gini = citation inequality (0=equal, 1=concentrated)"
      ),
      theme    = ggplot2::theme(
        plot.title    = ggplot2::element_text(
          face = "bold", size = 13
        ),
        plot.subtitle = ggplot2::element_text(
          color = "grey40", size = 9
        )
      )
    )

  .log_step("[PLOT] Citation metrics by era complete (3 panels).", verbose)
  return(combined)
}

# ============================================================
# MAIN EXPORTED PIPELINE FUNCTION
# ============================================================

#' Run the FPMRS Longitudinal Bibliometric Analysis Pipeline
#'
#' @description
#' A full manuscript-ready bibliometric analysis pipeline for Female Pelvic
#' Medicine and Reconstructive Surgery (FPMRS) and related urogynecology
#' literature. The function orchestrates data ingestion from PubMed, Web of
#' Science, or Scopus; cleans and filters the bibliography to a specified
#' year range; computes longitudinal trend tables (annual publications,
#' citations, keyword evolution, country contributions, journal dominance);
#' and generates five publication-quality \code{ggplot2} figures that are
#' saved to disk in the caller-specified format.
#'
#' All intermediate computations are logged to the console when
#' \code{verbose = TRUE}. No log file is written.
#'
#' @param data_source Character string. Data origin. Must be one of
#'   \code{"pubmed"}, \code{"wos"} (Web of Science plaintext export),
#'   or \code{"scopus"} (Scopus CSV export).
#' @param file_path Character string or \code{NULL}. Absolute or relative
#'   path to a bibliography export file. Required when
#'   \code{data_source \%in\% c("wos", "scopus")}; ignored for
#'   \code{"pubmed"}.
#' @param pubmed_query Character string or \code{NULL}. A valid PubMed
#'   search query string using standard PubMed syntax (MeSH terms,
#'   field tags, Boolean operators). Required when
#'   \code{data_source == "pubmed"}; ignored otherwise.
#' @param pubmed_api_key Character string or \code{NULL}. Optional NCBI
#'   API key to increase the PubMed rate limit from 3 to 10 requests/sec.
#'   Obtain at \url{https://www.ncbi.nlm.nih.gov/account/}. Defaults to
#'   \code{NULL}.
#' @param output_dir Character string. Directory where all figures will be
#'   saved. Created recursively if it does not exist. Defaults to
#'   \code{"figures/"}.
#' @param year_start Integer. First calendar year included in the analysis
#'   window (inclusive). Must be less than \code{year_end}. Defaults to
#'   \code{2000L}.
#' @param year_end Integer. Last calendar year included in the analysis
#'   window (inclusive). Cannot exceed the current calendar year. Defaults
#'   to the current year.
#' @param keyword_column Character string. Which bibliometrix keyword column
#'   to use for keyword analysis. \code{"DE"} = author-assigned keywords;
#'   \code{"ID"} = Keywords Plus (WoS) or indexed terms (Scopus). Falls
#'   back automatically if the specified column is absent. Defaults to
#'   \code{"DE"}.
#' @param top_n_keywords Integer. Number of keywords to display in the
#'   keyword evolution heatmap, ranked by total frequency. Defaults to
#'   \code{20L}.
#' @param top_n_countries Integer. Number of countries to display in the
#'   geographic contributions figure. Defaults to \code{10L}.
#' @param top_n_journals Integer. Number of journals to display in the
#'   journal dominance figure. Defaults to \code{8L}.
#' @param figure_format Character string. Output format for all saved
#'   figures. One of \code{"pdf"}, \code{"png"}, or \code{"svg"}.
#'   \code{"pdf"} is recommended for journal submission. Defaults to
#'   \code{"pdf"}.
#' @param figure_width Numeric. Figure width in inches. Defaults to
#'   \code{7}.
#' @param figure_height Numeric. Figure height in inches. Defaults to
#'   \code{5}.
#' @param verbose Logical. If \code{TRUE}, prints timestamped progress
#'   messages at every pipeline step. Defaults to \code{TRUE}.
#'
#' @return A named list with the following elements:
#' \describe{
#'   \item{\code{bibliography}}{Standardized bibliographic data frame
#'     filtered to \code{year_start}-\code{year_end}, as produced by
#'     \code{bibliometrix::convert2df()}.}
#'   \item{\code{bibliometric_summary}}{An object of class
#'     \code{bibliometrix} produced by
#'     \code{bibliometrix::biblioAnalysis()}, containing author,
#'     journal, citation, and network statistics.}
#'   \item{\code{annual_trends}}{A tibble with one row per year
#'     containing \code{publication_count}, \code{total_citations},
#'     \code{mean_citations}, \code{median_citations},
#'     \code{cumulative_publications}, and
#'     \code{cumulative_citations}.}
#'   \item{\code{keyword_trends}}{A tibble of keyword frequencies
#'     by year (\code{keyword_evolution_trends}).}
#'   \item{\code{country_trends}}{A tibble of country publication
#'     counts by year.}
#'   \item{\code{journal_trends}}{A tibble of top-journal publication
#'     counts by year.}
#'   \item{\code{figures}}{Named list of five \code{ggplot2} objects:
#'     \code{annual_publications}, \code{citation_trends},
#'     \code{keyword_evolution}, \code{country_contributions},
#'     \code{journal_trends}.}
#'   \item{\code{figure_paths}}{Named character vector mapping each
#'     figure name to its absolute file path on disk.}
#' }
#'
#' @examples
#' # Example 1: PubMed -- full FPMRS longitudinal analysis, PDF output
#' \dontrun{
#' fpmrs_pipeline_output <- run_fpmrs_bibliometric_pipeline(
#'   data_source     = "pubmed",
#'   pubmed_query    = paste0(
#'     "female pelvic medicine[MeSH Terms] OR ",
#'     "urogynecology[Title/Abstract] OR ",
#'     "FPMRS[Title/Abstract] OR ",
#'     "pelvic organ prolapse[MeSH Terms]"
#'   ),
#'   pubmed_api_key  = Sys.getenv("NCBI_API_KEY"),
#'   output_dir      = "output/fpmrs_bibliometrics/",
#'   year_start      = 1975L,
#'   year_end        = 2023L,
#'   keyword_column  = "DE",
#'   top_n_keywords  = 20L,
#'   top_n_countries = 10L,
#'   top_n_journals  = 8L,
#'   figure_format   = "pdf",
#'   figure_width    = 7,
#'   figure_height   = 5,
#'   verbose         = TRUE
#' )
#' # Inspect annual trends table
#' print(fpmrs_pipeline_output$annual_trends)
#' # View the publication volume figure
#' fpmrs_pipeline_output$figures$annual_publications
#' # Check where figures were saved
#' print(fpmrs_pipeline_output$figure_paths)
#' }
#'
#' # Example 2: Web of Science export -- PNG for slides, quiet mode
#' \dontrun{
#' wos_pipeline_output <- run_fpmrs_bibliometric_pipeline(
#'   data_source     = "wos",
#'   file_path       = "data/raw/wos_urogynecology_2000_2023.txt",
#'   pubmed_query    = NULL,
#'   pubmed_api_key  = NULL,
#'   output_dir      = "output/wos_figures/",
#'   year_start      = 2005L,
#'   year_end        = 2023L,
#'   keyword_column  = "ID",
#'   top_n_keywords  = 15L,
#'   top_n_countries = 8L,
#'   top_n_journals  = 6L,
#'   figure_format   = "png",
#'   figure_width    = 9,
#'   figure_height   = 6,
#'   verbose         = FALSE
#' )
#' # Keyword trends table
#' dplyr::filter(
#'   wos_pipeline_output$keyword_trends,
#'   keyword == "URINARY INCONTINENCE"
#' )
#' # Render citation impact panels
#' wos_pipeline_output$figures$citation_trends
#' }
#'
#' # Example 3: Scopus export -- SVG output, wider keyword view
#' \dontrun{
#' scopus_pipeline_output <- run_fpmrs_bibliometric_pipeline(
#'   data_source     = "scopus",
#'   file_path       = "data/raw/scopus_fpmrs_export.csv",
#'   pubmed_query    = NULL,
#'   pubmed_api_key  = NULL,
#'   output_dir      = "output/scopus_figures/",
#'   year_start      = 1975L,
#'   year_end        = 2022L,
#'   keyword_column  = "DE",
#'   top_n_keywords  = 25L,
#'   top_n_countries = 12L,
#'   top_n_journals  = 10L,
#'   figure_format   = "svg",
#'   figure_width    = 10,
#'   figure_height   = 7,
#'   verbose         = TRUE
#' )
#' # Country contribution data for USA only
#' dplyr::filter(
#'   scopus_pipeline_output$country_trends,
#'   country == "USA"
#' )
#' # Confirm 5 figures were saved
#' length(scopus_pipeline_output$figure_paths)
#' # Open all figures in the viewer
#' purrr::walk(
#'   scopus_pipeline_output$figures,
#'   print
#' )
#' }
#'
#' @importFrom assertthat assert_that is.string is.flag is.count is.number
#' @importFrom dplyr filter mutate select arrange group_by summarise
#'   ungroup left_join rename n n_distinct pull distinct slice_head count
#'   all_of tibble if_else
#' @importFrom tidyr unnest pivot_longer pivot_wider
#' @importFrom stringr str_split str_trim str_to_upper str_wrap
#' @importFrom ggplot2 ggplot aes geom_col geom_line geom_area geom_smooth
#'   geom_point geom_tile scale_x_continuous scale_y_continuous
#'   scale_fill_viridis_c scale_fill_viridis_d scale_fill_brewer
#'   expansion labs theme theme_minimal element_text element_line
#'   element_blank element_rect margin unit ggsave
#' @importFrom patchwork wrap_plots plot_annotation
#' @importFrom scales label_comma label_number
#' @importFrom bibliometrix convert2df biblioAnalysis
#' @importFrom pubmedR pmApiRequest pmApi2df
#' @importFrom purrr imap_chr walk
#' @importFrom rlang .data
#' @importFrom utils head
#' @importFrom stats median
#' @export
run_fpmrs_bibliometric_pipeline <- function(
    data_source     = "pubmed",
    file_path       = NULL,
    pubmed_query    = NULL,
    pubmed_api_key  = NULL,
    output_dir      = "figures/",
    year_start                = 1975L,
    year_end        = as.integer(format(Sys.Date(), "%Y")),
    keyword_column  = "DE",
    top_n_keywords  = 20L,
    top_n_countries = 10L,
    top_n_journals  = 8L,
    top_n_authors   = 20L,
    top_n_institutions = 20L,
    figure_format   = "pdf",
    figure_width    = 7,
    figure_height   = 5,
    english_only    = TRUE,
    seed            = 42L,
    verbose         = TRUE
) {
  # ----------------------------------------------------------
  # 0. Validate all inputs up front
  # ----------------------------------------------------------
  .validate_pipeline_inputs(
    data_source   = data_source,
    file_path     = file_path,
    pubmed_query  = pubmed_query,
    output_dir    = output_dir,
    year_start    = year_start,
    year_end      = year_end,
    figure_format = figure_format,
    figure_width  = figure_width,
    figure_height = figure_height,
    verbose       = verbose
  )

  # ----------------------------------------------------------
  # 0b. Log all inputs
  # ----------------------------------------------------------
  # ---- Feature 4: Set reproducible seed ----
  .set_pipeline_seed(seed, verbose)

  if (verbose) {
    message("================================================")
    message("  FPMRS Longitudinal Bibliometric Pipeline")
    message("================================================")
    .log_input("data_source",     data_source,     verbose)
    .log_input("file_path",       file_path,       verbose)
    .log_input("pubmed_query",    pubmed_query,    verbose)
    .log_input("output_dir",      output_dir,      verbose)
    .log_input("year_start",      year_start,      verbose)
    .log_input("year_end",        year_end,        verbose)
    .log_input("keyword_column",  keyword_column,  verbose)
    .log_input("top_n_keywords",  top_n_keywords,  verbose)
    .log_input("top_n_countries", top_n_countries, verbose)
    .log_input("top_n_journals",  top_n_journals,  verbose)
    .log_input("top_n_authors",   top_n_authors,   verbose)
    .log_input("top_n_institutions", top_n_institutions, verbose)
    .log_input("figure_format",   figure_format,   verbose)
    .log_input("figure_width",    figure_width,    verbose)
    .log_input("figure_height",   figure_height,   verbose)
  }

  # ----------------------------------------------------------
  # 1. Ensure output directory exists
  # ----------------------------------------------------------
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    .log_step(sprintf(
      "\n[DIR] Created output directory: %s",
      normalizePath(output_dir)
    ), verbose)
  } else {
    .log_step(sprintf(
      "\n[DIR] Output directory exists: %s",
      normalizePath(output_dir)
    ), verbose)
  }

  # ----------------------------------------------------------
  # 2. Load and standardize bibliography
  # ----------------------------------------------------------
  .log_step("\n--- STEP 1: Load Bibliography ---", verbose)
  bibliography_raw <- .load_bibliography(
    data_source    = data_source,
    file_path      = file_path,
    pubmed_query   = pubmed_query,
    pubmed_api_key = pubmed_api_key,
    verbose        = verbose
  )

  # ----------------------------------------------------------
  # 3. Filter to year range
  # ----------------------------------------------------------
  .log_step("\n--- STEP 2: Filter by Year Range ---", verbose)
  bibliography_filtered <- .standardize_and_filter_bibliography(
    bibliography_raw = bibliography_raw,
    year_start       = year_start,
    year_end         = year_end,
    english_only     = english_only,
    verbose          = verbose
  )

  # ----------------------------------------------------------
  # 4. Core bibliometric analysis
  # ----------------------------------------------------------
  # ---- Features 2, 3, 6: Search provenance, query hash, CONSORT flow ----
  n_retrieved_total <- nrow(bibliography_raw)
  n_after_year      <- nrow(bibliography_filtered)
  n_excluded_yr     <- n_retrieved_total - n_after_year
  # Language exclusions already happened inside standardize step;
  # count by comparing filtered vs year-only filtered
  n_excluded_lang   <- 0L  # logged inside .standardize_and_filter
  n_excluded_dedup  <- 0L  # logged inside .merge_bibliographies when both
  n_final           <- nrow(bibliography_filtered)

  query_hash <- .compute_query_hash(pubmed_query, data_source,
                                    year_start, year_end)
  .write_query_hash(output_dir, query_hash, verbose)
  .write_search_provenance(
    output_dir          = output_dir,
    data_source         = data_source,
    pubmed_query        = pubmed_query,
    file_path           = file_path,
    year_start          = year_start,
    year_end            = year_end,
    english_only        = english_only,
    n_retrieved         = n_retrieved_total,
    n_after_year_filter = n_after_year,
    n_after_lang_filter = n_final,
    n_final             = n_final,
    verbose             = verbose
  )
  .write_consort_flow(
    output_dir           = output_dir,
    data_source          = data_source,
    n_retrieved          = n_retrieved_total,
    n_excluded_year      = n_excluded_yr,
    n_excluded_language  = n_excluded_lang,
    n_excluded_dedup     = n_excluded_dedup,
    n_final              = n_final,
    year_start           = year_start,
    year_end             = year_end,
    english_only         = english_only,
    verbose              = verbose
  )

  # MeSH coverage check
  if (data_source %in% c("pubmed", "both")) {
    .write_mesh_coverage_note(output_dir, year_start, verbose)
  }

  .log_step("\n--- STEP 3: Core Bibliometric Analysis ---", verbose)
  bibliometric_summary_object <- bibliometrix::biblioAnalysis(
    bibliography_filtered,
    sep = ";"
  )
  .log_step("[STEP 3] bibliometrix::biblioAnalysis() complete.", verbose)

  # ----------------------------------------------------------
  # 5. Compute longitudinal trend tables
  # ----------------------------------------------------------
  .log_step("\n--- STEP 4: Compute Longitudinal Trend Tables ---", verbose)

  annual_publication_trends <- .compute_annual_publication_trends(
    bibliography_filtered = bibliography_filtered,
    verbose               = verbose
  )

  keyword_evolution_trends <- .compute_keyword_evolution(
    bibliography_filtered = bibliography_filtered,
    keyword_column        = keyword_column,
    verbose               = verbose
  )

  country_contribution_trends <- .compute_country_contributions(
    bibliography_filtered = bibliography_filtered,
    verbose               = verbose
  )

  journal_dominance_trends <- .compute_journal_trends(
    bibliography_filtered = bibliography_filtered,
    top_n_journals        = top_n_journals,
    verbose               = verbose
  )

  author_productivity_metrics <- .compute_author_metrics(
    bibliography_filtered = bibliography_filtered,
    top_n_authors         = top_n_authors,
    verbose               = verbose
  )

  institution_productivity_metrics <- .compute_institution_metrics(
    bibliography_filtered  = bibliography_filtered,
    top_n_institutions     = top_n_institutions,
    verbose                = verbose
  )

  journal_citation_impact_metrics <- .compute_journal_citation_metrics(
    bibliography_filtered = bibliography_filtered,
    top_n_journals        = top_n_journals,
    verbose               = verbose
  )

  country_summary_metrics <- .compute_country_summary(
    bibliography_filtered = bibliography_filtered,
    verbose               = verbose
  )

  growth_metrics <- .compute_growth_metrics(
    annual_publication_trends = annual_publication_trends,
    verbose                   = verbose
  )

  # ----------------------------------------------------------
  # 6. Generate figures
  # ----------------------------------------------------------
  .log_step("\n--- STEP 5: Generate Manuscript Figures ---", verbose)

  figure_annual_publications <- plot_annual_publications(
    annual_publication_trends = annual_publication_trends,
    year_start                = year_start,
    year_end                  = year_end,
    verbose                   = verbose
  )

  figure_citation_impact <- plot_citation_trends(
    annual_publication_trends = annual_publication_trends,
    year_start                = year_start,
    year_end                  = year_end,
    verbose                   = verbose
  )

  figure_keyword_evolution <- plot_keyword_evolution(
    keyword_evolution_trends = keyword_evolution_trends,
    top_n_keywords           = top_n_keywords,
    verbose                  = verbose
  )

  figure_country_contributions <- plot_country_contributions(
    country_contribution_trends = country_contribution_trends,
    top_n_countries             = top_n_countries,
    verbose                     = verbose
  )

  figure_journal_dominance <- plot_journal_trends(
    journal_trends = journal_dominance_trends,
    top_n_journals = top_n_journals,
    verbose        = verbose
  )

  pipeline_figures <- list(
    annual_publications   = figure_annual_publications,
    citation_trends       = figure_citation_impact,
    keyword_evolution     = figure_keyword_evolution,
    country_contributions = figure_country_contributions,
    journal_trends        = figure_journal_dominance
  )
  .log_step(sprintf(
    "[STEP 5] %d figures generated.", length(pipeline_figures)
  ), verbose)

  # ----------------------------------------------------------
  # 6b. Extended characterization metrics
  # ----------------------------------------------------------
  .log_step("\n--- STEP 5b: Extended Characterization Metrics ---", verbose)

  authorship_metrics_data   <- .compute_authorship_metrics(
    bibliography_filtered, verbose
  )
  study_type_metrics_data   <- .compute_study_type_metrics(
    bibliography_filtered, verbose
  )
  science_type_metrics_data <- .compute_basic_vs_clinical(
    bibliography_filtered, verbose
  )
  nih_funding_metrics_data  <- .compute_nih_funding_rate(
    bibliography_filtered, verbose
  )
  first_author_gender_data  <- .compute_first_author_gender(
    bibliography_filtered, verbose
  )
  disruption_index_data     <- .compute_disruption_index(
    bibliography_filtered, verbose
  )
  citation_metrics_data     <- .compute_citation_metrics(
    bibliography_filtered, verbose
  )
  intl_collab_data          <- .compute_intl_collaboration(
    bibliography_filtered, verbose
  )
  evidence_quality_data     <- .compute_evidence_quality_score(
    study_type_metrics_data, verbose
  )
  ref_age_metrics_data      <- .compute_reference_age_metrics(
    bibliography_filtered, verbose
  )
  # RCR is opt-in: only called when fetch_rcr = TRUE (future parameter)
  # and httr is available. Default FALSE to keep pipeline offline-capable.
  rcr_metrics_data <- .rcr_na_result(
    nrow(bibliography_filtered), "RCR not fetched (set fetch_rcr=TRUE)"
  )

  # ---- Science mapping (VOSviewer networks + thematic evolution) ----
  .log_step("\n--- STEP 5b: Science Mapping Networks ---", verbose)
  coword_network_matrix <- .compute_coword_network(
    bibliography_filtered,
    keyword_col   = keyword_column,
    min_frequency = 2L,
    verbose       = verbose
  )
  coauthorship_network_matrix <- .compute_coauthorship_network(
    bibliography_filtered,
    unit            = "countries",
    min_occurrences = 2L,
    verbose         = verbose
  )
  cocitation_network_matrix <- .compute_cocitation_network(
    bibliography_filtered,
    top_n_refs = 100L,
    verbose    = verbose
  )
  thematic_evolution_data <- .compute_thematic_evolution(
    bibliography_filtered,
    keyword_col = keyword_column,
    verbose     = verbose
  )
  mk_trend_data <- .compute_mann_kendall_trend(
    annual_publication_trends,
    verbose = verbose
  )
  evidence_evolution_data <- .compute_evidence_evolution(
    bibliography_filtered,
    verbose = verbose
  )
  equity_metrics_data <- .compute_equity_metrics(
    bibliography_filtered,
    verbose = verbose
  )

  # ---- Export VOSviewer files ----
  .log_step("\n--- STEP 5c: Export VOSviewer Files ---", verbose)
  vosviewer_export_paths <- list()
  if (!is.null(coword_network_matrix)) {
    vosviewer_export_paths$coword <- .export_vosviewer_network(
      network_matrix  = coword_network_matrix,
      output_dir      = output_dir,
      file_prefix     = "coword_keyword",
      weight_label    = "Co-occurrences",
      min_edge_weight = 1L,
      verbose         = verbose
    )
  }
  if (!is.null(coauthorship_network_matrix)) {
    vosviewer_export_paths$coauthorship <- .export_vosviewer_network(
      network_matrix  = coauthorship_network_matrix,
      output_dir      = output_dir,
      file_prefix     = "coauth_country",
      weight_label    = "Publications",
      min_edge_weight = 1L,
      verbose         = verbose
    )
  }
  if (!is.null(cocitation_network_matrix)) {
    vosviewer_export_paths$cocitation <- .export_vosviewer_network(
      network_matrix  = cocitation_network_matrix,
      output_dir      = output_dir,
      file_prefix     = "cocitation_refs",
      weight_label    = "Citations",
      min_edge_weight = 1L,
      verbose         = verbose
    )
  }

  if (isTRUE(verbose)) {
    message(sprintf(
      "  [OUTPUT] Mean authors/paper : %.1f",
      dplyr::coalesce(authorship_metrics_data$mean_authors_per_paper, NA_real_)
    ))
    if (nrow(study_type_metrics_data) > 0L) {
      message(sprintf(
        "  [OUTPUT] Top study type     : %s (%.1f%%)",
        study_type_metrics_data$study_type[[1L]],
        study_type_metrics_data$pct_of_total[[1L]]
      ))
    }
    n_female_fa <- if (nrow(first_author_gender_data) > 0L)
      sum(first_author_gender_data$n_papers[
        first_author_gender_data$gender == "Female"], na.rm = TRUE)
    else 0L
    message(sprintf(
      "  [OUTPUT] Female first authors: %d papers", n_female_fa
    ))
    if (!is.na(disruption_index_data$median_di)) {
      message(sprintf(
        "  [OUTPUT] Median DI            : %.3f (%d papers)",
        disruption_index_data$median_di,
        disruption_index_data$n_papers_with_di
      ))
    }
    message(sprintf(
      "  [OUTPUT] h-index              : %d",
      dplyr::coalesce(citation_metrics_data$h_index, NA_integer_)
    ))
    message(sprintf(
      "  [OUTPUT] Citation velocity    : %.2f TC/yr",
      dplyr::coalesce(citation_metrics_data$citation_velocity_median, NA_real_)
    ))
    message(sprintf(
      "  [OUTPUT] Intl collab rate     : %.1f%%",
      dplyr::coalesce(intl_collab_data$pct_intl_collab, NA_real_)
    ))
    message(sprintf(
      "  [OUTPUT] Evidence quality     : %.3f",
      dplyr::coalesce(evidence_quality_data$evidence_quality_score, NA_real_)
    ))
  }

  # ----------------------------------------------------------
  # 7. Save figures to disk
  # ----------------------------------------------------------
  .log_step("\n--- STEP 6: Save Figures to Disk ---", verbose)
  saved_figure_paths <- .save_all_figures(
    figures_list  = pipeline_figures,
    output_dir    = output_dir,
    figure_format = figure_format,
    figure_width  = figure_width,
    figure_height = figure_height,
    verbose       = verbose
  )

  # ----------------------------------------------------------
  # 8. Log outputs
  # ----------------------------------------------------------
  if (verbose) {
    message("\n================================================")
    message("  Pipeline Complete -- Output Summary")
    message("================================================")
    message(sprintf(
      "  [OUTPUT] Records analyzed   : %d",
      nrow(bibliography_filtered)
    ))
    message(sprintf(
      "  [OUTPUT] Year range         : %d-%d",
      year_start, year_end
    ))
    message(sprintf(
      "  [OUTPUT] Unique years       : %d",
      dplyr::n_distinct(bibliography_filtered$publication_year)
    ))
    message(sprintf(
      "  [OUTPUT] Total citations    : %s",
      scales::label_comma()(sum(annual_publication_trends$total_citations))
    ))
    message(sprintf(
      "  [OUTPUT] Unique keywords    : %d",
      dplyr::n_distinct(keyword_evolution_trends$keyword)
    ))
    message(sprintf(
      "  [OUTPUT] Unique countries   : %d",
      dplyr::n_distinct(country_contribution_trends$country)
    ))
    message(sprintf(
      "  [OUTPUT] Top author         : %s (%d pubs)",
      if (nrow(author_productivity_metrics) > 0)
        author_productivity_metrics$author[[1]] else "N/A",
      if (nrow(author_productivity_metrics) > 0)
        author_productivity_metrics$publication_count[[1]] else 0L
    ))
    message(sprintf(
      "  [OUTPUT] Top institution    : %s",
      if (nrow(institution_productivity_metrics) > 0)
        institution_productivity_metrics$institution[[1]] else "N/A"
    ))
    message(sprintf(
      "  [OUTPUT] Top journal (cites): %s",
      if (nrow(journal_citation_impact_metrics) > 0)
        journal_citation_impact_metrics$journal[[1]] else "N/A"
    ))
    message(sprintf(
      "  [OUTPUT] LOESS R2           : %.3f",
      growth_metrics$loess_r_squared
    ))
    message(sprintf(
      "  [OUTPUT] Peak year          : %d",
      growth_metrics$peak_year
    ))
    message(sprintf(
      "  [OUTPUT] CAGR               : %.1f%%",
      growth_metrics$cagr_pct
    ))
    message(sprintf(
      "  [OUTPUT] Figures saved      : %d",
      length(saved_figure_paths)
    ))
    purrr::walk2(
      names(saved_figure_paths), saved_figure_paths,
      ~ message(sprintf("  [OUTPUT]   %-28s: %s", .x, .y))
    )
    message("================================================")
  }

  # ---- Features 1, 5, 7: Session snapshot, manifest, renv ----
  session_snapshot_data <- .write_session_snapshot(output_dir, verbose)
  .snapshot_renv(output_dir, verbose)
  output_manifest_data  <- .write_output_manifest(output_dir, verbose)

  return(list(
    bibliography          = bibliography_filtered,
    bibliometric_summary  = bibliometric_summary_object,
    annual_trends         = annual_publication_trends,
    keyword_trends        = keyword_evolution_trends,
    country_trends        = country_contribution_trends,
    journal_trends        = journal_dominance_trends,
    author_metrics        = author_productivity_metrics,
    institution_metrics   = institution_productivity_metrics,
    journal_citations     = journal_citation_impact_metrics,
    country_summary       = country_summary_metrics,
    growth_metrics        = growth_metrics,
    authorship_metrics    = authorship_metrics_data,
    study_type_metrics    = study_type_metrics_data,
    science_type_metrics  = science_type_metrics_data,
    nih_funding_metrics   = nih_funding_metrics_data,
    first_author_gender   = first_author_gender_data,
    disruption_index      = disruption_index_data,
    citation_metrics      = citation_metrics_data,
    intl_collab           = intl_collab_data,
    evidence_quality      = evidence_quality_data,
    ref_age_metrics       = ref_age_metrics_data,
    rcr_metrics           = rcr_metrics_data,
    # ---- Science mapping ----
    coword_network        = coword_network_matrix,
    coauthorship_network  = coauthorship_network_matrix,
    cocitation_network    = cocitation_network_matrix,
    thematic_evolution    = thematic_evolution_data,
    mk_trend              = mk_trend_data,
    evidence_evolution    = evidence_evolution_data,
    equity_metrics        = equity_metrics_data,
    vosviewer_files       = vosviewer_export_paths,
    figures               = pipeline_figures,
    figure_paths          = saved_figure_paths,
    # ---- Reproducibility artefacts ----
    query_hash            = query_hash,
    session_snapshot      = session_snapshot_data,
    output_manifest       = output_manifest_data
  ))
}

# ============================================================
# TIER-1 METRICS: h-index, citation velocity, citation Gini
# ============================================================

#' @noRd
.compute_citation_metrics <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[CITATION METRICS] Computing h-index, velocity, Gini ...",
            verbose)

  tc_raw  <- suppressWarnings(as.numeric(bibliography$TC))
  py_raw  <- suppressWarnings(as.integer(bibliography$publication_year))
  current_year <- as.integer(format(Sys.Date(), "%Y"))

  tc_valid  <- tc_raw[!is.na(tc_raw) & tc_raw >= 0]
  n_total   <- length(tc_raw)
  n_valid   <- length(tc_valid)

  if (n_valid == 0L) {
    .log_step("[CITATION METRICS] No valid TC values -- returning NAs.",
              verbose)
    return(dplyr::tibble(
      h_index                  = NA_integer_,
      citation_velocity_median = NA_real_,
      citation_velocity_mean   = NA_real_,
      citation_gini            = NA_real_,
      n_uncited                = NA_integer_,
      pct_uncited              = NA_real_,
      n_papers_used            = 0L
    ))
  }

  # ---- h-index: h papers each cited >= h times ----
  tc_desc  <- sort(tc_valid, decreasing = TRUE)
  h_candidates <- which(tc_desc >= seq_along(tc_desc))
  h_index  <- if (length(h_candidates) > 0L)
    as.integer(max(h_candidates)) else 0L

  # ---- Citation velocity: TC / age in years ----
  age_yrs  <- pmax(current_year - py_raw, 1L)
  velocity <- tc_raw / age_yrs
  vel_valid <- velocity[!is.na(velocity) & is.finite(velocity)]

  # ---- Citation Gini: inequality of citation distribution ----
  # Gini = 0 (all equal) to 1 (one paper has everything)
  tc_sorted <- sort(tc_valid)
  n_g       <- length(tc_sorted)
  sum_tc    <- sum(tc_sorted)
  gini_val  <- if (sum_tc > 0) {
    (2 * sum(tc_sorted * seq_len(n_g)) - (n_g + 1L) * sum_tc) /
      (n_g * sum_tc)
  } else {
    0
  }

  n_uncited   <- sum(tc_valid == 0L)
  pct_uncited <- round(n_uncited / n_total * 100, 1L)

  .log_step(sprintf(
    "[CITATION METRICS] h=%d | vel_med=%.2f | gini=%.3f | uncited=%.1f%%",
    h_index,
    median(vel_valid, na.rm = TRUE),
    gini_val,
    pct_uncited
  ), verbose)

  dplyr::tibble(
    h_index                  = h_index,
    citation_velocity_median = round(median(vel_valid, na.rm = TRUE), 2L),
    citation_velocity_mean   = round(mean(vel_valid,   na.rm = TRUE), 2L),
    citation_gini            = round(gini_val, 3L),
    n_uncited                = n_uncited,
    pct_uncited              = pct_uncited,
    n_papers_used            = n_valid
  )
}


# ============================================================
# TIER-2 METRICS: international collaboration rate
# ============================================================

#' @noRd
.compute_intl_collaboration <- function(bibliography, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[INTL COLLAB] Computing international collaboration rate ...",
            verbose)

  if (!"AU_CO" %in% names(bibliography)) {
    .log_step("[INTL COLLAB] No AU_CO column -- returning NAs.", verbose)
    return(dplyr::tibble(
      pct_intl_collab      = NA_real_,
      mean_countries_paper = NA_real_,
      n_papers_with_auco   = 0L
    ))
  }

  co_col   <- bibliography$AU_CO
  has_data <- !is.na(co_col) & nchar(trimws(co_col)) > 0L
  co_valid <- co_col[has_data]
  n_valid  <- length(co_valid)

  if (n_valid == 0L) {
    return(dplyr::tibble(
      pct_intl_collab      = NA_real_,
      mean_countries_paper = NA_real_,
      n_papers_with_auco   = 0L
    ))
  }

  # Count distinct unique countries per paper.
  # bibliometrix can list the same country multiple times in AU_CO when
  # multiple authors share a country (e.g. "USA;USA;GERMANY" = 2 US authors
  # + 1 German author). Deduplicate before counting so that paper contributes
  # 2 unique countries, not 3 tokens.
  # Uses vapply for type safety; measurably faster than purrr::map_int
  # for large corpora (avoids purrr overhead per element).
  n_countries_per_paper <- vapply(co_valid, function(co) {
    parts <- unique(toupper(trimws(
      strsplit(co, ";", fixed = TRUE)[[1L]]
    )))
    sum(nchar(parts) > 0L)
  }, integer(1L))

  pct_intl <- round(mean(n_countries_per_paper > 1L) * 100, 1L)
  mean_co  <- round(mean(n_countries_per_paper), 2L)

  .log_step(sprintf(
    "[INTL COLLAB] %.1f%% multi-country | mean %.2f countries/paper",
    pct_intl, mean_co
  ), verbose)

  dplyr::tibble(
    pct_intl_collab      = pct_intl,
    mean_countries_paper = mean_co,
    n_papers_with_auco   = n_valid
  )
}


# ============================================================
# TIER-3 METRIC: evidence quality score from study_type_metrics
# ============================================================

#' @noRd
.compute_evidence_quality_score <- function(study_type_tbl, verbose = TRUE) {
  assertthat::assert_that(is.data.frame(study_type_tbl))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[EVIDENCE QUALITY] Computing weighted quality score ...", verbose)

  # Evidence hierarchy weights (0-5 scale)
  quality_weights <- c(
    "RCT"                               = 5L,
    "Meta-Analysis / Systematic Review" = 5L,
    "Multicenter Study"                 = 4L,
    "Guideline"                         = 3L,
    "Clinical Trial (Non-RCT)"          = 3L,
    "Observational / Comparative"       = 2L,
    "Narrative Review"                  = 2L,
    "Case Report"                       = 1L,
    "Original Article"                  = 1L,
    "Other"                             = 1L,
    "Unknown"                           = 1L,
    "Editorial / Comment / Letter"      = 0L
  )

  if (nrow(study_type_tbl) == 0L ||
      !"study_type" %in% names(study_type_tbl) ||
      !"n_papers" %in% names(study_type_tbl)) {
    .log_step("[EVIDENCE QUALITY] Empty or missing study_type_tbl -- NA.",
              verbose)
    return(dplyr::tibble(
      evidence_quality_score = NA_real_,
      pct_high_evidence      = NA_real_,    # RCT + meta + multicenter
      quality_weights_used   = list(quality_weights)
    ))
  }

  scored <- study_type_tbl |>
    dplyr::mutate(
      weight = dplyr::recode(
        .data$study_type,
        !!!quality_weights,
        .default = 1L
      ),
      weighted_n = .data$n_papers * .data$weight
    )

  n_total <- sum(scored$n_papers)
  quality_score <- if (n_total > 0L)
    round(sum(scored$weighted_n) / n_total, 3L)
  else NA_real_

  high_evidence_types <- c("RCT", "Meta-Analysis / Systematic Review",
                            "Multicenter Study")
  n_high <- sum(
    scored$n_papers[scored$study_type %in% high_evidence_types],
    na.rm = TRUE
  )
  pct_high <- round(n_high / max(n_total, 1L) * 100, 1L)

  .log_step(sprintf(
    "[EVIDENCE QUALITY] Score=%.3f | High-evidence=%.1f%%",
    quality_score, pct_high
  ), verbose)

  dplyr::tibble(
    evidence_quality_score = quality_score,
    pct_high_evidence      = pct_high,
    quality_weights_used   = list(quality_weights)
  )
}


# ============================================================
# TIER-4 METRIC: Price Index and mean reference age from CR
# ============================================================

#' @noRd
.compute_reference_age_metrics <- function(bibliography,
                                           price_index_window = 5L,
                                           verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(
    length(price_index_window) == 1L &&
      is.finite(as.numeric(price_index_window)) &&
      as.numeric(price_index_window) >= 1,
    msg = paste(
      "`price_index_window` must be a single positive number or integer.",
      "Received:", deparse(price_index_window)
    )
  )
  price_index_window <- as.integer(price_index_window)
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[REF AGE] Computing Price Index and mean reference age ...",
            verbose)

  if (!"CR" %in% names(bibliography)) {
    .log_step("[REF AGE] No CR column -- returning NAs.", verbose)
    return(dplyr::tibble(
      price_index    = NA_real_,
      mean_ref_age   = NA_real_,
      median_ref_age = NA_real_,
      n_refs_parsed  = 0L,
      method_note    = "CR field absent"
    ))
  }

  # Extract four-digit years from every reference in every paper
  all_ref_years <- unlist(
    lapply(bibliography$CR, function(cr) {
      if (is.na(cr) || nchar(trimws(cr)) == 0L) return(integer(0))
      refs <- strsplit(cr, ";")[[1L]]
      suppressWarnings(
        as.integer(stringr::str_extract(trimws(refs), "\\b(19|20)\\d{2}\\b"))
      )
    })
  )
  all_ref_years <- all_ref_years[!is.na(all_ref_years)]

  if (length(all_ref_years) == 0L) {
    .log_step("[REF AGE] No parseable years in CR field.", verbose)
    return(dplyr::tibble(
      price_index    = NA_real_,
      mean_ref_age   = NA_real_,
      median_ref_age = NA_real_,
      n_refs_parsed  = 0L,
      method_note    = "No year tokens found in CR field"
    ))
  }

  current_year   <- as.integer(format(Sys.Date(), "%Y"))
  ref_ages       <- current_year - all_ref_years
  ref_ages_valid <- ref_ages[ref_ages >= 0L & ref_ages < 200L]

  # Price Index: proportion of references published within the window
  threshold_year <- current_year - price_index_window
  price_idx      <- round(
    mean(all_ref_years >= threshold_year, na.rm = TRUE) * 100, 1L
  )
  mean_age       <- round(mean(ref_ages_valid,   na.rm = TRUE), 1L)
  median_age     <- round(median(ref_ages_valid, na.rm = TRUE), 1L)

  .log_step(sprintf(
    "[REF AGE] Price Index (<%dyr)=%.1f%% | Mean ref age=%.1f yr | n=%d refs",
    price_index_window, price_idx, mean_age, length(ref_ages_valid)
  ), verbose)

  dplyr::tibble(
    price_index    = price_idx,
    mean_ref_age   = mean_age,
    median_ref_age = median_age,
    n_refs_parsed  = length(ref_ages_valid),
    method_note    = sprintf(
      "Price Index = %% references within %d years of %d.",
      price_index_window, current_year
    )
  )
}


# ============================================================
# TIER-5 METRIC: RCR via NIH iCite API
# ============================================================

#' @noRd
.fetch_icite_rcr <- function(bibliography, batch_size = 100L,
                              verbose = TRUE) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(
    assertthat::is.count(batch_size) && batch_size <= 1000L,
    msg = "`batch_size` must be a positive integer <= 1000."
  )
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))
  .log_step("[RCR] Fetching Relative Citation Ratios from NIH iCite API ...",
            verbose)

  # PubMed exports store PMID in the UT column (e.g. "MEDLINE:12345678")
  ut_col <- if ("UT" %in% names(bibliography)) bibliography$UT else
    if ("PMID" %in% names(bibliography)) bibliography$PMID else
      NULL

  if (is.null(ut_col)) {
    .log_step(
      "[RCR] No UT/PMID column found -- RCR requires PubMed export.", verbose
    )
    return(.rcr_na_result(nrow(bibliography), "UT/PMID column absent"))
  }

  # Parse numeric PMID from strings like "MEDLINE:12345678" or "12345678".
  # Reject WOS-format strings ("WOS:...") which contain digit sequences
  # that are accession numbers, not PMIDs. Without this guard,
  # "WOS:0001234567" would parse as PMID 123456 -- a real but wrong PMID.
  ut_str <- as.character(ut_col)
  is_wos  <- grepl("^WOS:|^ISI:", ut_str, ignore.case = TRUE)
  if (any(is_wos)) {
    .log_step(sprintf(
      "[RCR] %d WOS-format UT values detected -- these are not PMIDs and will be excluded.",
      sum(is_wos)), verbose)
    ut_str[is_wos] <- NA_character_
  }
  pmids <- suppressWarnings(as.integer(
    stringr::str_extract(ut_str, "\\d{5,9}")
  ))
  pmids_valid <- pmids[!is.na(pmids) & pmids > 0L]

  if (length(pmids_valid) == 0L) {
    .log_step("[RCR] No valid PMIDs parseable from UT column.", verbose)
    return(.rcr_na_result(nrow(bibliography), "No valid PMIDs in UT column"))
  }

  .log_step(sprintf(
    "[RCR] Querying iCite for %d papers in batches of %d ...",
    length(pmids_valid), batch_size
  ), verbose)

  # Batch API calls
  batches <- split(pmids_valid,
                   ceiling(seq_along(pmids_valid) / batch_size))
  rcr_rows <- vector("list", length(batches))

  for (i in seq_along(batches)) {
    pmid_str <- paste(batches[[i]], collapse = ",")
    url      <- paste0(
      "https://icite.od.nih.gov/api/pubs?pmids=", pmid_str,
      "&format=json"
    )
    response <- tryCatch({
      resp <- httr::GET(url, httr::timeout(30))
      if (httr::status_code(resp) != 200L) {
        .log_step(sprintf("[RCR] Batch %d: HTTP %d -- skipping.",
                          i, httr::status_code(resp)), verbose)
        NULL
      } else {
        parsed <- httr::content(resp, as = "parsed", encoding = "UTF-8")
        if (!is.null(parsed$data) && length(parsed$data) > 0L) {
          purrr::map_dfr(parsed$data, function(pub) {
            dplyr::tibble(
              pmid         = as.integer(pub$pmid),
              rcr          = as.numeric(pub$relative_citation_ratio),
              citations    = as.integer(pub$citation_count),
              expected_cit = as.numeric(pub$expected_citations_per_year),
              field_citation_rate =
                as.numeric(pub$field_citation_rate)
            )
          })
        } else NULL
      }
    }, error = function(e) {
      .log_step(sprintf("[RCR] Batch %d: error -- %s", i, e$message),
                verbose)
      NULL
    })

    rcr_rows[[i]] <- response
    if (i < length(batches)) Sys.sleep(0.3)  # polite rate limiting
  }

  rcr_tbl <- dplyr::bind_rows(rcr_rows)

  if (nrow(rcr_tbl) == 0L) {
    .log_step("[RCR] API returned no data.", verbose)
    return(.rcr_na_result(nrow(bibliography), "iCite API returned no data"))
  }

  rcr_valid   <- rcr_tbl$rcr[!is.na(rcr_tbl$rcr)]
  n_retrieved <- nrow(rcr_tbl)
  coverage    <- round(n_retrieved / length(pmids_valid) * 100, 1L)

  .log_step(sprintf(
    "[RCR] Retrieved %d/%d papers (%.1f%%) | Median RCR=%.3f",
    n_retrieved, length(pmids_valid), coverage,
    median(rcr_valid, na.rm = TRUE)
  ), verbose)

  dplyr::tibble(
    rcr_median    = round(median(rcr_valid, na.rm = TRUE), 3L),
    rcr_mean      = round(mean(rcr_valid,   na.rm = TRUE), 3L),
    rcr_above_1   = round(mean(rcr_valid > 1, na.rm = TRUE) * 100, 1L),
    n_pmids_sent  = length(pmids_valid),
    n_rcr_retrieved = n_retrieved,
    coverage_pct  = coverage,
    method_note   = paste(
      "RCR from NIH iCite API (icite.od.nih.gov).",
      sprintf("Coverage: %d/%d papers (%.1f%%).",
              n_retrieved, length(pmids_valid), coverage),
      "RCR > 1.0 indicates above-field-average influence."
    )
  )
}

#' @noRd
.rcr_na_result <- function(n_papers, reason) {
  dplyr::tibble(
    rcr_median      = NA_real_,
    rcr_mean        = NA_real_,
    rcr_above_1     = NA_real_,
    n_pmids_sent    = 0L,
    n_rcr_retrieved = 0L,
    coverage_pct    = NA_real_,
    method_note     = reason
  )
}

# ============================================================
# ADDITION 1: FEMALE FIRST-AUTHORSHIP BY ERA × SUBSPECIALTY
# ============================================================

#' @noRd
.compute_gender_by_era <- function(
    bibliography,
    eras    = NULL,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  if (is.null(eras)) {
    yr_range <- range(bibliography$publication_year, na.rm = TRUE)
    eras     <- make_eras(yr_range[1L], yr_range[2L])
  }
  assertthat::assert_that(
    is.list(eras) && length(eras) >= 1L,
    msg = paste(
      ".compute_gender_by_era: `eras` must be a named list with",
      "at least one element. Got length:", length(eras)
    )
  )
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  if (!"AF" %in% names(bibliography) ||
      !"publication_year" %in% names(bibliography)) {
    .log_step("[GENDER ERA] AF or publication_year absent.", verbose)
    return(dplyr::tibble(
      era              = character(),
      n_papers         = integer(),
      n_female         = integer(),
      n_male           = integer(),
      n_classified     = integer(),
      n_unclassified   = integer(),
      pct_female       = numeric(),
      pct_unclassified = numeric()
    ))
  }

  .log_step("[GENDER ERA] Computing female first-authorship by era ...",
            verbose)

  purrr::map_dfr(names(eras), function(era_nm) {
    yr      <- eras[[era_nm]]
    era_bib <- dplyr::filter(bibliography,
      .data$publication_year >= yr[1L],
      .data$publication_year <= yr[2L]
    )
    if (nrow(era_bib) == 0L) return(dplyr::tibble())

    genders     <- .infer_gender_vectorized(era_bib$AF)
    n_female    <- sum(genders == "Female",  na.rm = TRUE)
    n_male      <- sum(genders == "Male",    na.rm = TRUE)
    n_class     <- n_female + n_male
    n_unclass   <- sum(is.na(genders))
    pct_female  <- if (n_class > 0L)
      round(n_female / n_class * 100, 1L) else NA_real_

    dplyr::tibble(
      era            = era_nm,
      n_papers       = nrow(era_bib),
      n_female       = n_female,
      n_male         = n_male,
      n_classified   = n_class,
      n_unclassified = n_unclass,
      pct_female     = pct_female,
      pct_unclassified = round(n_unclass / nrow(era_bib) * 100, 1L)
    )
  })
}

#' Plot Female First-Authorship Trends by Era Across Subspecialties
#'
#' @description
#' Connected dot plot showing the percentage of female first-authored
#' publications across four publication eras for each OB/GYN subspecialty.
#' Each line represents one subspecialty; the focal subspecialty
#' (default \code{"FPMRS"}) is drawn with a heavier, fully opaque line.
#' End-of-line labels identify each subspecialty without a legend.
#'
#' Gender is inferred from the first author's given name via
#' \code{.infer_gender_vectorized()}, which applies a pre-built lookup
#' table. Papers whose first author has only initials or a non-Western
#' given name cannot be classified and are excluded from both numerator
#' and denominator. The \code{min_classified} argument suppresses any
#' era with too few classified papers to produce a reliable estimate.
#'
#' @section Interpretation:
#' A rising trend in \% female first authors signals increasing
#' workforce diversity in that subspecialty over time. Comparing FPMRS
#' against Gyn Oncology (typically high) and Urology (typically low)
#' contextualises the field's trajectory within the broader OB/GYN
#' ecosystem. A flat or falling trend despite a growing corpus may
#' indicate structural barriers to authorship parity.
#'
#' @section Limitations:
#' \itemize{
#'   \item Name-based gender inference cannot capture gender identity;
#'     it approximates the binary male/female split from given-name
#'     probability and is therefore a proxy measure only.
#'   \item Classification rates vary by region. Asian, Middle Eastern,
#'     and African names are systematically harder to classify, which
#'     can bias estimates for subspecialties with high international
#'     authorship (e.g., MFM).
#'   \item Only first authorship is assessed; last authorship (senior
#'     author) and middle authorship trends are not captured here.
#' }
#'
#' @param subspecialty_results_list Named list of subspecialty pipeline
#'   results, as returned by \code{run_subspecialty_comparison()}.
#'   Each element must contain a \code{bibliography} data frame with
#'   at minimum an \code{AF} column (full author names in
#'   \code{"Surname, Given Names"} format) and a
#'   \code{publication_year} integer column.
#' @param highlight_subspecialty Character scalar. Name of the
#'   subspecialty to emphasise with a heavier line. Must match one of
#'   the \code{subspecialty} fields in \code{subspecialty_results_list}.
#'   Defaults to \code{"FPMRS"}.
#' @param eras Named list where each element is a length-2 integer
#'   vector \code{c(start_year, end_year)} (both inclusive). Names
#'   become x-axis tick labels. Defaults to the standard four-era
#'   breakdown used throughout this pipeline.
#' @param min_classified Integer \eqn{\geq 1}. Eras with fewer than
#'   this many classified papers are dropped from the plot to avoid
#'   unreliable percentage estimates. Defaults to \code{10L}.
#' @param verbose Logical. If \code{TRUE}, logs progress messages to
#'   the console. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object. Save with
#'   \code{ggplot2::ggsave()} at width \eqn{\geq 8} inches for
#'   readable end-of-line labels.
#'
#' @examples
#' \dontrun{
#' # After running run_subspecialty_comparison():
#' fig <- plot_female_authorship_by_era(
#'   subspecialty_results_list = comparison_result$subspecialty_results,
#'   highlight_subspecialty    = "FPMRS",
#'   min_classified            = 15L,
#'   verbose                   = FALSE
#' )
#' ggplot2::ggsave("female_authorship_trends.pdf", fig,
#'                  width = 9, height = 5)
#'
#' # Custom three-era breakdown
#' fig2 <- plot_female_authorship_by_era(
#'   subspecialty_results_list = comparison_result$subspecialty_results,
#'   eras = list(
#'     "Pre-2010"   = c(2000L, 2009L),
#'     "2010-2016"  = c(2010L, 2016L),
#'     "Post-2016"  = c(2017L, 2023L)
#'   ),
#'   highlight_subspecialty = "FPMRS",
#'   verbose = FALSE
#' )
#' }
#'
#' @seealso
#' \code{\link{run_subspecialty_comparison}} which produces
#'   \code{subspecialty_results_list};
#' \code{.compute_first_author_gender} for the single-subspecialty
#'   aggregate gender summary;
#' \code{.compute_gender_by_era} (internal) for the underlying
#'   era-stratified computation;
#' \code{\link{plot_thematic_evolution}} for the analogous era-based
#'   keyword trend visualisation.
#'
#' @importFrom assertthat assert_that is.string is.count is.flag
#' @importFrom dplyr mutate filter bind_rows group_by slice_head ungroup
#' @importFrom purrr imap_dfr map_dfr
#' @importFrom ggplot2 ggplot aes geom_line geom_point geom_text
#'   scale_color_viridis_d scale_linewidth_identity scale_alpha_identity
#'   scale_x_discrete scale_y_continuous expansion labs
#' @importFrom rlang .data
#' @export
plot_female_authorship_by_era <- function(
    subspecialty_results_list,
    highlight_subspecialty = "FPMRS",
    eras = list(
      "2000-2007" = c(2000L, 2007L),
      "2008-2013" = c(2008L, 2013L),
      "2014-2018" = c(2014L, 2018L),
      "2019+"     = c(2019L, 2100L)
    ),
    min_classified = 10L,
    verbose        = TRUE
) {
  assertthat::assert_that(is.list(subspecialty_results_list))
  assertthat::assert_that(assertthat::is.string(highlight_subspecialty))
  assertthat::assert_that(assertthat::is.count(min_classified))
  assertthat::assert_that(isTRUE(verbose) || isFALSE(verbose))

  .log_step("[PLOT] Building female first-authorship by era ...", verbose)

  era_order  <- names(eras)
  era_labels <- era_order   # use as-is; caller controls names

  # Build long table: subspecialty × era × pct_female
  gender_era_tbl <- purrr::imap_dfr(
    subspecialty_results_list,
    function(sp_result, key) {
      sp_name <- if (!is.null(sp_result$subspecialty))
        sp_result$subspecialty else key
      bib     <- sp_result$bibliography
      if (is.null(bib)) return(dplyr::tibble())

      era_tbl <- .compute_gender_by_era(bib, eras, verbose = FALSE)
      if (nrow(era_tbl) == 0L) return(dplyr::tibble())

      era_tbl |>
        dplyr::mutate(subspecialty = sp_name) |>
        dplyr::filter(.data$n_classified >= min_classified)
    }
  )

  if (nrow(gender_era_tbl) == 0L) {
    .log_step("[PLOT] No classifiable gender data — returning placeholder.",
              verbose)
    return(
      ggplot2::ggplot() +
        ggplot2::labs(title = "Female First-Authorship by Era",
                      subtitle = "Insufficient AF data.") +
        .theme_fpmrs_manuscript()
    )
  }

  plot_data <- gender_era_tbl |>
    dplyr::mutate(
      era = factor(.data$era, levels = era_order),
      is_focal    = .data$subspecialty == highlight_subspecialty,
      line_width  = dplyr::if_else(.data$is_focal, 2.2, 0.7),
      line_alpha  = dplyr::if_else(.data$is_focal, 1.0, 0.55)
    )

  # End-of-line labels at the last era in the data
  last_era_level <- era_order[max(which(era_order %in%
                                          unique(as.character(plot_data$era))))]
  label_data <- plot_data |>
    dplyr::filter(as.character(.data$era) == last_era_level) |>
    dplyr::group_by(.data$subspecialty) |>
    dplyr::slice_head(n = 1L) |>
    dplyr::ungroup()

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x         = .data$era,
      y         = .data$pct_female,
      group     = .data$subspecialty,
      color     = .data$subspecialty,
      linewidth = .data$line_width,
      alpha     = .data$line_alpha
    )
  ) +
    ggplot2::geom_line(na.rm = TRUE) +
    ggplot2::geom_point(size = 3.0, na.rm = TRUE) +
    ggplot2::geom_text(
      data = label_data,
      ggplot2::aes(
        x     = as.numeric(.data$era) + 0.08,
        label = .data$subspecialty,
        color = .data$subspecialty
      ),
      hjust = 0, size = 2.7, show.legend = FALSE, na.rm = TRUE
    ) +
    ggplot2::scale_color_viridis_d(option = "turbo", guide = "none") +
    ggplot2::scale_linewidth_identity() +
    ggplot2::scale_alpha_identity() +
    ggplot2::scale_x_discrete(
      labels = era_labels,
      expand = ggplot2::expansion(add = c(0.3, 1.8))
    ) +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      limits = c(0, 100),
      expand = ggplot2::expansion(mult = c(0.02, 0.05))
    ) +
    ggplot2::labs(
      title    = "Female First-Authorship Trends by Era and Subspecialty",
      subtitle = sprintf(
        "%s bolded | Only eras with \u2265%d classified papers shown",
        highlight_subspecialty, min_classified
      ),
      x       = "Publication Era",
      y       = "Female First Authors (% of classified)",
      caption = paste(
        "Gender inferred from first name via lookup table.",
        "Initials-only and non-Western names excluded from denominator.",
        "Trend reflects classified papers only."
      )
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      panel.grid.major.x = ggplot2::element_blank(),
      axis.text.x = ggplot2::element_text(face = "bold", size = 10)
    )

  .log_step("[PLOT] Female first-authorship by era complete.", verbose)
  fig
}

# ============================================================
# ADDITION 2: MESH INTERRUPTED TIME-SERIES ANALYSIS
# ============================================================

#' @noRd
.classify_mesh_subcorpus <- function(bibliography, verbose = TRUE) {
  # Classify each paper into mesh/sling, explant/complication, or other.
  # Uses PT (publication type), TI (title), and DE (keywords).
  # Returns bibliography with added `mesh_subcorpus` column.
  assertthat::assert_that(is.data.frame(bibliography))
  .log_step("[MESH ITS] Classifying mesh subcorpora ...", verbose)

  mesh_pattern <- paste(
    "\\bMESH\\b|\\bSLING\\b|MIDURETHRAL|MID-URETHRAL|TRANSVAGINAL",
    "PROLIFT|GYNEMESH|TENSION.FREE|\\bTVT\\b|\\bTOT\\b|VAGINAL TAPE",
    sep = "|"
  )
  explant_pattern <- paste(
    "EXPLANT|MESH REMOVAL|MESH COMPLICATION|MESH EROSION",
    "MESH EXTRUSION|MESH EXPOSURE|MESH REVISION|MESH PAIN|MESH INFECTION",
    sep = "|"
  )

  ti_up <- toupper(dplyr::coalesce(
    if ("TI" %in% names(bibliography)) bibliography$TI else NA_character_,
    ""
  ))
  de_up <- toupper(dplyr::coalesce(
    if ("DE" %in% names(bibliography)) bibliography$DE else NA_character_,
    ""
  ))
  combined_up <- paste(ti_up, de_up, sep = " ")

  is_mesh    <- grepl(mesh_pattern,    combined_up)
  is_explant <- grepl(explant_pattern, combined_up)

  # Explant subcorpus requires BOTH a mesh term AND a complication term
  subcorpus <- dplyr::case_when(
    is_mesh & is_explant ~ "Mesh explant/complication",
    is_mesh              ~ "Mesh/sling repair",
    TRUE                 ~ "Non-mesh urogyn"
  )

  n_mesh    <- sum(subcorpus == "Mesh/sling repair")
  n_explant <- sum(subcorpus == "Mesh explant/complication")
  .log_step(sprintf(
    "[MESH ITS] Mesh/sling: %d | Explant/complication: %d | Other: %d",
    n_mesh, n_explant, sum(subcorpus == "Non-mesh urogyn")
  ), verbose)

  bibliography |> dplyr::mutate(mesh_subcorpus = subcorpus)
}

#' Compute Mesh Interrupted Time-Series
#'
#' @description
#' Classifies each paper in a urogynecology bibliography into one of three
#' mutually exclusive subcorpora based on title (\code{TI}) and keyword
#' (\code{DE}) content, then fits segmented linear regression to annual
#' publication counts in each subcorpus at three pre-specified regulatory
#' breakpoints. Returns slope, p-value, and R\eqn{^2} per segment per
#' subcorpus, enabling a formal interrupted time-series assessment of how
#' mesh-related research responded to real-world policy events.
#'
#' @section Subcorpora:
#' Classification is hierarchical and requires co-occurrence for
#' the explant bucket:
#' \describe{
#'   \item{Mesh explant/complication}{Paper must match both a mesh/sling
#'     term \emph{and} an adverse-event term (erosion, extrusion, removal,
#'     pain, infection). This prevents non-mesh revision papers from
#'     contaminating the explant signal.}
#'   \item{Mesh/sling repair}{Paper matches a mesh/sling term but no
#'     adverse-event term — i.e., primary procedure literature.}
#'   \item{Non-mesh urogyn}{All remaining papers; serves as the
#'     concurrent control series for difference-in-differences framing.}
#' }
#'
#' @section Regulatory breakpoints:
#' The three default breakpoints correspond to landmark regulatory events
#' in the transvaginal mesh controversy:
#' \enumerate{
#'   \item \strong{2016} — FDA issued a reclassification order elevating
#'     transvaginal mesh for POP repair to Class III (high-risk) devices,
#'     requiring premarket approval.
#'   \item \strong{2018} — NHS England and NHS Improvement paused use
#'     of mesh for stress urinary incontinence and POP pending a national
#'     patient safety review (the Cumberlege Review).
#'   \item \strong{2019} — FDA ordered manufacturers to stop selling and
#'     distributing all remaining transvaginal mesh products for POP repair
#'     in the United States.
#' }
#' Custom breakpoints can be passed for sensitivity analyses (e.g.,
#' adding the 2011 FDA safety communication or the 2017 Australian TGA
#' decision).
#'
#' @section Statistical approach:
#' Segmented ordinary least squares is fitted independently within each
#' era segment (before first breakpoint, between each pair, after last
#' breakpoint) for each subcorpus. No external segmented-regression
#' package is required. For a fully Bayesian or Poisson-family ITS, the
#' \code{annual_counts} element of the return value can be passed to
#' downstream modelling functions.
#'
#' @param bibliography A data frame produced by
#'   \code{run_fpmrs_bibliometric_pipeline()} or
#'   \code{.standardize_and_filter_bibliography()}, containing at minimum
#'   \code{publication_year} (integer). Optionally \code{TI} (title) and
#'   \code{DE} (author keywords); without these, all papers land in
#'   \code{Non-mesh urogyn}.
#' @param breakpoints Named integer vector of policy event years used as
#'   segment boundaries. Names become annotation labels in
#'   \code{plot_mesh_its()}. Defaults to the three landmark regulatory
#'   events described above.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A named list with four elements:
#' \describe{
#'   \item{\code{classified_bib}}{The input \code{bibliography} with an
#'     added \code{mesh_subcorpus} character column.}
#'   \item{\code{annual_counts}}{Tibble with columns
#'     \code{publication_year}, \code{mesh_subcorpus},
#'     \code{n_papers} — one row per year × subcorpus combination.}
#'   \item{\code{segment_fits}}{Tibble with columns
#'     \code{mesh_subcorpus}, \code{seg_start}, \code{seg_end},
#'     \code{n_years}, \code{slope_papers_yr}, \code{p_value},
#'     \code{r_squared}, \code{mean_annual} — one row per segment ×
#'     subcorpus.}
#'   \item{\code{breakpoints}}{The named integer vector passed as input,
#'     preserved for downstream use in \code{plot_mesh_its()}.}
#' }
#'
#' @examples
#' \dontrun{
#' # Standard analysis with default breakpoints
#' its <- compute_mesh_its(
#'   bibliography = pipeline_result$bibliography,
#'   verbose      = TRUE
#' )
#' its$segment_fits
#'
#' # Sensitivity: add 2011 FDA safety communication as additional breakpoint
#' its_sensitivity <- compute_mesh_its(
#'   bibliography = pipeline_result$bibliography,
#'   breakpoints  = c(
#'     "2011 FDA safety communication"    = 2011L,
#'     "2016 FDA reclassification"        = 2016L,
#'     "2018 UK pause"                    = 2018L,
#'     "2019 FDA market withdrawal order" = 2019L
#'   )
#' )
#' plot_mesh_its(its_sensitivity)
#' }
#'
#' @seealso
#' \code{\link{plot_mesh_its}} for the faceted visualisation of these
#'   results;
#' \code{\link{compute_mesh_its}} uses \code{.classify_mesh_subcorpus}
#'   (internal) for the title/keyword classification step;
#' \code{\link{run_fpmrs_bibliometric_pipeline}} which produces the
#'   \code{bibliography} argument;
#' \code{\link{generate_abstract_results_text}} where the mesh ITS
#'   findings can be reported in the abstract results sentence.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr mutate filter count group_by summarise arrange
#' @importFrom purrr map_dfr
#' @importFrom rlang .data
#' @export
compute_mesh_its <- function(
    bibliography,
    breakpoints = c(
      "2016 FDA safety communication"    = 2016L,
      "2018 UK pause"                    = 2018L,
      "2019 FDA market withdrawal order" = 2019L
    ),
    alpha   = 0.05,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(
    "publication_year" %in% names(bibliography),
    msg = "`bibliography` must contain 'publication_year'."
  )
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    assertthat::is.number(alpha) && alpha > 0 && alpha < 1,
    msg = sprintf("`alpha` must be in (0, 1). Received: %g", alpha)
  )
  assertthat::assert_that(
    is.numeric(breakpoints) && !is.null(names(breakpoints)),
    msg = "`breakpoints` must be a named numeric vector of years."
  )

  .log_step("[MESH ITS] Starting mesh interrupted time-series ...", verbose)

  # Step 1: classify
  bib_classified <- .classify_mesh_subcorpus(bibliography, verbose)

  # Step 2: annual counts by subcorpus
  annual_counts <- bib_classified |>
    dplyr::count(.data$publication_year, .data$mesh_subcorpus) |>
    dplyr::rename(n_papers = n) |>
    dplyr::arrange(.data$mesh_subcorpus, .data$publication_year)

  .log_step(sprintf(
    "[MESH ITS] Annual counts: %d year×subcorpus rows",
    nrow(annual_counts)
  ), verbose)

  # Step 3: segmented fits — fit linear model in each segment defined
  # by breakpoints, per subcorpus.  No external package needed.
  bp_years   <- sort(unique(as.integer(breakpoints)))
  all_years  <- sort(unique(annual_counts$publication_year))
  subcorpora <- unique(annual_counts$mesh_subcorpus)

  segment_fits <- purrr::map_dfr(subcorpora, function(sc) {
    sc_data <- dplyr::filter(annual_counts,
                              .data$mesh_subcorpus == sc) |>
      dplyr::arrange(.data$publication_year)

    if (nrow(sc_data) < 4L) return(dplyr::tibble())

    # Segment boundaries: before first BP, between each pair, after last BP
    boundaries <- c(min(all_years) - 1L, bp_years, max(all_years))
    segments   <- purrr::map_dfr(
      seq_len(length(boundaries) - 1L),
      function(i) {
        seg_start <- boundaries[i]   + 1L
        seg_end   <- boundaries[i+1L]
        seg_data  <- dplyr::filter(sc_data,
          .data$publication_year >= seg_start,
          .data$publication_year <= seg_end
        )
        if (nrow(seg_data) < 3L) return(dplyr::tibble())

        # Theil-Sen non-parametric slope (3-8 points per segment —
        # too few for OLS normality assumptions).
        .ts_slope_its <- function(xi, yi) {
          pairs <- expand.grid(i = seq_along(xi), j = seq_along(xi))
          pairs <- pairs[pairs$j > pairs$i, ]
          if (nrow(pairs) == 0L) return(NA_real_)
          median((yi[pairs$j] - yi[pairs$i]) /
                 (xi[pairs$j] - xi[pairs$i]), na.rm = TRUE)
        }
        slope <- round(.ts_slope_its(
          seg_data$publication_year, seg_data$n_papers), 2L)

        # OLS p-value and R² for fit quality only (not for slope inference)
        fit <- tryCatch(
          stats::lm(n_papers ~ publication_year, data = seg_data),
          error = function(e) NULL
        )
        if (is.null(fit)) return(dplyr::tibble())

        p_slope <- round(summary(fit)$coefficients["publication_year",
                                                     "Pr(>|t|)"], 4L)
        r2      <- round(summary(fit)$r.squared, 3L)

        dplyr::tibble(
          mesh_subcorpus  = sc,
          seg_start       = seg_start,
          seg_end         = seg_end,
          n_years         = nrow(seg_data),
          slope_papers_yr = slope,
          p_value         = p_slope,
          r_squared       = r2,
          mean_annual     = round(mean(seg_data$n_papers), 1L)
        )
      }
    )
    segments
  })

  .log_step(sprintf(
    "[MESH ITS] Segment fits: %d rows across %d subcorpora",
    nrow(segment_fits), length(subcorpora)
  ), verbose)

  list(
    classified_bib = bib_classified,
    annual_counts  = annual_counts,
    segment_fits   = segment_fits,
    breakpoints    = breakpoints
  )
}

#' Plot Mesh Interrupted Time-Series
#'
#' @description
#' Faceted line chart with three panels — one per subcorpus (mesh/sling
#' repair, mesh explant/complication, non-mesh urogynecology) — showing
#' annual publication counts from the start of the corpus to the most
#' recent year. Vertical dashed lines mark each regulatory breakpoint.
#' Segment-specific slope (publications/year) and p-value are annotated
#' on each panel, making the size and statistical significance of any
#' policy shock immediately readable.
#'
#' @section Reading the figure:
#' \itemize{
#'   \item A \strong{negative slope} in the mesh/sling repair panel
#'     after a breakpoint indicates that regulatory action suppressed
#'     primary-procedure research output.
#'   \item A \strong{positive slope} in the explant/complication panel
#'     after a breakpoint indicates that adverse-event and removal
#'     research expanded in response.
#'   \item The \strong{non-mesh urogyn} panel serves as a concurrent
#'     control: a flat or rising trend confirms that the drop in mesh
#'     papers is not simply a global urogynecology publication decline.
#' }
#'
#' @param mesh_its_result Named list produced by
#'   \code{\link{compute_mesh_its}()}. Must contain elements
#'   \code{annual_counts}, \code{segment_fits}, and \code{breakpoints}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object. Each facet has a free y-axis scale
#'   (\code{scales = "free_y"}) so differences in corpus size between
#'   subcorpora do not obscure within-subcorpus trends.
#'
#' @examples
#' \dontrun{
#' its <- compute_mesh_its(pipeline_result$bibliography)
#' fig <- plot_mesh_its(its, verbose = FALSE)
#' ggplot2::ggsave("mesh_its.pdf", fig, width = 8, height = 9)
#'
#' # With sensitivity breakpoints
#' its2 <- compute_mesh_its(
#'   pipeline_result$bibliography,
#'   breakpoints = c("2011 FDA" = 2011L, "2016 FDA" = 2016L,
#'                   "2018 UK" = 2018L, "2019 FDA" = 2019L)
#' )
#' plot_mesh_its(its2)
#' }
#'
#' @seealso
#' \code{\link{compute_mesh_its}} for the underlying computation and
#'   a description of the three subcorpora;
#' \code{\link{plot_thematic_evolution}} for keyword-level evidence of
#'   the same mesh-to-rehabilitation thematic shift;
#' \code{\link{run_fpmrs_bibliometric_pipeline}} which produces the
#'   bibliography passed to \code{compute_mesh_its}.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr mutate left_join filter group_by summarise
#' @importFrom ggplot2 ggplot aes geom_line geom_point geom_vline
#'   geom_text facet_wrap scale_color_manual scale_y_continuous
#'   expansion labs
#' @importFrom scales label_comma
#' @importFrom rlang .data
#' @export
plot_mesh_its <- function(mesh_its_result, verbose = TRUE) {
  assertthat::assert_that(is.list(mesh_its_result))
  assertthat::assert_that(
    all(c("annual_counts","segment_fits","breakpoints") %in%
          names(mesh_its_result)),
    msg = "Pass the output of compute_mesh_its() directly."
  )
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step("[PLOT] Building mesh ITS figure ...", verbose)

  annual   <- mesh_its_result$annual_counts
  fits     <- mesh_its_result$segment_fits
  bps      <- mesh_its_result$breakpoints

  subcorpus_colours <- c(
    "Mesh/sling repair"          = "#D7191C",
    "Mesh explant/complication"  = "#FDAE61",
    "Non-mesh urogyn"            = "#2C7BB6"
  )
  subcorpus_order <- c(
    "Mesh/sling repair",
    "Mesh explant/complication",
    "Non-mesh urogyn"
  )

  plot_data <- annual |>
    dplyr::mutate(
      mesh_subcorpus = factor(.data$mesh_subcorpus,
                               levels = subcorpus_order)
    )

  # Slope annotation labels per segment per subcorpus.
  # Guard: segment_fits can be 0 rows (e.g. single-year corpus).
  slope_labels <- if (nrow(fits) == 0L) {
    dplyr::tibble(
      mesh_subcorpus = factor(character(), levels = subcorpus_order),
      label_x        = numeric(),
      label_y        = numeric(),
      slope_label    = character()
    )
  } else {
    fits |>
      dplyr::mutate(
        slope_label = sprintf(
          "%+.1f/yr\n(p%s)",
          .data$slope_papers_yr,
          dplyr::case_when(
            .data$p_value < 0.001 ~ "<.001",
            .data$p_value < 0.10  ~ sprintf("=%.3f", .data$p_value),
            TRUE                   ~ sprintf("=%.2f", .data$p_value)
          )
        ),
        label_x = (.data$seg_start + .data$seg_end) / 2,
        mesh_subcorpus = factor(.data$mesh_subcorpus,
                                 levels = subcorpus_order)
      ) |>
      dplyr::left_join(
        annual |>
          dplyr::group_by(.data$mesh_subcorpus) |>
          dplyr::summarise(y_max = max(.data$n_papers, na.rm = TRUE),
                            .groups = "drop"),
        by = "mesh_subcorpus"
      ) |>
      dplyr::mutate(label_y = .data$y_max * 0.9)
  }

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x     = .data$publication_year,
      y     = .data$n_papers,
      color = .data$mesh_subcorpus
    )
  ) +
    ggplot2::geom_line(linewidth = 1.0) +
    ggplot2::geom_point(size = 2.2) +
    # Regulatory breakpoint lines
    ggplot2::geom_vline(
      xintercept = as.numeric(bps),
      linetype   = "dashed",
      color      = "grey40",
      linewidth  = 0.7
    ) +
    # Breakpoint labels at top of plot
    ggplot2::geom_text(
      data = dplyr::tibble(
        x     = as.numeric(bps),
        label = names(bps)
      ),
      ggplot2::aes(x = .data$x, y = Inf, label = .data$label),
      inherit.aes = FALSE,
      vjust = 1.3, hjust = 0.5, size = 2.2, color = "grey30",
      angle = 0
    ) +
    # Slope annotations
    ggplot2::geom_text(
      data = slope_labels,
      ggplot2::aes(
        x     = .data$label_x,
        y     = .data$label_y,
        label = .data$slope_label
      ),
      inherit.aes = FALSE,
      size = 2.5, color = "grey20", hjust = 0.5
    ) +
    ggplot2::scale_color_manual(
      values = subcorpus_colours, guide = "none"
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(),
      expand = ggplot2::expansion(mult = c(0, 0.2))
    ) +
    ggplot2::facet_wrap(
      ~ mesh_subcorpus,
      ncol   = 1,
      scales = "free_y"
    ) +
    ggplot2::labs(
      title    = "Mesh Policy Shock: Publication Trends by Subcorpus",
      subtitle = paste0(
        "Dashed lines: ",
        paste(names(bps), collapse = " | ")
      ),
      x        = "Publication Year",
      y        = "Annual Publications",
      caption  = paste(
        "Subcorpora classified from title (TI) and keyword (DE) fields.",
        "Slope and p-value shown per segment between regulatory events."
      )
    ) +
    .theme_fpmrs_manuscript()

  .log_step("[PLOT] Mesh ITS figure complete.", verbose)
  fig
}

# ============================================================
# ADDITION 3: CITATION PARENTAGE — WHERE DOES FPMRS CITE FROM?
# ============================================================

#' @noRd
.parse_cited_journal <- function(cr_string) {
  # Extract journal name from a single bibliometrix CR string.
  # CR format: "AUTHOR, YEAR, JOURNAL, VVOL, PPAGE"
  if (is.na(cr_string) || nchar(trimws(cr_string)) == 0L) {
    return(character(0))
  }
  refs <- trimws(strsplit(cr_string, ";")[[1L]])
  refs <- refs[nchar(refs) > 0L]
  purrr::map_chr(refs, function(r) {
    parts <- strsplit(r, ",")[[1L]]
    if (length(parts) >= 3L) toupper(trimws(parts[[3L]])) else NA_character_
  })
}

#' Compute Citation Parentage — Which Journals Does the Corpus Cite?
#'
#' @description
#' Parses the \code{CR} (cited references) field of every paper in the
#' bibliography to extract the journals being cited, aggregates those
#' citations by journal, and tracks how the source mix shifts across four
#' publication eras. A field-classification step assigns each cited
#' journal to one of seven research domains (urogynecology/pelvic floor,
#' OB/GYN general, urology, surgery, rehabilitation/PT, general medicine,
#' basic science), enabling a stacked-bar view of intellectual lineage
#' over time.
#'
#' @section Why citation parentage matters:
#' Keyword analysis tells you what authors write \emph{about}; citation
#' parentage tells you what authors \emph{read}. For FPMRS the key
#' question is whether the post-2018 rise of PFMT as a keyword hotspot
#' is accompanied by an actual increase in citations to physical therapy
#' and rehabilitation journals. If the rehabilitation slice does not grow
#' in \code{by_era}, the keyword trend may reflect nomenclature adoption
#' rather than genuine intellectual engagement with that literature.
#'
#' @section CR field format:
#' bibliometrix stores cited references in a single semicolon-delimited
#' string per paper in the format
#' \code{"SURNAME I, YEAR, JOURNAL, VOLUME, PAGE"}. Journal names are
#' typically abbreviated (e.g., \code{"INT UROGYNECOL J"}).
#' Classification patterns in this function match on uppercase
#' abbreviations; extend \code{field_patterns} inside the function body
#' for custom domains.
#'
#' @param bibliography A data frame containing at minimum:
#'   \describe{
#'     \item{\code{CR}}{Character column of semicolon-delimited cited
#'       references in bibliometrix format.}
#'     \item{\code{publication_year}}{Integer column of publication year,
#'       used to assign each paper to an era.}
#'   }
#' @param top_n Positive integer. Number of most-cited journals to retain
#'   in the overall and per-era ranking. Journals outside the top
#'   \code{top_n} per era are summed into the \code{field_classification}
#'   but are not returned individually in \code{by_era}. Defaults to
#'   \code{20L}.
#' @param eras Named list where each element is
#'   \code{c(start_year, end_year)}. Names become era labels in
#'   \code{by_era}. Defaults to the standard four-era breakdown.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A named list with three elements:
#' \describe{
#'   \item{\code{overall_top}}{Tibble with columns \code{cited_journal}
#'     and \code{n_citations}, sorted descending, limited to
#'     \code{top_n} rows.}
#'   \item{\code{by_era}}{Tibble with columns \code{cited_journal},
#'     \code{n_citations}, \code{era}, \code{pct_of_era_cites},
#'     \code{n_era_cites_total} — one row per journal × era combination
#'     within the top \code{top_n} per era.}
#'   \item{\code{field_classification}}{Tibble with columns
#'     \code{cited_journal} and \code{field_category} for every journal
#'     appearing in \code{overall_top} or \code{by_era}.}
#' }
#'
#' @examples
#' \dontrun{
#' par <- compute_citation_parentage(
#'   bibliography = pipeline_result$bibliography,
#'   top_n        = 25L,
#'   verbose      = FALSE
#' )
#'
#' # Inspect top-cited journals overall
#' par$overall_top
#'
#' # Check whether physical therapy journals grew post-2018
#' library(dplyr)
#' par$by_era |>
#'   left_join(par$field_classification, by = "cited_journal") |>
#'   filter(field_category == "Rehabilitation / PT") |>
#'   arrange(era)
#'
#' # Plot
#' plot_citation_parentage(par)
#' }
#'
#' @seealso
#' \code{\link{plot_citation_parentage}} for the stacked-bar
#'   visualisation of the domain mix by era;
#' \code{\link{run_fpmrs_bibliometric_pipeline}} which produces the
#'   \code{bibliography} argument (the \code{CR} field is preserved
#'   from the original export);
#' \code{\link{compute_mesh_its}} for a complementary analysis of
#'   how the mesh subcorpus shifted in response to regulatory events;
#' \code{\link{plot_thematic_evolution}} for the keyword-side view of
#'   the same surgical-to-rehabilitation intellectual shift.
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr tibble count arrange desc slice_head mutate
#'   filter group_by summarise left_join
#' @importFrom purrr map_dfr map_chr
#' @importFrom rlang .data
#' @export
compute_citation_parentage <- function(
    bibliography,
    top_n = 20L,
    eras  = NULL,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.count(top_n))
  assertthat::assert_that(assertthat::is.flag(verbose))
  if (is.null(eras)) {
    yr_range <- range(bibliography$publication_year, na.rm = TRUE)
    eras     <- make_eras(yr_range[1L], yr_range[2L])
  }
  assertthat::assert_that(
    "CR" %in% names(bibliography),
    msg = "`bibliography` must contain a 'CR' column."
  )
  assertthat::assert_that(
    "publication_year" %in% names(bibliography),
    msg = "`bibliography` must contain 'publication_year'."
  )

  .log_step(
    sprintf("[PARENTAGE] Parsing cited journals from %d papers ...",
            nrow(bibliography)),
    verbose
  )

  # ---- overall: all cited journals ----
  all_cited <- unlist(lapply(bibliography$CR, .parse_cited_journal))
  all_cited <- all_cited[!is.na(all_cited) & nchar(all_cited) > 0L]

  overall_top <- dplyr::tibble(cited_journal = all_cited) |>
    dplyr::count(.data$cited_journal, name = "n_citations") |>
    dplyr::arrange(dplyr::desc(.data$n_citations)) |>
    dplyr::slice_head(n = top_n)

  if (nrow(overall_top) == 0L) {
    .log_step("[PARENTAGE] No parseable cited references found.", verbose)
  } else {
    .log_step(sprintf(
      "[PARENTAGE] Top cited journal: %s (%d citations)",
      overall_top$cited_journal[[1L]], overall_top$n_citations[[1L]]
    ), verbose)
  }

  # ---- by era ----
  by_era <- purrr::map_dfr(names(eras), function(era_nm) {
    yr      <- eras[[era_nm]]
    era_bib <- dplyr::filter(bibliography,
      .data$publication_year >= yr[1L],
      .data$publication_year <= yr[2L]
    )
    if (nrow(era_bib) == 0L) return(dplyr::tibble())

    era_cited <- unlist(lapply(era_bib$CR, .parse_cited_journal))
    era_cited <- era_cited[!is.na(era_cited) & nchar(era_cited) > 0L]
    n_total   <- length(era_cited)
    if (n_total == 0L) return(dplyr::tibble())

    dplyr::tibble(cited_journal = era_cited) |>
      dplyr::count(.data$cited_journal, name = "n_citations") |>
      dplyr::arrange(dplyr::desc(.data$n_citations)) |>
      dplyr::slice_head(n = top_n) |>
      dplyr::mutate(
        era              = era_nm,
        pct_of_era_cites = round(.data$n_citations / n_total * 100, 2L),
        n_era_cites_total = n_total
      )
  })

  # ---- field classification ----
  # Classify cited journals into broad research domains.
  field_patterns <- list(
    # Most specific patterns first to prevent broad matches swallowing
    # journals that belong to a narrower domain.
    # NEUROUROL URODYN (Neurourology and Urodynamics) is a urology/
    # pelvic floor journal — listed under Urology explicitly to prevent
    # the broad NEUROUROL token in the Urogyn pattern from claiming it.
    "Urology" = paste(
      "^J UROL$|^UROLOGY$|^BJU INT|^EUR UROL|NEUROUROL URODYN",
      "UROL CLIN|CAN J UROL|UROL ANN|UROL ONCOL", sep = "|"
    ),
    "Urogynecology / Pelvic floor" = paste(
      "UROGYNECOL|PELVIC FLOOR|INT UROGYN|FEMALE PELVIC",
      "PELVIPERINEOLOGY|NEUROUROL UROGYN", sep = "|"
    ),
    "OB/GYN (general)"   = paste(
      "OBSTET GYNECOL|AM J OBSTET|BJOG|ACTA OBSTET|ARCH GYNECOL",
      "EUR J OBSTET|GYNECOL OBSTET|INT J GYNECOL", sep = "|"
    ),
    "Rehabilitation / PT" = paste(
      "PHYS THER|PHYSIOTHER|REHABIL|^CONTINENCE$|INT CONT",
      "J WOMENS HEALTH", sep = "|"
    ),
    "Surgery (general)"  = paste(
      "SURGERY|SURG ENDOSC|ANN SURG|J AM COLL SURG|WORLD J SURG",
      "LAPAROENDOSC", sep = "|"
    ),
    "General medicine"   = paste(
      "N ENGL J MED|LANCET|JAMA|^BMJ$|ANN INTERN MED|COCHRANE",
      "BRIT MED J", sep = "|"
    ),
    "Basic science"      = paste(
      "AM J PHYSIOL|J CELL|^CELL$|^NATURE$|^SCIENCE$|PLOS|FASEB",
      "BIOMATERIALS|TISSUE ENG", sep = "|"
    )
  )

  # Guard: by_era may be a 0x0 tibble (all eras produced 0 citations).
  # Accessing $cited_journal on a 0-column tibble silently warns.
  by_era_journals <- if ("cited_journal" %in% names(by_era))
    by_era$cited_journal else character(0L)
  top_journals_vec <- unique(c(
    overall_top$cited_journal, by_era_journals
  ))

  classify_journal <- function(j) {
    for (field in names(field_patterns)) {
      if (grepl(field_patterns[[field]], j)) return(field)
    }
    "Other / Not classified"
  }

  field_classification <- dplyr::tibble(
    cited_journal  = top_journals_vec,
    field_category = purrr::map_chr(top_journals_vec, classify_journal)
  )

  .log_step("[PARENTAGE] Citation parentage complete.", verbose)

  list(
    overall_top        = overall_top,
    by_era             = by_era,
    field_classification = field_classification
  )
}

#' Plot Citation Parentage by Research Domain and Era
#'
#' @description
#' Stacked 100\% bar chart showing how the mix of research domains cited
#' by the focal subspecialty has shifted across four publication eras.
#' Bars are coloured by domain (urogynecology/pelvic floor, OB/GYN
#' general, urology, surgery, rehabilitation/PT, general medicine, basic
#' science, other). Percentage labels appear inside bars for slices
#' \eqn{\geq 5\%}.
#'
#' @section What to look for:
#' \itemize{
#'   \item A \strong{growing Rehabilitation/PT slice} after 2014--2018
#'     is the strongest signal that the field is genuinely engaging with
#'     conservative-management science, not just adopting PFMT terminology.
#'   \item A \strong{shrinking Urology slice} accompanied by a growing
#'     OB/GYN general slice suggests subspecialty maturation and reduced
#'     dependence on the parent field.
#'   \item A persistently \strong{dominant Urogynecology/Pelvic floor
#'     slice} is expected and healthy, confirming internal cohesion.
#'   \item A growing \strong{Basic science slice} signals translational
#'     interest, particularly relevant to mesh biomaterials research.
#' }
#'
#' @param parentage_result Named list produced by
#'   \code{\link{compute_citation_parentage}()}. Must contain elements
#'   \code{by_era} and \code{field_classification}.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A \code{ggplot2} object. Each era occupies one bar; domains
#'   are stacked from bottom (urogynecology) to top (other).
#'
#' @examples
#' \dontrun{
#' par <- compute_citation_parentage(
#'   bibliography = pipeline_result$bibliography,
#'   top_n = 20L, verbose = FALSE
#' )
#' fig <- plot_citation_parentage(par, verbose = FALSE)
#' ggplot2::ggsave("citation_parentage.pdf", fig, width = 7, height = 5)
#' }
#'
#' @seealso
#' \code{\link{compute_citation_parentage}} for the underlying data
#'   and a description of the domain classification scheme;
#' \code{\link{plot_evidence_evolution}} for the complementary
#'   evidence-pyramid view of what study types are being \emph{published}
#'   (vs what is being cited here);
#' \code{\link{plot_mesh_its}} for the mesh policy shock view, which
#'   provides a temporal narrative complementary to this domain map.
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr left_join group_by summarise mutate ungroup
#'   coalesce if_else
#' @importFrom ggplot2 ggplot aes geom_col geom_text scale_fill_brewer
#'   scale_y_continuous position_stack expansion labs
#' @importFrom rlang .data
#' @export
plot_citation_parentage <- function(parentage_result, verbose = TRUE) {
  assertthat::assert_that(is.list(parentage_result))
  assertthat::assert_that(
    all(c("by_era","field_classification") %in% names(parentage_result)),
    msg = "Pass the output of compute_citation_parentage() directly."
  )
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step("[PLOT] Building citation parentage figure ...", verbose)

  # Derive era order dynamically from the data itself.
  # IMPORTANT: do NOT use a hardcoded intersect list — make_eras()
  # produces different era names depending on year_start (e.g. it
  # generates "2008\u20132018" for pre-1990 corpora, but "2008\u20132012"
  # for 2000-start corpora). A hardcoded list silently drops any era
  # name it does not recognise, causing entire decades to vanish from
  # the figure.
  era_order <- if (nrow(parentage_result$by_era) > 0L) {
    eras_present <- unique(parentage_result$by_era$era)
    # Sort by the four-digit year embedded at the start of each label
    era_start_yr <- suppressWarnings(
      as.integer(stringr::str_extract(eras_present, "^[0-9]{4}"))
    )
    eras_present[order(era_start_yr, na.last = TRUE)]
  } else character(0L)

  plot_data <- parentage_result$by_era |>
    dplyr::left_join(parentage_result$field_classification,
                      by = "cited_journal") |>
    dplyr::mutate(
      field_category = dplyr::coalesce(
        .data$field_category, "Other / Not classified"
      ),
      era = factor(.data$era, levels = era_order)
    ) |>
    dplyr::group_by(.data$era, .data$field_category) |>
    dplyr::summarise(
      n_citations = sum(.data$n_citations, na.rm = TRUE),
      .groups     = "drop"
    ) |>
    dplyr::group_by(.data$era) |>
    dplyr::mutate(
      pct = round(.data$n_citations / sum(.data$n_citations) * 100, 1L)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      label_txt = dplyr::if_else(.data$pct >= 5, sprintf("%.0f%%", .data$pct), "")
    )

  fig <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(
      x    = .data$era,
      y    = .data$pct,
      fill = .data$field_category
    )
  ) +
    ggplot2::geom_col(
      position = ggplot2::position_stack(reverse = FALSE),
      width    = 0.72
    ) +
    ggplot2::geom_text(
      ggplot2::aes(label = .data$label_txt),
      position = ggplot2::position_stack(vjust = 0.5, reverse = FALSE),
      size = 2.8, color = "white", fontface = "bold"
    ) +
    ggplot2::scale_fill_brewer(
      palette = "Set2", name = "Research domain"
    ) +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      expand = ggplot2::expansion(mult = c(0, 0.03))
    ) +
    ggplot2::labs(
      title    = "Citation Parentage: Where Does FPMRS Cite From?",
      subtitle = paste(
        "Distribution of cited journals by research domain across eras.",
        "A shift toward Rehabilitation/PT signals a translational turn."
      ),
      x        = "Publication Era",
      y        = "Share of Citations to Domain (%)",
      caption  = paste(
        "Cited journal extracted from CR field (bibliometrix format).",
        "Top 20 journals per era classified by domain; remainder = Other."
      )
    ) +
    .theme_fpmrs_manuscript() +
    ggplot2::theme(
      legend.key.size = ggplot2::unit(0.45, "cm"),
      legend.text     = ggplot2::element_text(size = 7.5)
    )

  .log_step("[PLOT] Citation parentage figure complete.", verbose)
  fig
}

# ============================================================
# ADDITION 4: PLACEHOLDER — GBD BURDEN NORMALIZATION
# (requires GBD data download; implemented as a stub with
#  clear instructions for data acquisition)
# ============================================================

#' Compute Publications-per-Burden by ACOG District
#'
#' @description
#' Joins bibliometric output (publication counts by US state, aggregated
#' to ACOG district via \code{\link{add_acog_district}}) against Global
#' Burden of Disease (GBD) disability-adjusted life year (DALY) estimates
#' for pelvic floor disorders downloaded from the IHME GBD Results Tool.
#' Computes a \strong{publications-per-1,000-DALY index} per district: a
#' low value identifies a \emph{research desert} — a district bearing high
#' disease burden but producing disproportionately little literature.
#'
#' @section Methodological rationale:
#' Standard bibliometric concentration analyses (country/institution
#' rankings by volume) do not account for the underlying population at
#' risk. Two districts may publish the same number of papers, yet one
#' may serve a population with three times the pelvic floor disease
#' burden. The publications-per-1,000-DALY index normalises output by
#' need, enabling a defensible equity argument: districts below the
#' national median index are under-researched relative to their burden,
#' regardless of their absolute publication counts.
#'
#' @section Data acquisition:
#' GBD data must be downloaded manually from the IHME GBD Results Tool
#' (\url{https://vizhub.healthdata.org/gbd-results/}). Use the
#' following selections:
#' \itemize{
#'   \item \strong{Measure}: DALYs (Disability-Adjusted Life Years)
#'   \item \strong{Cause}: Search \dQuote{pelvic floor} or use
#'     \dQuote{Urinary diseases and male infertility} + \dQuote{Other
#'     musculoskeletal disorders} which captures pelvic floor in GBD 2021.
#'   \item \strong{Location}: United States — select individual states
#'     (not the national aggregate).
#'   \item \strong{Year}: Most recent available (GBD 2021 as of 2025).
#'   \item \strong{Age}: All Ages
#'   \item \strong{Sex}: Female
#'   \item \strong{Metric}: Rate or Number — use Number for DALY totals.
#' }
#' Export as CSV and pass the file path to \code{gbd_csv_path}.
#' Column names vary between GBD releases; this function auto-detects
#' \code{cause}, \code{location}, and \code{val}/\code{mean}/\code{value}
#' columns.
#'
#' @section Limitations:
#' \itemize{
#'   \item GBD cause definitions do not map perfectly onto FPMRS
#'     clinical scope; \code{gbd_cause_filter} may require tuning for
#'     your specific research question.
#'   \item DALY estimates are modelled quantities with uncertainty
#'     intervals; for a rigorous analysis, propagate the GBD uncertainty
#'     bounds through the index calculation.
#'   \item Publication counts are assigned to districts via state of
#'     institutional affiliation (\code{AU_ST} or the state embedded in
#'     \code{AU_UN}), not by the state of patient care. Highly
#'     centralised academic centres (e.g., Cleveland Clinic in District V)
#'     may inflate that district's count.
#' }
#'
#' @param publication_by_state A data frame with exactly two required
#'   columns:
#'   \describe{
#'     \item{\code{state}}{Two-letter US postal abbreviation
#'       (e.g., \code{"TX"}, \code{"CA"}).}
#'     \item{\code{n_publications}}{Integer or numeric. Number of papers
#'       attributed to that state.}
#'   }
#'   Build this from your isochrones pipeline by grouping
#'   physician-NPI records by state, or from your bibliometric data by
#'   parsing the \code{AU_UN} (author affiliation) field.
#' @param gbd_csv_path Character scalar. Absolute or relative path to the
#'   IHME GBD results CSV. The function checks \code{file.exists()} and
#'   raises an informative error with the download URL if the file is
#'   absent.
#' @param gbd_cause_filter Character vector of regular expression
#'   patterns matched case-insensitively against the GBD cause column.
#'   Defaults to terms capturing urinary, pelvic, prolapse, incontinence,
#'   fistula, and musculoskeletal causes. Broaden or narrow as needed.
#' @param verbose Logical. Defaults to \code{TRUE}.
#'
#' @return A tibble with one row per ACOG district, sorted ascending by
#'   \code{pubs_per_1000_dalys} (lowest = most under-researched first),
#'   containing:
#' \describe{
#'   \item{\code{acog_district}}{District label (e.g.,
#'     \code{"District VII"}).}
#'   \item{\code{n_publications}}{Total publications from that district.}
#'   \item{\code{total_dalys}}{Sum of GBD DALYs across states in that
#'     district, for the filtered cause set.}
#'   \item{\code{pubs_per_1000_dalys}}{Primary index:
#'     \code{n_publications / total_dalys * 1000}. \code{NA} if
#'     \code{total_dalys} is zero or unavailable.}
#'   \item{\code{burden_rank}}{Integer rank from 1 (lowest index =
#'     most under-researched) to \eqn{n} districts.}
#' }
#'
#' @examples
#' \dontrun{
#' # Build publication-by-state table from your bibliometric data
#' pub_by_state <- pipeline_result$bibliography |>
#'   dplyr::filter(!is.na(AU_ST)) |>
#'   dplyr::count(state = AU_ST, name = "n_publications")
#'
#' # Run burden normalization
#' burden_tbl <- compute_burden_normalized_output(
#'   publication_by_state = pub_by_state,
#'   gbd_csv_path         = "~/data/IHME-GBD_2021_DATA-pelvic.csv",
#'   verbose              = TRUE
#' )
#'
#' # Districts with lowest research-per-burden
#' head(burden_tbl, 4L)
#'
#' # Custom cause filter (SUI only)
#' burden_sui <- compute_burden_normalized_output(
#'   publication_by_state = pub_by_state,
#'   gbd_csv_path         = "~/data/IHME-GBD_2021_DATA-pelvic.csv",
#'   gbd_cause_filter     = c("Urinary incontinence", "stress"),
#'   verbose              = FALSE
#' )
#' }
#'
#' @seealso
#' \code{\link{add_acog_district}} which maps state abbreviations to
#'   district labels, used internally;
#' \code{\link{summarise_by_acog_district}} for aggregating any numeric
#'   metric to the district level without GBD data;
#' \code{\link{acog_district_table}} for the authoritative state →
#'   district mapping;
#' \code{\link{run_fpmrs_bibliometric_pipeline}} which produces the
#'   bibliography from which \code{publication_by_state} can be derived.
#' For the GBD data itself, see
#'   \url{https://vizhub.healthdata.org/gbd-results/} and
#'   GBD 2021 Collaborators (2024), \emph{The Lancet}.
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr left_join group_by summarise mutate arrange
#'   filter if_else row_number coalesce
#' @importFrom utils read.csv
#' @importFrom rlang .data
#' @export
compute_burden_normalized_output <- function(
    publication_by_state,
    gbd_csv_path,
    gbd_cause_filter = c(
      "Urinary", "pelvic", "prolapse", "incontinence",
      "fistula", "Other musculoskeletal"
    ),
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(publication_by_state))
  assertthat::assert_that(assertthat::is.string(gbd_csv_path))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("state","n_publications") %in% names(publication_by_state)),
    msg = "`publication_by_state` must contain 'state' and 'n_publications'."
  )
  assertthat::assert_that(
    file.exists(gbd_csv_path),
    msg = sprintf(
      "GBD file not found: '%s'.\nDownload from https://vizhub.healthdata.org/gbd-results/",
      gbd_csv_path
    )
  )

  .log_step("[BURDEN] Loading GBD DALY data ...", verbose)

  gbd_raw <- utils::read.csv(gbd_csv_path, stringsAsFactors = FALSE)
  .log_step(sprintf("[BURDEN] GBD file: %d rows, %d columns",
                    nrow(gbd_raw), ncol(gbd_raw)), verbose)

  # Flexible column detection — IHME changes column names between releases
  cause_col    <- names(gbd_raw)[grepl("cause",    names(gbd_raw),
                                        ignore.case = TRUE)][1L]
  location_col <- names(gbd_raw)[grepl("location", names(gbd_raw),
                                        ignore.case = TRUE)][1L]
  val_col      <- names(gbd_raw)[grepl("^val$|mean|value",names(gbd_raw),
                                        ignore.case = TRUE)][1L]

  assertthat::assert_that(
    !is.na(cause_col) && !is.na(location_col) && !is.na(val_col),
    msg = paste(
      "Could not detect cause/location/value columns in GBD CSV.",
      "Expected columns matching 'cause', 'location', and 'val' or 'mean'.",
      sprintf("Columns found: %s", paste(names(gbd_raw), collapse = ", "))
    )
  )

  # Filter to pelvic floor causes
  cause_pattern <- paste(gbd_cause_filter, collapse = "|")
  gbd_filtered  <- gbd_raw[
    grepl(cause_pattern, gbd_raw[[cause_col]], ignore.case = TRUE), ,
    drop = FALSE
  ]
  .log_step(sprintf("[BURDEN] After cause filter: %d rows", nrow(gbd_filtered)),
            verbose)

  # Map GBD US state names to abbreviations and then to ACOG districts
  gbd_with_state <- gbd_filtered |>
    dplyr::mutate(
      state = state_to_acog_district(
        # GBD location names are full state names (e.g. "Texas")
        .data[[location_col]]
      ) |> {\(d) names(acog_district_vector)[
        match(d, acog_district_vector)
      ]}()
    ) |>
    add_acog_district(state_col = "state", verbose = FALSE) |>
    dplyr::filter(!is.na(.data$acog_district))

  # Aggregate DALYs to district
  daly_by_district <- gbd_with_state |>
    dplyr::group_by(.data$acog_district) |>
    dplyr::summarise(
      total_dalys = sum(as.numeric(.data[[val_col]]), na.rm = TRUE),
      .groups     = "drop"
    )

  # Add ACOG district to publication data
  pubs_with_district <- publication_by_state |>
    add_acog_district(state_col = "state", verbose = FALSE) |>
    dplyr::filter(!is.na(.data$acog_district)) |>
    dplyr::group_by(.data$acog_district) |>
    dplyr::summarise(
      n_publications = sum(.data$n_publications, na.rm = TRUE),
      .groups        = "drop"
    )

  # Join and compute index
  result <- pubs_with_district |>
    dplyr::left_join(daly_by_district, by = "acog_district") |>
    dplyr::mutate(
      pubs_per_1000_dalys = dplyr::if_else(
        !is.na(.data$total_dalys) & .data$total_dalys > 0,
        round(.data$n_publications / .data$total_dalys * 1000, 3L),
        NA_real_
      )
    ) |>
    dplyr::arrange(.data$pubs_per_1000_dalys) |>
    dplyr::mutate(
      burden_rank = dplyr::row_number()
    )

  .log_step(sprintf(
    "[BURDEN] Index computed for %d districts. Lowest: %s (%.3f pubs/1000 DALY)",
    nrow(result),
    result$acog_district[[1L]],
    dplyr::coalesce(result$pubs_per_1000_dalys[[1L]], 0)
  ), verbose)

  result
}


# ============================================================
# ============================================================
#
# SECTION 10: JAMA REPOSITORY PATTERNS
#
# Priorities 1, 2, 3, 4, 5, 6, 8, 9, 10 from:
#   mkiang/opioid_geographic   (P1 YAML, P2 bivariate, P4 geo)
#   ferenci-tamas/MortalityPrediction (P3 NB, P5 simulation)
#   hollina/duke-replication   (P6 README, P8 naming, P10 session)
#
# All functions exported; helpers prefixed with .
# ============================================================
# ============================================================


# ============================================================
# P1: YAML-BACKED CONFIGURATION
# Source: mkiang/opioid_geographic config.yml pattern
# ============================================================

#' @noRd
.pipeline_config_defaults <- function() {
  list(
    year_start          = 1975L,
    year_end            = as.integer(format(Sys.Date(), "%Y")),
    focal_subspecialty  = "FPMRS",
    alpha               = 0.05,
    correction_method   = "BH",
    seed                = 42L,
    top_n_authors       = 20L,
    top_n_journals      = 8L,
    top_n_keywords      = 20L,
    top_n_countries     = 10L,
    top_n_institutions  = 20L,
    figure_format       = "pdf",
    figure_width        = 9,
    figure_height       = 6,
    figure_dpi          = 300L,
    figure_base_size    = 11L,
    geo_rate_cuts       = c(5, 15),
    geo_trend_cuts      = c(-0.5, 0.5),
    db_path             = "data/cache/biblio_cache.duckdb",
    cache_ttl_days      = 7L,
    force_rerun         = FALSE,
    rds_cache_dir       = "data/cache/rds",
    proc_in_parallel    = FALSE,
    n_cores             = NULL,
    ram_per_core_gb     = 4,
    sim_n_datasets      = 500L,
    sim_true_theta      = 15,
    sim_true_slope      = 0.05,
    output_dir          = "output",
    figures_dir         = "output/figures",
    tables_dir          = "output/tables",
    manifests_dir       = "output/manifests",
    logs_dir            = "logs",
    verbose             = TRUE
  )
}

#' Load Pipeline Configuration from YAML or Defaults
#'
#' @description
#' Loads analysis parameters from a \code{config.yml} file using the
#' \pkg{config} package (mkiang/opioid_geographic pattern). Falls back to
#' hard-coded defaults when \pkg{config} is not installed, so the pipeline
#' remains runnable without the package.
#'
#' The active environment is controlled by the \code{R_CONFIG_ACTIVE}
#' environment variable (\code{"default"}, \code{"test"}, or
#' \code{"production"}).
#'
#' @param config_file Character. Path to \code{config.yml}. If \code{NULL}
#'   (default), looks in the project root via \code{here::here()}.
#' @param verbose Logical. Print confirmation message.
#'
#' @return A named list of all configuration values.
#'
#' @examples
#' # Load from config.yml in project root (if it exists)
#' cfg <- load_pipeline_config(verbose = FALSE)
#' cfg$year_start
#' cfg$focal_subspecialty
#'
#' # Override environment for test runs
#' Sys.setenv(R_CONFIG_ACTIVE = "test")
#' cfg_test <- load_pipeline_config(verbose = FALSE)
#' Sys.unsetenv("R_CONFIG_ACTIVE")
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @export
load_pipeline_config <- function(config_file = NULL,
                                  verbose      = TRUE) {
  assertthat::assert_that(assertthat::is.flag(verbose))

  # Locate config.yml
  yml_path <- if (!is.null(config_file)) {
    assertthat::assert_that(assertthat::is.string(config_file))
    config_file
  } else if (requireNamespace("here", quietly = TRUE)) {
    here::here("config.yml")
  } else {
    file.path(getwd(), "config.yml")
  }

  if (!file.exists(yml_path)) {
    .log_step(paste(
      "[CONFIG] config.yml not found at", yml_path,
      "-- using built-in defaults."
    ), verbose)
    cfg <- .pipeline_config_defaults()
    return(cfg)
  }

  if (!requireNamespace("config", quietly = TRUE)) {
    .log_step(paste(
      "[CONFIG] 'config' package not installed.",
      "Install with: install.packages('config')",
      "Using built-in defaults."
    ), verbose)
    return(.pipeline_config_defaults())
  }

  cfg <- config::get(file = yml_path)

  # Merge with defaults so any omitted keys are filled in
  defaults    <- .pipeline_config_defaults()
  merged_keys <- union(names(defaults), names(cfg))
  cfg_merged  <- stats::setNames(
    purrr::map(merged_keys, function(k) {
      if (!is.null(cfg[[k]])) cfg[[k]] else defaults[[k]]
    }),
    merged_keys
  )

  env_name <- Sys.getenv("R_CONFIG_ACTIVE", unset = "default")
  .log_step(sprintf(
    "[CONFIG] Loaded (%s): years %d-%d | focal=%s | seed=%d",
    env_name, cfg_merged$year_start, cfg_merged$year_end,
    cfg_merged$focal_subspecialty, cfg_merged$seed
  ), verbose)

  cfg_merged
}


# ============================================================
# P2: BIVARIATE HOTSPOT CLASSIFICATION  (rate x trend)
# Source: mkiang/opioid_geographic Figure 1 approach
# ============================================================

#' Classify Locations on Publication Rate × Annual Trend
#'
#' @description
#' Creates a 3×3 bivariate classification combining current publication
#' volume and annual growth direction (Joshua Stevens colour scheme).
#' Substantially more informative than a single-variable choropleth.
#' Adapted from mkiang/opioid_geographic \code{09_fig1_current_hotspots.R}.
#'
#' @param location_data Data frame with one row per location.
#' @param rate_col  Character. Column with publication rate.
#' @param slope_col Character. Column with Theil-Sen annual slope.
#' @param rate_cuts  Numeric(2). Low→mid and mid→high boundaries.
#' @param slope_cuts Numeric(2). Declining→stable and stable→growing.
#' @param rate_labels  Character(3). Low / Medium / High labels.
#' @param slope_labels Character(3). Declining / Stable / Growing labels.
#' @param verbose Logical.
#'
#' @return \code{location_data} augmented with columns:
#'   \code{rate_cat}, \code{trend_cat}, \code{bivar_class},
#'   \code{bivar_colour}.
#'
#' @examples
#' state_df <- data.frame(
#'   state      = c("TX","CA","NY","FL"),
#'   pub_rate   = c(3.2, 18.5, 9.1, 7.0),
#'   ts_slope   = c(0.8, -0.3, 0.1, 1.2)
#' )
#' out <- classify_bivariate_hotspot(
#'   state_df, rate_col="pub_rate", slope_col="ts_slope",
#'   rate_cuts=c(5,15), slope_cuts=c(-0.5,0.5), verbose=FALSE
#' )
#' table(out$bivar_class)
#'
#' out2 <- classify_bivariate_hotspot(
#'   state_df, rate_col="pub_rate", slope_col="ts_slope",
#'   rate_cuts=c(4,12), slope_cuts=c(-1,1), verbose=FALSE
#' )
#' out2[, c("state","rate_cat","trend_cat","bivar_colour")]
#'
#' # Use bivar_colour directly in ggplot2:
#' # ggplot(out, aes(state, 1, fill=bivar_colour)) +
#' #   geom_tile() + scale_fill_identity()
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr mutate
#' @export
classify_bivariate_hotspot <- function(
    location_data,
    rate_col    = "pub_rate",
    slope_col   = "theil_sen_slope",
    rate_cuts   = c(5, 15),
    slope_cuts  = c(-0.5, 0.5),
    rate_labels  = c("Low",      "Medium",  "High"),
    slope_labels = c("Declining","Stable",  "Growing"),
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(location_data))
  assertthat::assert_that(assertthat::is.string(rate_col))
  assertthat::assert_that(assertthat::is.string(slope_col))
  assertthat::assert_that(
    rate_col %in% names(location_data),
    msg = sprintf("Column '%s' not found in location_data.", rate_col))
  assertthat::assert_that(
    slope_col %in% names(location_data),
    msg = sprintf("Column '%s' not found in location_data.", slope_col))
  assertthat::assert_that(length(rate_cuts)  == 2L)
  assertthat::assert_that(length(slope_cuts) == 2L)
  assertthat::assert_that(assertthat::is.flag(verbose))

  # Joshua Stevens 3×3 bivariate colour palette
  # rows = trend (Declining/Stable/Growing), cols = rate (Low/Med/High)
  bivar_palette <- c(
    "Declining.Low"    = "#e8e8e8",
    "Declining.Medium" = "#b0d5df",
    "Declining.High"   = "#64acbe",
    "Stable.Low"       = "#e4d9ac",
    "Stable.Medium"    = "#ad9ea5",
    "Stable.High"      = "#627f8c",
    "Growing.Low"      = "#c85a5a",
    "Growing.Medium"   = "#985356",
    "Growing.High"     = "#574249"
  )

  result <- location_data |>
    dplyr::mutate(
      rate_cat = cut(.data[[rate_col]],
                     breaks = c(-Inf, rate_cuts, Inf),
                     labels = rate_labels,
                     include.lowest = TRUE),
      trend_cat = cut(.data[[slope_col]],
                      breaks = c(-Inf, slope_cuts, Inf),
                      labels = slope_labels,
                      include.lowest = TRUE),
      bivar_class  = interaction(.data$trend_cat, .data$rate_cat,
                                 sep = ".", drop = FALSE),
      bivar_colour = bivar_palette[as.character(.data$bivar_class)]
    )

  n_hot <- sum(result$bivar_class == "Growing.High",  na.rm = TRUE)
  n_col <- sum(result$bivar_class == "Declining.High", na.rm = TRUE)
  .log_step(sprintf(
    "[BIVAR] %d hotspots (Growing+High rate), %d cold spots (Declining+High rate)",
    n_hot, n_col), verbose)

  result
}


#' Build Bivariate Legend as a ggplot Object
#'
#' @description
#' Returns a stand-alone 3×3 tile legend for embedding alongside a
#' bivariate map using \pkg{patchwork} or \code{cowplot::ggdraw}.
#'
#' @param rate_labels  Character(3). X-axis labels (Low/Medium/High).
#' @param slope_labels Character(3). Y-axis labels (Declining/Stable/Growing).
#' @param base_size    Integer. ggplot2 base font size.
#'
#' @return A \code{ggplot} object.
#'
#' @examples
#' leg <- build_bivariate_legend()
#' # embed with patchwork:
#' # main_map + patchwork::inset_element(leg, 0.02, 0.02, 0.22, 0.22)
#'
#' leg2 <- build_bivariate_legend(
#'   rate_labels  = c("Low","Mod","High"),
#'   slope_labels = c("Down","Flat","Up")
#' )
#'
#' leg3 <- build_bivariate_legend(base_size = 6L)
#'
#' @importFrom ggplot2 ggplot aes geom_tile scale_fill_identity
#'   labs theme_minimal theme element_text element_blank
#' @export
build_bivariate_legend <- function(
    rate_labels  = c("Low","Medium","High"),
    slope_labels = c("Declining","Stable","Growing"),
    base_size    = 8L
) {
  assertthat::assert_that(length(rate_labels)  == 3L)
  assertthat::assert_that(length(slope_labels) == 3L)

  palette_vec <- c(
    "#e8e8e8","#b0d5df","#64acbe",
    "#e4d9ac","#ad9ea5","#627f8c",
    "#c85a5a","#985356","#574249"
  )

  legend_df <- expand.grid(
    rate  = factor(rate_labels,  levels = rate_labels),
    trend = factor(slope_labels, levels = slope_labels),
    stringsAsFactors = FALSE
  ) |>
    dplyr::mutate(fill = palette_vec)

  ggplot2::ggplot(legend_df,
    ggplot2::aes(x = .data$rate, y = .data$trend,
                 fill = .data$fill)) +
    ggplot2::geom_tile(colour = "white", linewidth = 0.6) +
    ggplot2::scale_fill_identity() +
    ggplot2::labs(x = "Publication rate \u2192",
                  y = "Annual trend \u2192",
                  title = "Bivariate key") +
    ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(
      axis.text      = ggplot2::element_text(size = base_size - 1L),
      axis.title     = ggplot2::element_text(size = base_size - 1L,
                                             face = "italic"),
      plot.title     = ggplot2::element_text(size = base_size,
                                             face = "bold"),
      panel.grid     = ggplot2::element_blank(),
      plot.background = ggplot2::element_rect(fill = "white",
                                              colour = NA)
    )
}


# ============================================================
# P3: NB GLM SPECIFICATION VALIDATION  (mgcv comparison)
# Source: ferenci-tamas/MortalityPrediction
# ============================================================

#' Compare Negative Binomial GLM Specifications
#'
#' @description
#' Fits four candidate models to annual publication counts and returns
#' an AIC/BIC comparison table, enabling validation that the pipeline's
#' \code{MASS::glm.nb} specification is not substantially mis-specified.
#' Inspired by the ferenci-tamas/MortalityPrediction finding that
#' over-flexible splines inflated WHO excess mortality estimates.
#'
#' Models fitted:
#' \describe{
#'   \item{M1_linear_ols}{Log-linear OLS — simplest baseline}
#'   \item{M2_glm_nb}{MASS::glm.nb — current pipeline method}
#'   \item{M3_gam_nb_reml}{mgcv::gam NB with thin-plate spline (REML)}
#'   \item{M4_poisson}{Poisson GLM — NB special case}
#' }
#'
#' @param annual_trends Data frame with \code{publication_year} and
#'   \code{publication_count} columns.
#' @param year_start Integer. Used to scale the year predictor.
#' @param verbose Logical.
#'
#' @return A tibble with columns: \code{model}, \code{family},
#'   \code{AIC}, \code{BIC}, \code{delta_AIC},
#'   \code{overdispersion_ratio}, \code{theta}, \code{converged},
#'   \code{note}. Rows sorted by AIC ascending.
#'
#' @examples
#' ann <- data.frame(
#'   publication_year  = 2000:2020,
#'   publication_count = as.integer(round(exp(3 + 0.05 * (0:20) +
#'     rnorm(21, 0, 0.2))))
#' )
#' cmp <- validate_nb_specification(ann, year_start=2000L, verbose=FALSE)
#' cmp[, c("model","AIC","delta_AIC","theta")]
#'
#' # Confirm M2_glm_nb is competitive (delta_AIC < 4):
#' cmp2 <- validate_nb_specification(ann, verbose=FALSE)
#' stopifnot(
#'   cmp2$delta_AIC[cmp2$model == "M2_glm_nb"] < 10
#' )
#'
#' # Overdispersion check: if M4_poisson deviance/df >> 1, NB is needed
#' cmp3 <- validate_nb_specification(ann, verbose=FALSE)
#' cmp3[, c("model","overdispersion_ratio")]
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr bind_rows filter mutate arrange
#' @importFrom tibble tibble
#' @importFrom purrr compact map
#' @importFrom MASS glm.nb
#' @export
validate_nb_specification <- function(annual_trends,
                                       year_start = 1975L,
                                       verbose    = TRUE) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(
    all(c("publication_year","publication_count") %in%
        names(annual_trends)))
  assertthat::assert_that(assertthat::is.flag(verbose))

  df <- dplyr::filter(annual_trends,
                      !is.na(.data$publication_count),
                      .data$publication_count >= 0L) |>
    dplyr::mutate(
      yr_s     = (.data$publication_year - year_start) / 10,
      log_cnt  = log(pmax(.data$publication_count, 0.5))
    )

  if (nrow(df) < 5L) {
    .log_step("[NB VALID] Fewer than 5 data points — skipping.", verbose)
    return(dplyr::tibble(model=character(), AIC=numeric()))
  }

  results <- list()

  # M1: log-linear OLS
  results[["M1_linear_ols"]] <- tryCatch({
    fit <- lm(log_cnt ~ yr_s, data = df)
    dplyr::tibble(model="M1_linear_ols", family="gaussian(log)",
      AIC=AIC(fit), BIC=BIC(fit),
      deviance=deviance(fit), df_resid=df.residual(fit),
      theta=NA_real_, converged=TRUE,
      note="Baseline: log-linear OLS")
  }, error=function(e) NULL)

  # M2: MASS::glm.nb (current pipeline)
  results[["M2_glm_nb"]] <- tryCatch({
    fit <- suppressWarnings(
      MASS::glm.nb(publication_count ~ yr_s, data = df))
    dplyr::tibble(model="M2_glm_nb", family="negative_binomial",
      AIC=AIC(fit), BIC=BIC(fit),
      deviance=fit$deviance, df_resid=fit$df.residual,
      theta=fit$theta, converged=fit$converged,
      note="Current pipeline: MASS::glm.nb")
  }, error=function(e) dplyr::tibble(
    model="M2_glm_nb", family="negative_binomial",
    AIC=NA_real_, BIC=NA_real_, deviance=NA_real_,
    df_resid=NA_integer_, theta=NA_real_, converged=FALSE,
    note=paste("FAILED:", e$message)))

  # M3: mgcv::gam NB with REML (flexible spline)
  if (requireNamespace("mgcv", quietly=TRUE) && nrow(df) >= 10L) {
    results[["M3_gam_nb_reml"]] <- tryCatch({
      k_val <- min(floor(nrow(df) / 3L), 5L)
      fit   <- mgcv::gam(
        publication_count ~ s(yr_s, k = k_val),
        data = df, family = mgcv::nb(), method = "REML")
      dplyr::tibble(model="M3_gam_nb_reml", family="mgcv_nb_spline",
        AIC=AIC(fit), BIC=BIC(fit),
        deviance=fit$deviance, df_resid=fit$df.residual,
        theta=as.numeric(fit$family$getTheta(TRUE)),
        converged=fit$converged,
        note=sprintf("mgcv::gam NB thin-plate spline k=%d (REML)", k_val))
    }, error=function(e) NULL)
  }

  # M4: Poisson GLM
  results[["M4_poisson"]] <- tryCatch({
    fit <- glm(publication_count ~ yr_s,
               data=df, family=poisson(link="log"))
    dplyr::tibble(model="M4_poisson", family="poisson",
      AIC=AIC(fit), BIC=BIC(fit),
      deviance=fit$deviance, df_resid=fit$df.residual,
      theta=Inf, converged=fit$converged,
      note="Poisson — check overdispersion_ratio >> 1")
  }, error=function(e) NULL)

  cmp <- dplyr::bind_rows(purrr::compact(results)) |>
    dplyr::arrange(.data$AIC) |>
    dplyr::mutate(
      delta_AIC            = round(.data$AIC - min(.data$AIC, na.rm=TRUE), 2L),
      best_by_aic          = .data$delta_AIC < 2,
      overdispersion_ratio = round(.data$deviance / .data$df_resid, 3L)
    )

  if (verbose) {
    .log_step("[NB VALID] Model comparison (lower AIC = better):", TRUE)
    for (i in seq_len(nrow(cmp))) {
      r <- cmp[i, ]
      message(sprintf("  %-20s AIC=%8.1f  ΔAIC=%5.1f  θ=%7.2f",
        r$model, r$AIC, r$delta_AIC, r$theta))
    }
    m2_delta <- cmp$delta_AIC[cmp$model == "M2_glm_nb"]
    if (!is.na(m2_delta) && m2_delta > 4) {
      warning(sprintf(paste(
        "[NB VALID] M2_glm_nb is %.1f AIC units worse than best model.",
        "Consider alternative specification."), m2_delta),
        call. = FALSE)
    }
  }
  cmp
}


# ============================================================
# P4: GEOFACET + STATEBIN GEOGRAPHIC VISUALISATIONS
# Source: mkiang/opioid_geographic
# ============================================================

#' State Publication Trends in Geographic Facet Layout
#'
#' @description
#' Uses \code{geofacet::facet_geo()} to arrange per-state small-multiple
#' trend panels in approximate US state map topology, following the
#' mkiang/opioid_geographic visualisation pattern. Falls back to a
#' standard \code{facet_wrap} when \pkg{geofacet} is not installed.
#'
#' @param state_trends Data frame with columns \code{state} (2-letter),
#'   \code{year}, and a numeric column named by \code{y_col}.
#' @param y_col     Character. Column to plot on y-axis.
#' @param year_start Integer. X-axis lower bound.
#' @param year_end   Integer. X-axis upper bound.
#' @param line_colour  Hex colour for the trend line.
#' @param shade_colour Hex colour for the area fill.
#' @param verbose   Logical.
#'
#' @return A \code{ggplot} object.
#'
#' @examples
#' df <- data.frame(
#'   state = rep(c("TX","CA","NY","FL"), each = 5),
#'   year  = rep(2015:2019, 4),
#'   pub_count = abs(rnorm(20, 10, 3))
#' )
#' p <- plot_geofacet_state_trends(df, y_col="pub_count",
#'   year_start=2015L, year_end=2019L, verbose=FALSE)
#' inherits(p, "gg")
#'
#' p2 <- plot_geofacet_state_trends(df, y_col="pub_count",
#'   line_colour="#d73027", verbose=FALSE)
#'
#' p3 <- plot_geofacet_state_trends(df, y_col="pub_count",
#'   shade_colour="#fee090", verbose=FALSE)
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr filter
#' @importFrom ggplot2 ggplot aes geom_area geom_line facet_wrap
#'   scale_x_continuous scale_y_continuous labs theme_bw theme
#'   element_text element_blank
#' @importFrom scales label_comma
#' @export
plot_geofacet_state_trends <- function(
    state_trends,
    y_col        = "publication_count",
    year_start   = 1990L,
    year_end     = as.integer(format(Sys.Date(), "%Y")),
    line_colour  = "#2c7bb6",
    shade_colour = "#a6cee3",
    verbose      = TRUE
) {
  assertthat::assert_that(is.data.frame(state_trends))
  assertthat::assert_that(assertthat::is.string(y_col))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("state","year", y_col) %in% names(state_trends)),
    msg = sprintf("state_trends must contain: state, year, %s", y_col))

  plot_data <- dplyr::filter(state_trends,
    .data$year >= year_start, .data$year <= year_end)

  base_plot <- ggplot2::ggplot(
    plot_data,
    ggplot2::aes(x = .data$year, y = .data[[y_col]])
  ) +
    ggplot2::geom_area(fill = shade_colour, alpha = 0.35, na.rm = TRUE) +
    ggplot2::geom_line(colour = line_colour, linewidth = 0.6, na.rm = TRUE) +
    ggplot2::scale_x_continuous(
      breaks = seq(year_start, year_end, by = 10L),
      labels = function(x) paste0("'", substr(x, 3L, 4L))
    ) +
    ggplot2::scale_y_continuous(
      labels = scales::label_comma(accuracy = 1)
    ) +
    ggplot2::labs(
      title    = sprintf("FPMRS Publications by State (%d\u2013%d)",
                         year_start, year_end),
      subtitle = "Small-multiple trend per state in geographic layout",
      x = NULL, y = "Publications"
    ) +
    ggplot2::theme_bw(base_size = 8) +
    ggplot2::theme(
      strip.background = ggplot2::element_blank(),
      strip.text       = ggplot2::element_text(size=6, face="bold"),
      axis.text        = ggplot2::element_text(size=5),
      panel.grid.minor = ggplot2::element_blank()
    )

  if (!requireNamespace("geofacet", quietly = TRUE)) {
    .log_step("[GEO] 'geofacet' not installed; using facet_wrap.", verbose)
    return(base_plot + ggplot2::facet_wrap(~state, ncol = 8L))
  }

  .log_step("[GEO] Building geofacet layout ...", verbose)
  base_plot + geofacet::facet_geo(~state,
    grid = "us_state_grid2", label = "code")
}


#' Equal-Area US State Tile Choropleth (Statebins)
#'
#' @description
#' Produces a tile-based US map where every state has equal visual
#' weight via \pkg{statebins}, following the opioid_geographic pattern
#' of pairing a statebin (main text) with a traditional choropleth
#' (supplement). Large states (TX, CA) no longer dominate the visual.
#'
#' @param state_data   Data frame with \code{state} (2-letter code)
#'   and a numeric column named by \code{value_col}.
#' @param value_col    Character. Column to colour tiles by.
#' @param palette_name RColorBrewer palette.
#' @param palette_dir  Integer \code{1} (light=low) or \code{-1}.
#' @param legend_title Character. Legend title.
#' @param title        Character. Plot title.
#' @param verbose      Logical.
#'
#' @return A \code{ggplot} object, or \code{NULL} if \pkg{statebins}
#'   is not installed.
#'
#' @examples
#' df <- data.frame(
#'   state    = c("TX","CA","NY","FL","OH"),
#'   pub_rate = c(3.2, 18.5, 9.1, 7.0, 5.5)
#' )
#' p <- plot_statebin_map(df, value_col="pub_rate", verbose=FALSE)
#' is.null(p) || inherits(p, "gg")
#'
#' p2 <- plot_statebin_map(df, value_col="pub_rate",
#'   palette_name="Reds", verbose=FALSE)
#'
#' p3 <- plot_statebin_map(df, value_col="pub_rate",
#'   legend_title="Rate", title="FPMRS Rate Map", verbose=FALSE)
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom ggplot2 labs theme element_text
#' @export
plot_statebin_map <- function(
    state_data,
    value_col    = "pub_rate",
    palette_name = "Blues",
    palette_dir  = 1L,
    legend_title = "Publications\nper 1M beneficiaries",
    title        = "FPMRS Publication Rate by State",
    verbose      = TRUE
) {
  assertthat::assert_that(is.data.frame(state_data))
  assertthat::assert_that(assertthat::is.string(value_col))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("state", value_col) %in% names(state_data)),
    msg = sprintf("state_data must contain 'state' and '%s'", value_col))

  if (!requireNamespace("statebins", quietly = TRUE)) {
    .log_step(paste(
      "[GEO] 'statebins' not installed.",
      "Install: install.packages('statebins')",
      "Returning NULL."
    ), verbose)
    return(NULL)
  }

  .log_step("[GEO] Building statebin map ...", verbose)
  statebins::statebins(
    state_data  = state_data,
    state_col   = "state",
    value_col   = value_col,
    palette     = palette_name,
    direction   = palette_dir,
    name        = legend_title,
    font_size   = 3,
    round       = FALSE
  ) +
    ggplot2::labs(
      title    = title,
      subtitle = "Equal-area tile map \u2014 each state = 1 tile"
    ) +
    statebins::theme_statebins(legend_position = "right") +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face="bold", size=12),
      plot.subtitle = ggplot2::element_text(size=9, colour="grey40"),
      legend.title  = ggplot2::element_text(size=8)
    )
}


# ============================================================
# P5: SIMULATION-BASED PIPELINE VALIDATION
# Source: ferenci-tamas/MortalityPrediction Monte Carlo framework
# ============================================================

#' Generate One Synthetic Annual Publication Count Series
#'
#' @description
#' Creates a tibble with known ground-truth trend parameters so all
#' pipeline trend-detection methods can be validated against it.
#' Follows the MortalityPrediction negative-binomial simulation
#' framework.
#'
#' @param n_years        Integer. Length of time series.
#' @param year_start     Integer. First calendar year.
#' @param true_slope     Numeric. True log-scale annual growth rate.
#' @param true_theta     Numeric. NB dispersion parameter.
#' @param true_intercept Numeric. Log expected count at year 1.
#' @param add_breakpoint Logical. Add structural break at 55\% of series.
#' @param seed           Integer or NULL.
#'
#' @return A tibble with \code{publication_year}, \code{publication_count},
#'   \code{log_true_mu}.
#'
#' @examples
#' sim <- simulate_annual_counts(n_years=20L, true_slope=0.04, seed=1L)
#' nrow(sim) == 20L
#'
#' sim2 <- simulate_annual_counts(n_years=30L, true_slope=0.08,
#'   true_theta=5, add_breakpoint=TRUE, seed=42L)
#' all(sim2$publication_count >= 0L)
#'
#' # Ground truth: slope recoverable from log(mu)
#' sim3 <- simulate_annual_counts(n_years=25L, true_slope=0.06, seed=7L)
#' cor(sim3$publication_year, sim3$log_true_mu) > 0.99
#'
#' @importFrom tibble tibble
#' @importFrom stats rnbinom
#' @export
simulate_annual_counts <- function(
    n_years        = 30L,
    year_start     = 1990L,
    true_slope     = 0.05,
    true_theta     = 15,
    true_intercept = 3.0,
    add_breakpoint = FALSE,
    seed           = NULL
) {
  if (!is.null(seed)) set.seed(seed)
  years  <- seq_len(n_years)
  log_mu <- true_intercept + true_slope * years
  if (isTRUE(add_breakpoint)) {
    bp <- floor(n_years * 0.55)
    log_mu[years > bp] <- log_mu[years > bp] +
                          true_slope * (years[years > bp] - bp)
  }
  counts <- stats::rnbinom(n_years, mu = exp(log_mu), size = true_theta)
  tibble::tibble(
    publication_year  = as.integer(year_start + years - 1L),
    publication_count = as.integer(pmax(counts, 0L)),
    log_true_mu       = log_mu
  )
}


#' Validate Trend-Detection Methods via Monte Carlo Simulation
#'
#' @description
#' Generates \code{n_sim} synthetic time series with known slope,
#' then runs the pipeline's NB GLM and Theil-Sen trend methods on
#' each, returning bias, RMSE, 95\% CI coverage, and Type-II error
#' rate. Follows the MortalityPrediction validation framework.
#'
#' @param n_sim        Integer. Number of synthetic datasets.
#' @param true_slope   Numeric. Ground-truth log-scale annual slope.
#' @param true_theta   Numeric. NB dispersion parameter.
#' @param n_years      Integer. Length of each time series.
#' @param seed         Integer. Master seed.
#' @param verbose      Logical.
#'
#' @return A named list with elements \code{summary} (tibble of
#'   bias/RMSE/coverage per method) and \code{all_results} (one row
#'   per simulation × method).
#'
#' @examples
#' res <- run_simulation_validation(n_sim=30L, true_slope=0.04,
#'   true_theta=15, n_years=25L, seed=1L, verbose=FALSE)
#' "summary" %in% names(res)
#'
#' # Check bias is small
#' res2 <- run_simulation_validation(n_sim=50L, true_slope=0.06,
#'   verbose=FALSE)
#' all(abs(res2$summary$bias) < 0.05)
#'
#' # Type-II error should be low when signal is strong
#' res3 <- run_simulation_validation(n_sim=50L, true_slope=0.1,
#'   true_theta=20, verbose=FALSE)
#' all(res3$summary$type_ii_error < 0.5)
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr bind_rows group_by summarise filter mutate
#' @importFrom purrr map
#' @importFrom tibble tibble
#' @importFrom MASS glm.nb
#' @export
run_simulation_validation <- function(
    n_sim      = 500L,
    true_slope = 0.05,
    true_theta = 15,
    n_years    = 30L,
    seed       = 42L,
    verbose    = TRUE
) {
  assertthat::assert_that(assertthat::is.count(n_sim))
  assertthat::assert_that(is.numeric(true_slope))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[SIM] Running %d simulations (slope=%.3f, theta=%.0f, n=%d) ...",
    n_sim, true_slope, true_theta, n_years), verbose)

  set.seed(seed)
  seeds_vec <- sample.int(1e6L, n_sim)

  run_one <- function(idx) {
    dat <- simulate_annual_counts(
      n_years=n_years, true_slope=true_slope,
      true_theta=true_theta, seed=seeds_vec[[idx]])

    # Method A: NB GLM
    nb_row <- tryCatch({
      fit <- suppressWarnings(
        MASS::glm.nb(publication_count ~ publication_year, data=dat))
      ci  <- suppressMessages(
        confint(fit, "publication_year", level=0.95))
      tibble::tibble(sim=idx, method="NB_GLM",
        est_slope=coef(fit)[["publication_year"]],
        ci_lo=ci[1L], ci_hi=ci[2L], converged=fit$converged)
    }, error=function(e) tibble::tibble(
      sim=idx, method="NB_GLM", est_slope=NA_real_,
      ci_lo=NA_real_, ci_hi=NA_real_, converged=FALSE))

    # Method B: Theil-Sen
    ts_row <- tryCatch({
      if (!requireNamespace("mblm", quietly=TRUE))
        stop("mblm not available")
      fit_ts <- mblm::mblm(
        publication_count ~ publication_year,
        dataframe=dat, repeated=FALSE)
      tibble::tibble(sim=idx, method="Theil_Sen",
        est_slope=coef(fit_ts)[["publication_year"]],
        ci_lo=NA_real_, ci_hi=NA_real_, converged=TRUE)
    }, error=function(e) tibble::tibble(
      sim=idx, method="Theil_Sen", est_slope=NA_real_,
      ci_lo=NA_real_, ci_hi=NA_real_, converged=FALSE))

    dplyr::bind_rows(nb_row, ts_row)
  }

  all_results <- dplyr::bind_rows(purrr::map(seq_len(n_sim), run_one))

  summary_tbl <- all_results |>
    dplyr::filter(!is.na(.data$est_slope)) |>
    dplyr::group_by(.data$method) |>
    dplyr::summarise(
      n_converged   = sum(.data$converged, na.rm=TRUE),
      bias          = round(mean(.data$est_slope - true_slope, na.rm=TRUE), 6L),
      rmse          = round(sqrt(mean((.data$est_slope - true_slope)^2, na.rm=TRUE)), 6L),
      coverage_95   = if (all(!is.na(.data$ci_lo)))
                        round(mean(.data$ci_lo <= true_slope &
                                   .data$ci_hi >= true_slope, na.rm=TRUE), 3L)
                      else NA_real_,
      type_ii_error = round(mean(.data$est_slope < 0, na.rm=TRUE), 3L),
      .groups = "drop"
    )

  if (verbose) {
    .log_step("[SIM] Results:", TRUE)
    purrr::walk(seq_len(nrow(summary_tbl)), function(i) {
      r <- summary_tbl[i, ]
      message(sprintf(
        "  %-12s | bias=% .5f | RMSE=%.5f | coverage=%s | type_II=%.3f",
        r$method, r$bias, r$rmse,
        if (is.na(r$coverage_95)) "N/A" else sprintf("%.2f", r$coverage_95),
        r$type_ii_error))
    })
  }

  list(summary=summary_tbl, all_results=all_results)
}


# ============================================================
# P8: UNDERSCORE-PREFIXED HELPER ALIASES
# Ensures consistent naming convention where helper functions
# are prefixed with a dot (internal) or underscore (documented)
# Source: hollina/duke-replication naming pattern
# ============================================================

# These are documented aliases that expose commonly called
# internals under the underscore-prefix convention for
# scripts that prefer this style (e.g. _geo_helpers.R).
# They delegate directly to the underlying dot-prefixed function.

#' @rdname .compute_annual_publication_trends
#' @export
compute_annual_trends <- function(bibliography_filtered, verbose = TRUE)
  .compute_annual_publication_trends(bibliography_filtered, verbose)

#' @rdname .compute_equity_metrics
#' @export
compute_equity_metrics <- function(bibliography, verbose = TRUE)
  .compute_equity_metrics(bibliography, verbose)

#' @rdname .compute_authorship_metrics
#' @export
compute_authorship_metrics <- function(bibliography, verbose = TRUE)
  .compute_authorship_metrics(bibliography, verbose)

#' @rdname .compute_disruption_index
#' @export
compute_disruption_index <- function(bibliography, verbose = TRUE)
  .compute_disruption_index(bibliography, verbose)


# ============================================================
# P9: RDS CACHING WITH force_rerun FLAG
# Source: opioid_geographic + MortalityPrediction file.exists() pattern
# ============================================================

#' Cache-Backed Computation: Load RDS if Fresh, Else Compute and Save
#'
#' @description
#' Combines a \code{file.exists()} freshness guard with a
#' \code{force_rerun} flag, following the mkiang/opioid_geographic
#' and MortalityPrediction caching patterns. DuckDB caches raw data;
#' this function caches \emph{analysis results} (model objects,
#' statistics tables, expensive intermediate data frames).
#'
#' @param label       Character. Short name used in the RDS filename.
#' @param compute_fn  Zero-argument function that produces the result.
#' @param cache_dir   Character. Directory to store \code{.rds} files.
#' @param force_rerun Logical. If \code{TRUE}, ignore cache and recompute.
#' @param ttl_days    Numeric. Maximum cache age in days (0 = always
#'   recompute regardless of force_rerun).
#' @param verbose     Logical.
#'
#' @return The computed or cached result (whatever \code{compute_fn}
#'   returns).
#'
#' @examples
#' tmpdir <- tempdir()
#'
#' # First call: computes and saves
#' result1 <- rds_cache("test_sum", function() sum(1:100),
#'   cache_dir=tmpdir, verbose=FALSE)
#' result1  # 5050
#'
#' # Second call: loads from cache
#' result2 <- rds_cache("test_sum", function() stop("should not run"),
#'   cache_dir=tmpdir, verbose=FALSE)
#' identical(result1, result2)  # TRUE
#'
#' # Force recompute
#' result3 <- rds_cache("test_sum", function() 42L,
#'   cache_dir=tmpdir, force_rerun=TRUE, verbose=FALSE)
#' result3  # 42L
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @export
rds_cache <- function(
    label,
    compute_fn,
    cache_dir   = file.path("data", "cache", "rds"),
    force_rerun = FALSE,
    ttl_days    = 7L,
    verbose     = TRUE
) {
  assertthat::assert_that(assertthat::is.string(label))
  assertthat::assert_that(is.function(compute_fn))
  assertthat::assert_that(assertthat::is.string(cache_dir))
  assertthat::assert_that(assertthat::is.flag(force_rerun))
  assertthat::assert_that(assertthat::is.flag(verbose))

  dir.create(cache_dir, recursive=TRUE, showWarnings=FALSE)
  path <- file.path(cache_dir, paste0(label, ".rds"))

  is_fresh <- function() {
    if (!file.exists(path)) return(FALSE)
    if (ttl_days == 0)      return(FALSE)
    age <- as.numeric(difftime(Sys.time(), file.mtime(path), units="days"))
    age <= ttl_days
  }

  if (!isTRUE(force_rerun) && is_fresh()) {
    .log_step(sprintf("[RDS] Loaded from cache: %s", path), verbose)
    return(readRDS(path))
  }

  .log_step(sprintf("[RDS] Computing: %s", label), verbose)
  result <- compute_fn()
  saveRDS(result, path)
  .log_step(sprintf("[RDS] Saved: %s", path), verbose)
  result
}


# ============================================================
# P10: DUAL SESSION INFO CAPTURE
# Source: mkiang/opioid_geographic + hollina/duke-replication
# ============================================================

#' Capture Dual Session Info to a Timestamped Text File
#'
#' @description
#' Writes both \code{sessioninfo::session_info()} (which records
#' package source: CRAN vs GitHub) and base \code{sessionInfo()} to
#' a timestamped text file in the manifests directory. Following the
#' opioid_geographic and duke-replication patterns, this is called at
#' the end of each pipeline stage so the exact computational
#' environment is archived with the outputs.
#'
#' @param output_dir Character. Directory to write the session file.
#' @param label      Character. Prefix for the filename.
#' @param verbose    Logical.
#'
#' @return Invisibly, the path to the written file.
#'
#' @examples
#' tmpdir <- tempdir()
#' path <- capture_dual_session_info(tmpdir, label="test", verbose=FALSE)
#' file.exists(path)
#'
#' path2 <- capture_dual_session_info(tmpdir, label="stage_c1",
#'   verbose=FALSE)
#' any(grepl("sessionInfo", readLines(path2)))
#'
#' path3 <- capture_dual_session_info(tmpdir, verbose=FALSE)
#' grepl("session_pipeline.*\\.txt", basename(path3))
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @export
capture_dual_session_info <- function(
    output_dir = file.path("output", "manifests"),
    label      = "pipeline",
    verbose    = TRUE
) {
  assertthat::assert_that(assertthat::is.string(output_dir))
  assertthat::assert_that(assertthat::is.string(label))
  assertthat::assert_that(assertthat::is.flag(verbose))

  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ts       <- format(Sys.time(), "%Y%m%d_%H%M%S")
  out_file <- file.path(output_dir,
                        sprintf("session_%s_%s.txt", label, ts))

  sink(out_file)
  cat(sprintf("=== Session Info: %s ===\n", label))
  cat(sprintf("Captured:          %s\n", Sys.time()))
  cat(sprintf("R version:         %s\n",
              paste(R.version$major, R.version$minor, sep=".")))
  cat(sprintf("Working directory: %s\n", getwd()))
  cat(sprintf("Platform:          %s\n", R.version$platform))
  cat(sprintf("R_CONFIG_ACTIVE:   %s\n\n",
              Sys.getenv("R_CONFIG_ACTIVE", unset="default")))

  if (requireNamespace("sessioninfo", quietly=TRUE)) {
    cat("--- sessioninfo::session_info() ---\n")
    tryCatch(print(sessioninfo::session_info()),
             error=function(e) cat("sessioninfo failed:", e$message, "\n"))
    cat("\n")
  }

  cat("--- base sessionInfo() ---\n")
  print(sessionInfo())
  sink()

  .log_step(sprintf("[SESSION] Info written: %s", out_file), verbose)
  invisible(out_file)
}


# ============================================================
# ============================================================
#
# SECTION 11: ALTMETRIC ANALYSIS PATTERNS
#
# Three patterns from bjarnebartlett/AltmetricAnalysis:
#   P-A: Per-stratum RDS model caching
#   P-B: Platform-discontinuity stratification (FPMRS→URPS break)
#   P-C: Author gender as a covariate layer in count models
#
# ============================================================
# ============================================================


# ============================================================
# P-A: PER-STRATUM RDS MODEL CACHING
# Source: AltmetricAnalysis — one RDS per journal per model type
# Adapted to: one RDS per subspecialty per model type
# ============================================================

#' Fit and Cache Models Across Subspecialty Strata
#'
#' @description
#' Adapts the bjarnebartlett/AltmetricAnalysis pattern of saving one
#' \code{.rds} per journal per model type to the FPMRS pipeline:
#' one \code{.rds} per subspecialty per model type. This makes it
#' trivial to re-run a single subspecialty without re-fitting the
#' entire eight-specialty comparison, and ensures model objects are
#' never lost between sessions.
#'
#' The workflow:
#' \enumerate{
#'   \item Split the long-format bibliography by \code{strat_col}
#'         via \code{dplyr::group_split()}.
#'   \item Apply \code{fit_fn} to each stratum via \code{purrr::map()}.
#'   \item Save each model object via \code{purrr::walk2()} to
#'         \code{<cache_dir>/<model_type>_<stratum>.rds}.
#'   \item Return a named list of model objects.
#' }
#'
#' @param bibliography   Data frame with all subspecialties combined.
#'   Must contain \code{strat_col}.
#' @param strat_col      Character. Column to split on (default:
#'   \code{"subspecialty"}).
#' @param fit_fn         Function. Accepts a single-stratum data frame
#'   and returns a model object (or any R object).
#' @param model_type     Character. Used in the RDS filename, e.g.
#'   \code{"nb_growth"}, \code{"logistic_gender"}.
#' @param cache_dir      Character. Directory for \code{.rds} files.
#' @param force_rerun    Logical. Ignore existing cache files.
#' @param verbose        Logical.
#'
#' @return A named list: one element per stratum, each holding the
#'   model object returned by \code{fit_fn}.
#'
#' @examples
#' # Minimal example with a trivial fit_fn
#' bib <- data.frame(
#'   subspecialty     = rep(c("FPMRS","REI","MFM"), each = 20),
#'   publication_year = rep(2000:2019, 3),
#'   publication_count = as.integer(abs(rnorm(60, 50, 10)))
#' )
#' tmpdir <- tempdir()
#' models <- fit_models_per_stratum(
#'   bib, strat_col = "subspecialty",
#'   fit_fn = function(df) lm(publication_count ~ publication_year, data = df),
#'   model_type = "linear_trend",
#'   cache_dir  = tmpdir,
#'   verbose    = FALSE
#' )
#' length(models) == 3L
#'
#' # Re-run loads from cache
#' models2 <- fit_models_per_stratum(
#'   bib, strat_col = "subspecialty",
#'   fit_fn = function(df) stop("should not be called"),
#'   model_type = "linear_trend",
#'   cache_dir  = tmpdir,
#'   verbose    = FALSE
#' )
#' all(names(models2) %in% c("FPMRS","REI","MFM"))
#'
#' # force_rerun bypasses cache
#' models3 <- fit_models_per_stratum(
#'   bib, strat_col = "subspecialty",
#'   fit_fn = function(df) list(n = nrow(df)),
#'   model_type = "linear_trend",
#'   cache_dir  = tmpdir, force_rerun = TRUE, verbose = FALSE
#' )
#' models3$FPMRS$n == 20L
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr group_split
#' @importFrom purrr map set_names walk2 map_chr compact
#' @export
fit_models_per_stratum <- function(
    bibliography,
    strat_col   = "subspecialty",
    fit_fn,
    model_type  = "model",
    cache_dir   = file.path("data", "cache", "rds", "models"),
    force_rerun = FALSE,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.string(strat_col))
  assertthat::assert_that(is.function(fit_fn))
  assertthat::assert_that(assertthat::is.string(model_type))
  assertthat::assert_that(assertthat::is.flag(force_rerun))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    strat_col %in% names(bibliography),
    msg = sprintf("Column '%s' not found in bibliography.", strat_col))

  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

  # ── Step 1: group_split by stratum (AltmetricAnalysis pattern) ─────────────
  # dplyr::group_split() returns an unnamed list; .keep = TRUE keeps the
  # grouping column in each slice so fit_fn receives the full data frame.
  bibliography[[strat_col]] <- as.character(bibliography[[strat_col]])
  strata_list <- bibliography |>
    dplyr::filter(!is.na(.data[[strat_col]])) |>
    dplyr::group_by(.data[[strat_col]]) |>
    dplyr::group_split(.keep = TRUE)

  strata_names <- purrr::map_chr(strata_list,
    ~as.character(.x[[strat_col]][[1L]]))
  strata_list  <- purrr::set_names(strata_list, strata_names)

  assertthat::assert_that(length(strata_list) >= 1L,
    msg = sprintf("No non-NA values in '%s'.", strat_col))

  .log_step(sprintf(
    "[STRATUM CACHE] Fitting '%s' across %d strata: %s",
    model_type, length(strata_list), paste(sort(strata_names), collapse = ", ")
  ), verbose)

  # ── Step 2: compute per-stratum input hash for cache invalidation ───────────
  # MD5 of the serialised stratum data frame; if the data changes the cache
  # is automatically invalidated even without force_rerun = TRUE.
  # Hash only named column vectors sorted by name — invariant to:
  #   1. Container class (tibble vs data.frame)
  #   2. Attribute serialization order (row.names/names order differs
  #      between group_split() tibble and []-filtered data.frame,
  #      making digest::digest(df) return different values even when
  #      identical(a, b) is TRUE)
  .stratum_hash <- function(df) {
    vecs <- lapply(df[sort(names(df))], function(col) {
      if (is.factor(col)) as.character(col) else col
    })
    digest::digest(vecs, algo = "md5")
  }
  .hash_path <- function(nm) {
    file.path(cache_dir, sprintf("%s_%s.hash", model_type,
      stringr::str_replace_all(nm, "[^A-Za-z0-9_-]", "_")))
  }
  .rds_path <- function(nm) {
    file.path(cache_dir, sprintf("%s_%s.rds",  model_type,
      stringr::str_replace_all(nm, "[^A-Za-z0-9_-]", "_")))
  }

  .cache_is_valid <- function(nm, df) {
    rds  <- .rds_path(nm)
    hash <- .hash_path(nm)
    if (!file.exists(rds) || !file.exists(hash)) return(FALSE)
    stored <- tryCatch(readLines(hash, warn = FALSE)[[1L]],
                       error = function(e) "")
    stored == .stratum_hash(df)
  }

  # ── Step 3: purrr::map() over strata ────────────────────────────────────────
  model_list <- purrr::map(strata_names, function(nm) {
    df <- strata_list[[nm]]

    if (!isTRUE(force_rerun) && .cache_is_valid(nm, df)) {
      .log_step(sprintf("[STRATUM CACHE] Hash match — loaded (%s)", nm), verbose)
      return(readRDS(.rds_path(nm)))
    }

    if (nrow(df) == 0L) {
      .log_step(sprintf("[STRATUM CACHE] Skipping '%s' — 0 rows.", nm), verbose)
      return(NULL)
    }

    .log_step(sprintf("[STRATUM CACHE] Fitting '%s' for '%s' (%d rows) ...",
                      model_type, nm, nrow(df)), verbose)
    tryCatch(fit_fn(df),
      error = function(e) {
        .log_step(sprintf("[STRATUM CACHE] FAILED (%s): %s", nm, e$message), TRUE)
        NULL
      }
    )
  }) |> purrr::set_names(strata_names)

  # ── Step 4: purrr::walk2() to save each model + its hash ────────────────────
  purrr::walk2(strata_names, model_list, function(nm, obj) {
    if (is.null(obj)) return(invisible(NULL))
    saveRDS(obj, .rds_path(nm))
    writeLines(.stratum_hash(strata_list[[nm]]), .hash_path(nm))
    .log_step(sprintf("[STRATUM CACHE] Saved + fingerprinted: %s",
                      .rds_path(nm)), verbose)
  })

  results <- purrr::compact(model_list)
  .log_step(sprintf(
    "[STRATUM CACHE] Complete: %d/%d strata fitted successfully.",
    length(results), length(strata_list)
  ), verbose)
  results
}


# ============================================================
# P-B: PLATFORM-DISCONTINUITY STRATIFICATION
# Source: AltmetricAnalysis — bioRxiv 2015 break → 4 model objects
# Adapted to: FPMRS→URPS 2024 nomenclature break → pre/post strata
# ============================================================

#' Annotate Bibliography with a Structural Break Flag Column
#'
#' @description
#' Adapts the AltmetricAnalysis bioRxiv-discontinuity pattern to FPMRS:
#' any year-based structural break (nomenclature rename, regulatory event,
#' mesh withdrawal, COVID, etc.) can be encoded as a stratification column.
#' The resulting flag column feeds directly into \code{fit_models_per_stratum()}
#' or \code{dplyr::group_split()} for split-sample estimation.
#'
#' Built-in breaks:
#' \describe{
#'   \item{fpmrs_urps_rename}{January 1 2024 — FPMRS subspecialty officially
#'     renamed to Urogynecology and Reconstructive Pelvic Surgery (URPS).}
#'   \item{mesh_fda_2019}{April 2019 — FDA ordered all transvaginal mesh
#'     products for POP pulled from market.}
#'   \item{mesh_fda_2016}{January 2016 — FDA reclassified transvaginal POP
#'     mesh as Class III (high-risk).}
#'   \item{pubmed_free_2000}{January 2000 — PubMed became freely accessible,
#'     dramatically expanding literature coverage.}
#'   \item{custom}{User-supplied \code{break_year} and \code{flag_labels}.}
#' }
#'
#' @param bibliography  Data frame with \code{publication_year}.
#' @param break_type    Character. One of \code{"fpmrs_urps_rename"},
#'   \code{"mesh_fda_2019"}, \code{"mesh_fda_2016"},
#'   \code{"pubmed_free_2000"}, or \code{"custom"}.
#' @param break_year    Integer. Required when \code{break_type = "custom"}.
#' @param flag_col      Character. Name of the new column to add.
#' @param flag_labels   Character(2). Labels for pre-break and post-break
#'   periods (default: \code{c("pre","post")}).
#' @param inclusive     Logical. If \code{TRUE}, the break year itself
#'   is included in the post-break stratum.
#' @param verbose       Logical.
#'
#' @return \code{bibliography} with an additional character column
#'   named \code{flag_col} with values from \code{flag_labels}.
#'
#' @examples
#' bib <- data.frame(publication_year = 2018:2026)
#'
#' # FPMRS → URPS rename (2024)
#' out <- annotate_structural_break(bib, break_type="fpmrs_urps_rename",
#'   verbose=FALSE)
#' table(out$era_flag)
#'
#' # Mesh FDA withdrawal (2019)
#' out2 <- annotate_structural_break(bib, break_type="mesh_fda_2019",
#'   verbose=FALSE)
#' table(out2$era_flag)
#'
#' # Custom break at 2022
#' out3 <- annotate_structural_break(bib, break_type="custom",
#'   break_year=2022L, flag_col="covid_era",
#'   flag_labels=c("pre_covid","post_covid"), verbose=FALSE)
#' all(out3$covid_era %in% c("pre_covid","post_covid"))
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr mutate if_else
#' @export
annotate_structural_break <- function(
    bibliography,
    break_type  = "fpmrs_urps_rename",
    break_year  = NULL,
    flag_col    = "era_flag",
    flag_labels = c("pre", "post"),
    inclusive   = TRUE,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.string(break_type))
  assertthat::assert_that(assertthat::is.string(flag_col))
  assertthat::assert_that(
    is.character(flag_labels) && length(flag_labels) == 2L)
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    "publication_year" %in% names(bibliography),
    msg = "`bibliography` must contain 'publication_year'.")

  break_table <- list(
    fpmrs_urps_rename = list(
      year = 2024L,
      note = "FPMRS renamed to URPS (Jan 1 2024)"
    ),
    mesh_fda_2019 = list(
      year = 2019L,
      note = "FDA halted transvaginal POP mesh sales (Apr 2019)"
    ),
    mesh_fda_2016 = list(
      year = 2016L,
      note = "FDA reclassified transvaginal POP mesh as Class III (Jan 2016)"
    ),
    pubmed_free_2000 = list(
      year = 2000L,
      note = "PubMed became freely accessible (Jan 2000)"
    )
  )

  if (break_type == "custom") {
    assertthat::assert_that(
      !is.null(break_year) && is.numeric(break_year),
      msg = "Provide break_year when break_type = 'custom'.")
    effective_year <- as.integer(break_year)
    note_str       <- sprintf("Custom break at %d", effective_year)
  } else {
    assertthat::assert_that(
      break_type %in% names(break_table),
      msg = sprintf("break_type must be one of: %s, or 'custom'.",
                    paste(names(break_table), collapse = ", ")))
    effective_year <- break_table[[break_type]]$year
    note_str       <- break_table[[break_type]]$note
  }

  threshold <- if (isTRUE(inclusive)) effective_year else effective_year + 1L

  result <- bibliography |>
    dplyr::mutate(
      !!flag_col := dplyr::if_else(
        !is.na(.data$publication_year) &
          .data$publication_year >= threshold,
        flag_labels[[2L]],
        flag_labels[[1L]]
      )
    )

  n_pre  <- sum(result[[flag_col]] == flag_labels[[1L]], na.rm = TRUE)
  n_post <- sum(result[[flag_col]] == flag_labels[[2L]], na.rm = TRUE)
  .log_step(sprintf(
    "[BREAK] %s | pre='%s' n=%d | post='%s' n=%d",
    note_str, flag_labels[[1L]], n_pre, flag_labels[[2L]], n_post
  ), verbose)

  result
}


#' Fit Split-Sample Models Across a Structural Break
#'
#' @description
#' Implements the four-model-object pattern from AltmetricAnalysis
#' (binary + linear, pre-bioRxiv + post-bioRxiv) adapted to FPMRS:
#' one model object per \strong{subspecialty × era stratum}.
#'
#' Workflow:
#' \enumerate{
#'   \item Calls \code{annotate_structural_break()} to add an era column.
#'   \item Calls \code{fit_models_per_stratum()} on the era-stratified
#'     data, producing one cached \code{.rds} per stratum per model type.
#'   \item Returns a named list: \code{pre_break} and \code{post_break},
#'     each holding the model object for its era.
#' }
#'
#' @param bibliography Data frame with \code{publication_year}.
#' @param fit_fn       Function. Accepts a stratum data frame → model.
#' @param break_type   Character. Passed to \code{annotate_structural_break()}.
#' @param break_year   Integer or NULL.
#' @param model_type   Character. Used in cache filenames.
#' @param cache_dir    Character.
#' @param force_rerun  Logical.
#' @param verbose      Logical.
#'
#' @return Named list with elements \code{pre_break}, \code{post_break},
#'   \code{break_year}, \code{break_type}, and \code{n_rows_each}.
#'
#' @examples
#' bib <- data.frame(
#'   publication_year  = rep(2018:2026, each = 10),
#'   publication_count = as.integer(abs(rnorm(90, 50, 10))),
#'   subspecialty = "FPMRS"
#' )
#' tmpdir <- tempdir()
#' res <- fit_split_sample_models(
#'   bib,
#'   fit_fn     = function(df) lm(publication_count ~ publication_year, df),
#'   break_type = "fpmrs_urps_rename",
#'   model_type = "nb_trend",
#'   cache_dir  = tmpdir,
#'   verbose    = FALSE
#' )
#' all(c("pre_break","post_break") %in% names(res))
#'
#' # Custom break
#' res2 <- fit_split_sample_models(bib,
#'   fit_fn = function(df) list(n=nrow(df)),
#'   break_type="custom", break_year=2022L,
#'   model_type="custom_split", cache_dir=tmpdir, verbose=FALSE)
#' res2$break_year == 2022L
#'
#' # Check that pre and post are distinct
#' res3 <- fit_split_sample_models(bib,
#'   fit_fn = function(df) nrow(df),
#'   break_type="fpmrs_urps_rename",
#'   model_type="nrow_check", cache_dir=tmpdir, verbose=FALSE)
#' res3$pre_break + res3$post_break == nrow(bib)
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @export
fit_split_sample_models <- function(
    bibliography,
    fit_fn,
    break_type  = "fpmrs_urps_rename",
    break_year  = NULL,
    model_type  = "model",
    cache_dir   = file.path("data", "cache", "rds", "models"),
    force_rerun = FALSE,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(is.function(fit_fn))
  assertthat::assert_that(assertthat::is.string(break_type))
  assertthat::assert_that(assertthat::is.flag(verbose))

  # 1. Annotate with era flag
  bib_flagged <- annotate_structural_break(
    bibliography, break_type = break_type,
    break_year = break_year, flag_col = ".era_split",
    flag_labels = c("pre_break", "post_break"), verbose = verbose)

  # 2. Fit per era using the stratum cache pattern
  era_models <- fit_models_per_stratum(
    bib_flagged, strat_col = ".era_split",
    fit_fn = fit_fn, model_type = model_type,
    cache_dir = cache_dir, force_rerun = force_rerun, verbose = verbose)

  # 3. Report row counts
  n_rows <- table(bib_flagged$.era_split)

  list(
    pre_break  = era_models[["pre_break"]],
    post_break = era_models[["post_break"]],
    break_type = break_type,
    break_year = if (break_type == "custom") as.integer(break_year)
                 else switch(break_type,
                   fpmrs_urps_rename = 2024L, mesh_fda_2019 = 2019L,
                   mesh_fda_2016 = 2016L, pubmed_free_2000 = 2000L, NA_integer_),
    n_rows_each = as.list(n_rows)
  )
}


# ============================================================
# P-C: AUTHOR GENDER COVARIATE LAYER
# Source: AltmetricAnalysis — gender of 200k article authors as
#   covariates in Altmetric score models.
# Adapted to: Gender features as covariates in NB growth models
#   and citation impact models for OB/GYN subspecialties.
# ============================================================

#' Build per-Paper Author Gender Feature Matrix
#'
#' @description
#' Extracts first-author gender, last-author gender, total author count,
#' proportion of female authors, and publication month from each paper's
#' \code{AU} and \code{AF} fields — exactly the covariate set used in
#' bjarnebartlett/AltmetricAnalysis. These features can be joined to any
#' citation or Altmetric model as covariates.
#'
#' @param bibliography Data frame with \code{UT} (paper ID), \code{AF}
#'   (author full names, semicolon-separated), \code{AU} (author
#'   abbreviated names), and optionally \code{PY} / \code{publication_year}
#'   and \code{DP} (date published).
#' @param min_authors Integer. Minimum author count to infer proportion
#'   (papers below this get \code{NA} for \code{pct_female_authors}).
#' @param verbose Logical.
#'
#' @return A tibble with one row per paper: \code{UT},
#'   \code{n_authors}, \code{first_author_gender},
#'   \code{last_author_gender}, \code{n_female_classified},
#'   \code{n_male_classified}, \code{n_gender_unclassified},
#'   \code{pct_female_authors}, \code{pub_month},
#'   \code{gender_data_quality} (\code{"complete"}, \code{"partial"},
#'   or \code{"unavailable"}).
#'
#' @examples
#' bib <- data.frame(
#'   UT = paste0("W", 1:4),
#'   AF = c("Smith Jane A;Jones Robert B;Lee Kelly C",
#'           "Patel Anita;Chen Wei;Brown Michael",
#'           "Garcia Maria L;Kim David H",
#'           NA_character_),
#'   AU = c("SMITH JA;JONES RB;LEE KC",
#'           "PATEL A;CHEN W;BROWN M",
#'           "GARCIA ML;KIM DH", NA_character_),
#'   publication_year = c(2018L, 2020L, 2022L, 2015L),
#'   PY = c("2018-06","2020-02","2022-11","2015-08"),
#'   stringsAsFactors = FALSE
#' )
#' feats <- build_gender_covariate_matrix(bib, verbose=FALSE)
#' nrow(feats) == 4L
#'
#' feats2 <- build_gender_covariate_matrix(bib, min_authors=2L, verbose=FALSE)
#' all(c("pct_female_authors","first_author_gender") %in% names(feats2))
#'
#' # Verify first-author detection
#' feats3 <- build_gender_covariate_matrix(bib, verbose=FALSE)
#' feats3$first_author_gender[1L]  # "Female" (Jane Smith)
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr mutate select coalesce
#' @importFrom purrr map_int map_chr
#' @importFrom stringr str_split str_extract str_trim
#' @importFrom tibble tibble
#' @export
build_gender_covariate_matrix <- function(
    bibliography,
    min_authors = 3L,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.count(min_authors))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[GENDER FEAT] Building gender covariate matrix for %d papers ...",
    nrow(bibliography)), verbose)

  # Helper: count non-empty tokens after splitting on ";"
  .count_tokens <- function(s) {
    if (is.na(s) || nchar(trimws(s)) == 0L) return(0L)
    toks <- stringr::str_split(s, ";")[[1L]]
    sum(nchar(trimws(toks)) > 0L)
  }

  # Helper: classify all AF authors, return vector of genders
  .classify_all_authors <- function(af_str) {
    if (is.na(af_str) || nchar(trimws(af_str)) == 0L)
      return(character(0))
    authors <- stringr::str_split(af_str, ";")[[1L]]
    authors <- stringr::str_trim(authors)
    authors <- authors[nchar(authors) > 0L]
    .infer_gender_vectorized(authors)
  }

  # Helper: extract publication month from PY column
  .extract_month <- function(py_str) {
    if (is.na(py_str)) return(NA_integer_)
    m <- stringr::str_extract(py_str, "(?<=-)[0-9]{2}(?=(-|$))")
    if (!is.na(m)) {
      val <- as.integer(m)
      if (val >= 1L && val <= 12L) return(val)
    }
    NA_integer_
  }

  # Derive UT column if absent
  if (!"UT" %in% names(bibliography)) {
    bibliography <- dplyr::mutate(bibliography,
                                   UT = paste0("ROW_", seq_len(nrow(bibliography))))
  }

  af_col <- if ("AF" %in% names(bibliography)) bibliography$AF
            else rep(NA_character_, nrow(bibliography))
  au_col <- if ("AU" %in% names(bibliography)) bibliography$AU
            else rep(NA_character_, nrow(bibliography))
  py_col <- if ("PY" %in% names(bibliography)) as.character(bibliography$PY)
            else if ("publication_year" %in% names(bibliography))
              as.character(bibliography$publication_year)
            else rep(NA_character_, nrow(bibliography))

  n <- nrow(bibliography)

  # Author counts from AU (abbreviated — more complete for count)
  n_authors <- purrr::map_int(au_col, .count_tokens)
  # Replace 0 with NA when AU is missing
  n_authors[n_authors == 0L] <- NA_integer_

  # Gender classification from AF (full names — more accurate)
  all_genders <- lapply(af_col, .classify_all_authors)

  # First author gender
  first_gender <- purrr::map_chr(all_genders, function(g) {
    if (length(g) == 0L || is.na(g[1L])) NA_character_ else g[1L]
  })

  # Last author gender (different from first for multi-author papers)
  last_gender <- purrr::map_chr(all_genders, function(g) {
    if (length(g) == 0L) return(NA_character_)
    last <- g[length(g)]
    if (is.na(last)) NA_character_ else last
  })

  # Female author proportions
  n_female_classified <- purrr::map_int(all_genders, function(g) {
    sum(g == "Female", na.rm = TRUE)
  })
  n_male_classified <- purrr::map_int(all_genders, function(g) {
    sum(g == "Male", na.rm = TRUE)
  })
  n_gender_unclassified <- purrr::map_int(all_genders, function(g) {
    sum(is.na(g))
  })
  n_classified <- n_female_classified + n_male_classified

  pct_female <- ifelse(
    n_authors >= min_authors & n_classified > 0L,
    round(n_female_classified / n_classified * 100, 1L),
    NA_real_
  )

  # Publication month
  pub_month <- purrr::map_int(py_col, .extract_month)

  # Data quality flag
  quality <- dplyr::case_when(
    is.na(af_col)                            ~ "unavailable",
    n_classified == 0L                       ~ "unavailable",
    n_classified < dplyr::coalesce(n_authors, 0L) ~ "partial",
    TRUE                                     ~ "complete"
  )

  result <- tibble::tibble(
    UT                    = bibliography$UT,
    n_authors             = n_authors,
    first_author_gender   = first_gender,
    last_author_gender    = last_gender,
    n_female_classified   = n_female_classified,
    n_male_classified     = n_male_classified,
    n_gender_unclassified = n_gender_unclassified,
    pct_female_authors    = pct_female,
    pub_month             = pub_month,
    gender_data_quality   = quality
  )

  n_complete <- sum(quality == "complete")
  n_partial  <- sum(quality == "partial")
  .log_step(sprintf(
    "[GENDER FEAT] %d papers: %d complete | %d partial | %d unavailable",
    n, n_complete, n_partial, n - n_complete - n_partial
  ), verbose)

  result
}


#' Attach Gender Covariates to Bibliography and Fit Citation Model
#'
#' @description
#' Complete implementation of the AltmetricAnalysis gender-covariate
#' workflow for FPMRS: builds the gender feature matrix, left-joins it to
#' the bibliography, then fits a negative binomial regression of citation
#' count on \code{publication_year}, \code{pub_month}, \code{n_authors},
#' \code{first_author_gender}, \code{last_author_gender}, and
#' \code{pct_female_authors}.
#'
#' @param bibliography  Data frame from the pipeline with \code{TC}
#'   (citation count) and author fields.
#' @param year_start    Integer. Earliest year to include in model.
#' @param min_authors   Integer. Passed to
#'   \code{build_gender_covariate_matrix()}.
#' @param verbose       Logical.
#'
#' @return A named list:
#' \describe{
#'   \item{model}{The fitted \code{glm.nb} or \code{glm} object.}
#'   \item{model_data}{The joined data frame passed to the model.}
#'   \item{gender_features}{The raw feature matrix.}
#'   \item{formula_used}{The model formula as a character string.}
#'   \item{n_complete}{Papers with complete gender + citation data.}
#' }
#'
#' @examples
#' bib <- data.frame(
#'   UT = paste0("W", 1:30),
#'   AF = rep(c("Smith Jane A;Jones Robert B", "Lee Kelly C"), 15),
#'   AU = rep(c("SMITH JA;JONES RB","LEE KC"), 15),
#'   TC = as.character(as.integer(abs(rnorm(30, 30, 15)))),
#'   publication_year = rep(2015:2019, each=6),
#'   PY = rep(paste0(2015:2019, "-06"), each=6),
#'   stringsAsFactors = FALSE
#' )
#' res <- fit_citation_gender_model(bib, year_start=2015L, verbose=FALSE)
#' inherits(res$model, "glm")
#'
#' # Check gender features attached
#' res2 <- fit_citation_gender_model(bib, verbose=FALSE)
#' "pct_female_authors" %in% names(res2$model_data)
#'
#' # n_complete <= nrow(bibliography)
#' res3 <- fit_citation_gender_model(bib, verbose=FALSE)
#' res3$n_complete <= nrow(bib)
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr left_join mutate filter
#' @importFrom MASS glm.nb
#' @export
fit_citation_gender_model <- function(
    bibliography,
    year_start  = 1975L,
    min_authors = 3L,
    verbose     = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.flag(verbose))

  # 1. Build gender feature matrix
  gender_features <- build_gender_covariate_matrix(
    bibliography, min_authors = min_authors, verbose = verbose)

  # 2. Prepare citation data
  if (!"UT" %in% names(bibliography)) {
    bibliography <- dplyr::mutate(bibliography,
      UT = paste0("ROW_", seq_len(nrow(bibliography))))
  }

  model_data <- bibliography |>
    dplyr::mutate(
      citation_count = suppressWarnings(as.integer(.data$TC)),
      yr_s           = (.data$publication_year - year_start) / 10
    ) |>
    dplyr::left_join(gender_features, by = "UT") |>
    dplyr::filter(
      !is.na(.data$citation_count),
      .data$citation_count >= 0L,
      !is.na(.data$publication_year),
      !is.na(.data$first_author_gender),
      !is.na(.data$n_authors)
    ) |>
    dplyr::mutate(
      first_author_female = as.integer(.data$first_author_gender == "Female"),
      last_author_female  = as.integer(.data$last_author_gender  == "Female"),
      log_n_authors       = log(pmax(.data$n_authors, 1L))
    )

  n_complete <- nrow(model_data)
  .log_step(sprintf(
    "[GENDER MODEL] Fitting citation ~ gender model with %d papers ...",
    n_complete), verbose)

  if (n_complete < 10L) {
    .log_step("[GENDER MODEL] Too few complete cases (< 10). Returning NULL.",
              verbose)
    return(list(model=NULL, model_data=model_data,
                gender_features=gender_features,
                formula_used="insufficient data",
                n_complete=n_complete))
  }

  # 3. Fit negative binomial GLM with gender covariates
  # Formula: AltmetricAnalysis used first/last gender + month + n_authors
  # FPMRS equivalent adds year trend and pct_female.
  # Guard: only include factor(pub_month) when >= 2 unique levels are
  # present — a single month level causes glm contrasts to crash.
  n_months <- dplyr::n_distinct(model_data$pub_month[!is.na(model_data$pub_month)])
  include_month    <- n_months >= 2L &&
                      sum(!is.na(model_data$pub_month)) > n_complete * 0.5
  include_pct_fem  <- sum(!is.na(model_data$pct_female_authors)) > n_complete * 0.3
  formula_str <- paste(
    "citation_count ~ yr_s + first_author_female + last_author_female +",
    "log_n_authors",
    if (include_month) "+ factor(pub_month)" else "",
    if (include_pct_fem) "+ pct_female_authors" else ""
  )
  formula_obj <- stats::as.formula(formula_str)

  model_obj <- tryCatch(
    suppressWarnings(MASS::glm.nb(formula_obj, data = model_data)),
    error = function(e) {
      .log_step(sprintf(
        "[GENDER MODEL] glm.nb failed (%s). Falling back to Poisson.",
        e$message), verbose)
      tryCatch(
        glm(formula_obj, data = model_data, family = poisson(link = "log")),
        error = function(e2) {
          .log_step(sprintf("[GENDER MODEL] Poisson also failed: %s",
                            e2$message), verbose)
          NULL
        }
      )
    }
  )

  if (!is.null(model_obj)) {
    .log_step(sprintf(
      "[GENDER MODEL] AIC=%.1f | family=%s | n=%d",
      AIC(model_obj),
      if (inherits(model_obj,"negbin")) "NB" else "Poisson",
      n_complete), verbose)
  }

  list(
    model           = model_obj,
    model_data      = model_data,
    gender_features = gender_features,
    formula_used    = formula_str,
    n_complete      = n_complete
  )
}


# ============================================================
# ============================================================
#
# SECTION 12: MESH-DRIVEN QUERY CONSTRUCTION AND
#             TERMINOLOGY COVERAGE SENSITIVITY ANALYSIS
#
# Pattern source: palolili23/pubmed_webscrap
#   MeSH-term–driven query construction via NCBI Entrez API.
#   rentrez::entrez_search() with MeSH field tags, parallel
#   free-text arm, and dplyr::anti_join() gap analysis.
#
# Key insight: NLM actively remapped "Female Pelvic Medicine
#   and Reconstructive Surgery" headings when the subspecialty
#   was renamed to URPS in 2024. MeSH is the authoritative
#   way to capture this transition; free-text keyword matching
#   misses papers that were indexed under the MeSH heading
#   but do not contain the acronym in title/abstract.
#
# ============================================================
# ============================================================


# ============================================================
# MeSH TERM CATALOGUE FOR FPMRS / URPS
# Single source of truth for all MeSH search arms.
# ============================================================

#' MeSH Term Catalogue for FPMRS/URPS Literature
#'
#' @description
#' Named list mapping conceptual categories to their authoritative
#' NLM MeSH heading strings (suitable for \code{[MeSH Terms]} field
#' tagging in PubMed queries). Tracks introduction years so the
#' pipeline can correctly flag pre-introduction gaps.
#'
#' @format A named list; each element is a list with:
#' \describe{
#'   \item{heading}{Character. Exact NLM MeSH heading (lowercase).}
#'   \item{introduced}{Integer. Year MeSH heading was first used.}
#'   \item{ui}{Character. MeSH Unique Identifier (D-number).}
#'   \item{note}{Character. Brief scope note.}
#' }
#'
#' @examples
#' # See all headings
#' names(fpmrs_mesh_catalogue)
#'
#' # Check introduction year for pelvic floor heading
#' fpmrs_mesh_catalogue$pelvic_floor$introduced
#'
#' # Build a query from selected entries
#' build_mesh_query(c("pelvic_floor","pelvic_organ_prolapse"))
#'
#' @export
fpmrs_mesh_catalogue <- list(

  # ── Core pelvic floor headings ─────────────────────────────
  pelvic_floor = list(
    heading    = "pelvic floor",
    introduced = 1998L,
    ui         = "D053761",
    note       = "Introduced 1998; primary umbrella term"
  ),
  pelvic_organ_prolapse = list(
    heading    = "pelvic organ prolapse",
    introduced = 1999L,
    ui         = "D056887",
    note       = "Introduced 1999; supersedes uterine prolapse for general POP"
  ),
  pelvic_floor_disorders = list(
    heading    = "pelvic floor disorders",
    introduced = 2002L,
    ui         = "D054380",
    note       = "Introduced 2002; broadest FPMRS umbrella"
  ),
  female_pelvic_medicine = list(
    heading    = "female pelvic medicine and reconstructive surgery",
    introduced = 2019L,
    ui         = "D000090962",
    note       = "Introduced 2019; specialty-specific heading. NLM is updating to URPS nomenclature"
  ),

  # ── Incontinence headings ───────────────────────────────────
  urinary_incontinence = list(
    heading    = "urinary incontinence",
    introduced = 1966L,
    ui         = "D014549",
    note       = "Parent term; use narrower terms for stress/urge"
  ),
  stress_urinary_incontinence = list(
    heading    = "urinary incontinence, stress",
    introduced = 1978L,
    ui         = "D014550",
    note       = "Introduced 1978; key FPMRS term throughout entire history"
  ),
  urge_urinary_incontinence = list(
    heading    = "urinary incontinence, urge",
    introduced = 1980L,
    ui         = "D053202",
    note       = "Introduced 1980"
  ),
  fecal_incontinence = list(
    heading    = "fecal incontinence",
    introduced = 1966L,
    ui         = "D005242",
    note       = "OAB / defecatory dysfunction overlap"
  ),
  overactive_bladder = list(
    heading    = "urinary bladder, overactive",
    introduced = 2004L,
    ui         = "D053201",
    note       = "Introduced 2004; OAB / urge-incontinence cluster"
  ),

  # ── Prolapse sub-types (pre-1999 primary terms) ────────────
  uterine_prolapse = list(
    heading    = "uterine prolapse",
    introduced = 1966L,
    ui         = "D014596",
    note       = "Pre-1999 primary prolapse term; still indexed alongside POP"
  ),
  cystocele = list(
    heading    = "cystocele",
    introduced = 1966L,
    ui         = "D052858",
    note       = "Anterior compartment defect; active since 1966"
  ),
  rectocele = list(
    heading    = "rectocele",
    introduced = 1966L,
    ui         = "D012002",
    note       = "Posterior compartment defect"
  ),

  # ── Surgical procedures ─────────────────────────────────────
  suburethral_slings = list(
    heading    = "suburethral slings",
    introduced = 2002L,
    ui         = "D060468",
    note       = "Sling procedures for SUI; introduced 2002"
  ),
  colposuspension = list(
    heading    = "colposuspension",
    introduced = 1993L,
    ui         = "D000074322",
    note       = "Burch colposuspension and variants"
  ),
  sacrocolpopexy = list(
    heading    = "colpopexy",
    introduced = 2000L,
    ui         = "D059349",
    note       = "Apical prolapse repair; covers sacrocolpopexy"
  ),
  surgical_mesh = list(
    heading    = "surgical mesh",
    introduced = 1998L,
    ui         = "D013526",
    note       = "Transvaginal mesh; peak use 2005-2012"
  ),

  # ── Urodynamics / diagnostics ───────────────────────────────
  urodynamics = list(
    heading    = "urodynamics",
    introduced = 1968L,
    ui         = "D014570",
    note       = "Urodynamic testing umbrella"
  ),
  pelvic_pain = list(
    heading    = "pelvic pain",
    introduced = 1991L,
    ui         = "D017699",
    note       = "Chronic pelvic pain; introduced 1991"
  )
)


# ============================================================
# BUILD MeSH QUERY FROM CATALOGUE KEYS
# ============================================================

#' Build a PubMed MeSH Query String from Catalogue Keys
#'
#' @description
#' Constructs a valid PubMed query string using \code{[MeSH Terms]}
#' field tags from selected entries in \code{fpmrs_mesh_catalogue}.
#' The resulting string can be passed directly to
#' \code{rentrez::entrez_search()} or used in the \code{pubmed_query}
#' argument of \code{run_fpmrs_bibliometric_pipeline()}.
#'
#' @param keys      Character vector of keys from \code{fpmrs_mesh_catalogue}.
#'   Use \code{names(fpmrs_mesh_catalogue)} to see all available keys.
#'   If \code{NULL} (default), all catalogue entries are included.
#' @param year_start Integer. Warn when any selected heading was introduced
#'   after \code{year_start} (pre-introduction literature will be missed).
#' @param add_freetext_fallback Logical. Append free-text \code{[Title/Abstract]}
#'   arms for the FPMRS/URPS acronyms and "urogynecology" to capture
#'   post-2024 URPS literature not yet remapped by NLM.
#' @param operator  Character. \code{"OR"} (default) or \code{"AND"}.
#' @param verbose   Logical.
#'
#' @return Character. A single PubMed query string ready for
#'   \code{rentrez::entrez_search(db="pubmed", term=<result>)}.
#'
#' @examples
#' # All FPMRS MeSH terms
#' q1 <- build_mesh_query(verbose=FALSE)
#' nchar(q1) > 100L
#'
#' # Core pelvic floor terms only
#' q2 <- build_mesh_query(
#'   keys = c("pelvic_floor","pelvic_organ_prolapse",
#'            "stress_urinary_incontinence"),
#'   verbose = FALSE
#' )
#' grepl("pelvic floor", q2)
#'
#' # With free-text fallback for post-2024 URPS literature
#' q3 <- build_mesh_query(
#'   keys = c("pelvic_floor","pelvic_organ_prolapse"),
#'   add_freetext_fallback = TRUE,
#'   year_start = 1975L,
#'   verbose = FALSE
#' )
#' grepl("URPS", q3)
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom purrr map_chr keep
#' @export
build_mesh_query <- function(
    keys                  = NULL,
    year_start            = 1975L,
    add_freetext_fallback = TRUE,
    operator              = "OR",
    verbose               = TRUE
) {
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(operator %in% c("OR","AND"))

  # Default: use all catalogue keys
  selected_keys <- if (is.null(keys)) {
    names(fpmrs_mesh_catalogue)
  } else {
    unknown <- setdiff(keys, names(fpmrs_mesh_catalogue))
    if (length(unknown) > 0L) {
      warning(sprintf(
        "[MESH QUERY] Unknown keys ignored: %s\nAvailable: %s",
        paste(unknown, collapse=", "),
        paste(names(fpmrs_mesh_catalogue), collapse=", ")),
        call.=FALSE)
    }
    intersect(keys, names(fpmrs_mesh_catalogue))
  }

  assertthat::assert_that(length(selected_keys) >= 1L,
    msg = "No valid keys selected.")

  # Gap warnings: headings introduced after year_start
  gaps <- purrr::keep(selected_keys, function(k) {
    fpmrs_mesh_catalogue[[k]]$introduced > year_start
  })
  if (length(gaps) > 0L && verbose) {
    gap_details <- purrr::map_chr(gaps, function(k) {
      e <- fpmrs_mesh_catalogue[[k]]
      sprintf("  %s (MeSH %s, introduced %d)",
              e$heading, e$ui, e$introduced)
    })
    message(sprintf(
      "[MESH QUERY] %d heading(s) post-date year_start=%d; pre-introduction literature will be missed:\n%s",
      length(gaps), year_start, paste(gap_details, collapse="\n")))
  }

  # Build MeSH arms
  mesh_arms <- purrr::map_chr(selected_keys, function(k) {
    sprintf('"%s"[MeSH Terms]', fpmrs_mesh_catalogue[[k]]$heading)
  })

  # Optional free-text fallback for URPS / post-rename literature
  ft_arms <- if (isTRUE(add_freetext_fallback)) {
    c(
      '"URPS"[Title/Abstract]',
      '"urogynecology and reconstructive pelvic surgery"[Title/Abstract]',
      '"FPMRS"[Title/Abstract]',
      '"urogynecology"[Title/Abstract]'
    )
  } else character(0L)

  all_arms <- c(mesh_arms, ft_arms)
  sep      <- sprintf(" %s\n  ", operator)
  query    <- sprintf("(\n  %s\n)", paste(all_arms, collapse=sep))

  .log_step(sprintf(
    "[MESH QUERY] Built %d-arm query (%d MeSH + %d free-text)",
    length(all_arms), length(mesh_arms), length(ft_arms)), verbose)

  query
}


# ============================================================
# RENTREZ-BASED MESH SEARCH
# ============================================================

#' Search PubMed with a MeSH Query via rentrez
#'
#' @description
#' Executes a PubMed search using \code{rentrez::entrez_search()} and
#' returns a tibble of PMIDs with the query arm that retrieved each.
#' Handles NCBI rate limits (3 req/s without key; 10 req/s with key)
#' via retry-with-backoff.
#'
#' @param query      Character. PubMed query string.
#' @param year_start Integer. Restrict to \code{year_start:year_end}.
#' @param year_end   Integer.
#' @param api_key    Character or NULL. NCBI API key.
#' @param max_records Integer. Maximum PMIDs to retrieve.
#' @param verbose    Logical.
#'
#' @return A tibble with columns \code{pmid} (character) and
#'   \code{query_arm} (\code{"mesh_entrez"}).
#'
#' @examples
#' \dontrun{
#' # Requires rentrez and NCBI API access
#' q <- build_mesh_query(
#'   keys       = c("stress_urinary_incontinence","pelvic_floor"),
#'   year_start = 2020L,
#'   verbose    = FALSE
#' )
#' pmids <- search_pubmed_mesh(q, year_start=2020L, year_end=2024L,
#'   api_key=Sys.getenv("PUBMED_API_KEY"), verbose=TRUE)
#' nrow(pmids)
#' }
#'
#' @importFrom assertthat assert_that is.string is.count is.flag
#' @importFrom tibble tibble
#' @export
search_pubmed_mesh <- function(
    query,
    year_start  = 1975L,
    year_end    = as.integer(format(Sys.Date(), "%Y")),
    api_key     = NULL,
    max_records = 100000L,
    verbose     = TRUE
) {
  assertthat::assert_that(assertthat::is.string(query))
  assertthat::assert_that(assertthat::is.flag(verbose))

  if (!requireNamespace("rentrez", quietly=TRUE)) {
    stop(paste(
      "Package 'rentrez' required for MeSH search.",
      "Install with: install.packages('rentrez')"))
  }

  # Add date filter to query
  date_filter <- sprintf(' AND ("%d/01/01"[PDAT]:"%d/12/31"[PDAT])',
                          year_start, year_end)
  full_query  <- paste0(query, date_filter)

  .log_step(sprintf(
    "[MESH SEARCH] Searching PubMed (%d-%d) ...", year_start, year_end),
    verbose)

  # Retry-with-backoff for NCBI rate limits
  search_with_retry <- function(q, retries=3L, wait_sec=2L) {
    for (attempt in seq_len(retries)) {
      result <- tryCatch({
        if (!is.null(api_key) && nchar(api_key) > 0L) {
          rentrez::entrez_search(db="pubmed", term=q,
            retmax=max_records, api_key=api_key, use_history=FALSE)
        } else {
          rentrez::entrez_search(db="pubmed", term=q,
            retmax=max_records, use_history=FALSE)
        }
      }, error=function(e) {
        if (attempt < retries) {
          .log_step(sprintf(
            "[MESH SEARCH] Attempt %d failed (%s). Retrying in %ds ...",
            attempt, e$message, wait_sec * attempt), verbose)
          Sys.sleep(wait_sec * attempt)
          return(NULL)
        }
        stop(sprintf("[MESH SEARCH] All %d attempts failed: %s",
                     retries, e$message))
      })
      if (!is.null(result)) return(result)
    }
  }

  search_result <- search_with_retry(full_query)
  pmids         <- as.character(search_result$ids)

  .log_step(sprintf(
    "[MESH SEARCH] Retrieved %d PMIDs (of %s total matches)",
    length(pmids),
    format(search_result$count, big.mark=",")), verbose)

  tibble::tibble(
    pmid      = pmids,
    query_arm = "mesh_entrez"
  )
}


# ============================================================
# MESH VS FREE-TEXT SENSITIVITY ANALYSIS
# ============================================================

#' Sensitivity Analysis: MeSH Coverage vs Free-Text Keyword Coverage
#'
#' @description
#' Implements the palolili23/pubmed_webscrap approach for FPMRS:
#' runs two parallel searches (MeSH-only and free-text-only), then
#' uses \code{dplyr::anti_join()} to identify what each arm captures
#' that the other misses. Quantifies terminology coverage gaps and
#' is particularly important for the FPMRS→URPS nomenclature
#' transition because NLM actively remapped existing headings.
#'
#' @section Why this matters for FPMRS→URPS:
#' Papers indexed between 2019 and 2023 under
#' "female pelvic medicine and reconstructive surgery" [MeSH] may
#' not contain the acronym FPMRS in title or abstract. After January
#' 2024, newly indexed papers may appear under the URPS heading before
#' the MeSH vocabulary update is complete. The anti-join gap analysis
#' quantifies exactly how many papers fall into each gap.
#'
#' @param year_start   Integer. Start of search window.
#' @param year_end     Integer. End of search window.
#' @param mesh_keys    Character vector. Keys from \code{fpmrs_mesh_catalogue}.
#'   If \code{NULL}, uses core FPMRS headings.
#' @param freetext_query Character or NULL. Custom free-text query.
#'   If \code{NULL}, uses \code{get_subspecialty_pubmed_query("fpmrs")}
#'   stripped of \code{[MeSH Terms]} arms.
#' @param api_key      Character or NULL. NCBI API key.
#' @param pmid_col     Character. Column name for PMIDs in input data
#'   (used when existing bibliography is provided instead of live search).
#' @param mesh_pmids   Tibble or NULL. Pre-fetched MeSH PMID table
#'   (avoids re-querying NCBI).
#' @param freetext_pmids Tibble or NULL. Pre-fetched free-text PMID table.
#' @param verbose      Logical.
#'
#' @return A named list:
#' \describe{
#'   \item{mesh_only}{Tibble of PMIDs captured by MeSH but not free-text.}
#'   \item{freetext_only}{Tibble of PMIDs captured by free-text but not MeSH.}
#'   \item{both}{Tibble of PMIDs captured by both arms.}
#'   \item{summary}{One-row tibble: n_mesh, n_freetext, n_both,
#'     n_mesh_only, n_freetext_only, n_union, pct_mesh_recall,
#'     pct_freetext_recall.}
#'   \item{venn_data}{List suitable for plotting a Venn diagram.}
#'   \item{recommendation}{Character. Automated recommendation based on gaps.}
#' }
#'
#' @examples
#' \dontrun{
#' # Live search (requires rentrez + API key)
#' sens <- analyse_mesh_vs_freetext_coverage(
#'   year_start = 2019L,
#'   year_end   = 2024L,
#'   api_key    = Sys.getenv("PUBMED_API_KEY"),
#'   verbose    = TRUE
#' )
#' sens$summary
#' sens$recommendation
#' }
#'
#' # Offline use: supply pre-fetched PMID tables
#' mesh_tbl <- tibble::tibble(pmid=c("36000001","36000002","36000003"),
#'   query_arm="mesh_entrez")
#' ft_tbl   <- tibble::tibble(pmid=c("36000002","36000003","36000004"),
#'   query_arm="freetext")
#' sens <- analyse_mesh_vs_freetext_coverage(
#'   mesh_pmids=mesh_tbl, freetext_pmids=ft_tbl, verbose=FALSE)
#' sens$summary$n_mesh_only    # 1 (36000001)
#' sens$summary$n_freetext_only # 1 (36000004)
#' sens$summary$n_both          # 2
#'
#' # Check recommendation is generated
#' nchar(sens$recommendation) > 0L
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr anti_join inner_join bind_rows n_distinct tibble
#' @importFrom tibble tibble
#' @export
analyse_mesh_vs_freetext_coverage <- function(
    year_start      = 1975L,
    year_end        = as.integer(format(Sys.Date(), "%Y")),
    mesh_keys       = NULL,
    freetext_query  = NULL,
    api_key         = NULL,
    pmid_col        = "pmid",
    mesh_pmids      = NULL,
    freetext_pmids  = NULL,
    verbose         = TRUE
) {
  assertthat::assert_that(assertthat::is.flag(verbose))

  # ── Arm 1: MeSH search ──────────────────────────────────────
  if (is.null(mesh_pmids)) {
    .log_step("[SENS] Running MeSH-only search arm ...", verbose)
    core_mesh_keys <- mesh_keys %||%
      c("pelvic_floor", "pelvic_organ_prolapse",
        "stress_urinary_incontinence", "urge_urinary_incontinence",
        "pelvic_floor_disorders", "female_pelvic_medicine",
        "uterine_prolapse", "cystocele", "rectocele",
        "suburethral_slings", "fecal_incontinence")
    mesh_query_str <- build_mesh_query(
      keys                  = core_mesh_keys,
      year_start            = year_start,
      add_freetext_fallback = FALSE,  # pure MeSH arm
      verbose               = FALSE)
    mesh_pmids <- search_pubmed_mesh(
      mesh_query_str, year_start=year_start, year_end=year_end,
      api_key=api_key, verbose=verbose)
  }

  # ── Arm 2: Free-text search ──────────────────────────────────
  if (is.null(freetext_pmids)) {
    .log_step("[SENS] Running free-text-only search arm ...", verbose)
    ft_query <- freetext_query %||% .build_freetext_only_query()
    freetext_pmids <- search_pubmed_mesh(
      ft_query, year_start=year_start, year_end=year_end,
      api_key=api_key, verbose=verbose)
    freetext_pmids$query_arm <- "freetext"
  }

  # Normalise column names
  assertthat::assert_that(pmid_col %in% names(mesh_pmids),
    msg=sprintf("Column '%s' not found in mesh_pmids", pmid_col))
  assertthat::assert_that(pmid_col %in% names(freetext_pmids),
    msg=sprintf("Column '%s' not found in freetext_pmids", pmid_col))

  mesh_ids <- tibble::tibble(pmid=as.character(mesh_pmids[[pmid_col]]))
  ft_ids   <- tibble::tibble(pmid=as.character(freetext_pmids[[pmid_col]]))

  # ── dplyr::anti_join gap analysis ───────────────────────────
  mesh_only     <- dplyr::anti_join(mesh_ids, ft_ids,   by="pmid")
  freetext_only <- dplyr::anti_join(ft_ids,   mesh_ids, by="pmid")
  both          <- dplyr::inner_join(mesh_ids, ft_ids,  by="pmid")

  n_mesh        <- nrow(mesh_ids)
  n_freetext    <- nrow(ft_ids)
  n_both        <- nrow(both)
  n_mesh_only   <- nrow(mesh_only)
  n_freetext_only <- nrow(freetext_only)
  n_union       <- n_mesh_only + n_freetext_only + n_both

  pct_mesh_recall     <- if (n_union > 0) round(n_mesh / n_union * 100, 1L) else NA_real_
  pct_freetext_recall <- if (n_union > 0) round(n_freetext / n_union * 100, 1L) else NA_real_

  summary_tbl <- tibble::tibble(
    n_mesh              = n_mesh,
    n_freetext          = n_freetext,
    n_both              = n_both,
    n_mesh_only         = n_mesh_only,
    n_freetext_only     = n_freetext_only,
    n_union             = n_union,
    pct_mesh_recall     = pct_mesh_recall,
    pct_freetext_recall = pct_freetext_recall
  )

  if (verbose) {
    .log_step(sprintf(paste(
      "[SENS] Union: %d | MeSH: %d (%.0f%%) | Free-text: %d (%.0f%%)",
      "| MeSH-only gap: %d | Free-text-only gap: %d"),
      n_union,
      n_mesh, pct_mesh_recall,
      n_freetext, pct_freetext_recall,
      n_mesh_only, n_freetext_only), TRUE)
  }

  # ── Automated recommendation ─────────────────────────────────
  recommendation <- .generate_sensitivity_recommendation(
    n_mesh_only, n_freetext_only, n_both, n_union)
  if (verbose) message(sprintf("[SENS] Recommendation: %s",
                               recommendation))

  list(
    mesh_only      = mesh_only,
    freetext_only  = freetext_only,
    both           = both,
    summary        = summary_tbl,
    venn_data      = list(
      mesh     = n_mesh,
      freetext = n_freetext,
      overlap  = n_both
    ),
    recommendation = recommendation
  )
}


#' @noRd
.build_freetext_only_query <- function() {
  paste0(
    "(",
    '"urogynecology"[Title/Abstract] OR ',
    '"urogynecological"[Title/Abstract] OR ',
    '"FPMRS"[Title/Abstract] OR ',
    '"URPS"[Title/Abstract] OR ',
    '"urogynecology and reconstructive pelvic surgery"[Title/Abstract] OR ',
    '"pelvic floor dysfunction"[Title/Abstract] OR ',
    '"pelvic organ prolapse"[Title/Abstract] OR ',
    '"stress urinary incontinence"[Title/Abstract] OR ',
    '"urge urinary incontinence"[Title/Abstract] OR ',
    '"urinary incontinence"[Title/Abstract] OR ',
    '"pelvic floor"[Title/Abstract] OR ',
    '"reconstructive pelvic surgery"[Title/Abstract] OR ',
    '"vaginal prolapse"[Title/Abstract] OR ',
    '"uterine prolapse"[Title/Abstract] OR ',
    '"cystocele"[Title/Abstract] OR ',
    '"rectocele"[Title/Abstract] OR ',
    '"colporrhaphy"[Title/Abstract] OR ',
    '"sacrocolpopexy"[Title/Abstract] OR ',
    '"suburethral sling"[Title/Abstract] OR ',
    '"colposuspension"[Title/Abstract]',
    ")"
  )
}


#' @noRd
.generate_sensitivity_recommendation <- function(n_mesh_only, n_freetext_only,
                                                   n_both, n_union) {
  if (n_union == 0L) return("No records retrieved by either arm.")

  pct_mesh_only <- n_mesh_only / n_union * 100
  pct_ft_only   <- n_freetext_only / n_union * 100

  if (pct_mesh_only < 5 && pct_ft_only < 5) {
    return(paste(
      "Both arms are highly concordant (< 5% unique to each).",
      "Current combined query is adequate."))
  }
  if (pct_mesh_only >= 10) {
    return(paste(sprintf(
      "MeSH arm captures %.0f%% unique records not found by free-text.",
      pct_mesh_only),
      "Recommendation: INCLUDE MeSH Terms arms in the production query.",
      "These likely represent papers indexed under the heading without",
      "containing FPMRS/URPS acronyms in title/abstract."))
  }
  if (pct_ft_only >= 10) {
    return(paste(sprintf(
      "Free-text arm captures %.0f%% unique records not found by MeSH.",
      pct_ft_only),
      "Recommendation: Review free-text-only records for relevance.",
      "These may include URPS-era literature not yet remapped by NLM,",
      "or false positives from broad keyword matching."))
  }
  sprintf(paste(
    "Moderate divergence: %.0f%% MeSH-only, %.0f%% free-text-only.",
    "Use combined (MeSH OR free-text) query for maximum recall."),
    pct_mesh_only, pct_ft_only)
}


# ============================================================
# NOMENCLATURE TRANSITION: MESH-AUGMENTED VERSION
# ============================================================

#' Augment Nomenclature Transition Analysis with MeSH Index Data
#'
#' @description
#' Extends \code{compute_nomenclature_transition()} with MeSH-level
#' evidence: adds a column tracking whether each paper was indexed
#' under "female pelvic medicine and reconstructive surgery" [MeSH]
#' (the FPMRS-era heading) vs the expected URPS heading post-2024,
#' and computes the annual MeSH indexing lag relative to the
#' free-text transition.
#'
#' When PMID-to-MeSH mapping data is available (via
#' \code{rentrez::entrez_fetch()} or a pre-fetched table), the
#' function uses it; otherwise it falls back to free-text detection.
#'
#' @param bibliography   Data frame from the pipeline.
#' @param mesh_index_tbl Tibble or NULL. Optional table with columns
#'   \code{pmid} and \code{mesh_heading} (one row per paper-heading pair).
#'   If NULL, falls back to free-text detection only.
#' @param focal_year     Integer. Official rename year (default 2024).
#' @param verbose        Logical.
#'
#' @return The result of \code{compute_nomenclature_transition()} with
#'   an additional element \code{mesh_indexing_summary} — a tibble
#'   with columns \code{publication_year}, \code{n_fpmrs_mesh},
#'   \code{n_urps_mesh}, \code{n_freetext_only}, \code{n_total},
#'   and \code{indexing_lag_flag}.
#'
#' @examples
#' bib <- data.frame(
#'   publication_year = rep(2020:2025, each=10),
#'   TI = c(rep("FPMRS pelvic floor repair outcomes", 30),
#'           rep("URPS reconstructive pelvic surgery", 30)),
#'   AB = "Abstract.",
#'   UT = paste0("W", 1:60),
#'   stringsAsFactors = FALSE
#' )
#' res <- compute_nomenclature_transition_mesh(bib, verbose=FALSE)
#' "mesh_indexing_summary" %in% names(res)
#'
#' # With mesh index table (simulated)
#' mesh_tbl <- data.frame(
#'   pmid = paste0("W", 1:60),
#'   mesh_heading = c(
#'     rep("female pelvic medicine and reconstructive surgery", 30),
#'     rep("pelvic floor", 30)
#'   )
#' )
#' res2 <- compute_nomenclature_transition_mesh(bib,
#'   mesh_index_tbl=mesh_tbl, verbose=FALSE)
#' nrow(res2$mesh_indexing_summary) >= 1L
#'
#' # Transition year detected
#' res3 <- compute_nomenclature_transition_mesh(bib, verbose=FALSE)
#' is.na(res3$transition_year) || is.integer(res3$transition_year)
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr left_join mutate group_by summarise case_when
#' @export
compute_nomenclature_transition_mesh <- function(
    bibliography,
    mesh_index_tbl = NULL,
    focal_year     = 2024L,
    verbose        = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.flag(verbose))

  # Base transition analysis (free-text)
  base_result <- compute_nomenclature_transition(
    bibliography, focal_year=focal_year, verbose=verbose)

  # ── MeSH index augmentation (when data available) ───────────
  if (!is.null(mesh_index_tbl)) {
    assertthat::assert_that(
      all(c("pmid","mesh_heading") %in% names(mesh_index_tbl)),
      msg="mesh_index_tbl must contain 'pmid' and 'mesh_heading'")

    # Normalise PMIDs: use UT as proxy if no PMID column
    bib_pmids <- if ("pmid" %in% names(bibliography)) {
      dplyr::select(bibliography, pmid=.data$pmid,
                    publication_year=.data$publication_year)
    } else if ("UT" %in% names(bibliography)) {
      dplyr::rename(bibliography, pmid=.data$UT) |>
        dplyr::select(.data$pmid, .data$publication_year)
    } else {
      NULL
    }

    if (!is.null(bib_pmids)) {
      # Classify each paper by its MeSH headings
      fpmrs_heading_pattern <- "female pelvic medicine"
      urps_heading_pattern  <- "urps|urogynecology and reconstructive"

      mesh_classified <- mesh_index_tbl |>
        dplyr::mutate(
          is_fpmrs_mesh = grepl(fpmrs_heading_pattern,
                                tolower(.data$mesh_heading)),
          is_urps_mesh  = grepl(urps_heading_pattern,
                                tolower(.data$mesh_heading))
        ) |>
        dplyr::group_by(.data$pmid) |>
        dplyr::summarise(
          n_fpmrs_mesh = sum(.data$is_fpmrs_mesh, na.rm=TRUE),
          n_urps_mesh  = sum(.data$is_urps_mesh,  na.rm=TRUE),
          .groups="drop"
        )

      mesh_summary <- bib_pmids |>
        dplyr::left_join(mesh_classified, by="pmid") |>
        dplyr::mutate(
          n_fpmrs_mesh = dplyr::coalesce(.data$n_fpmrs_mesh, 0L),
          n_urps_mesh  = dplyr::coalesce(.data$n_urps_mesh,  0L),
          freetext_only = .data$n_fpmrs_mesh == 0L & .data$n_urps_mesh == 0L
        ) |>
        dplyr::group_by(.data$publication_year) |>
        dplyr::summarise(
          n_fpmrs_mesh    = sum(.data$n_fpmrs_mesh > 0L, na.rm=TRUE),
          n_urps_mesh     = sum(.data$n_urps_mesh  > 0L, na.rm=TRUE),
          n_freetext_only = sum(.data$freetext_only, na.rm=TRUE),
          n_total         = dplyr::n(),
          .groups         = "drop"
        ) |>
        dplyr::mutate(
          indexing_lag_flag = dplyr::case_when(
            .data$publication_year >= focal_year &
              .data$n_urps_mesh == 0L &
              .data$n_fpmrs_mesh > 0L  ~ "post_rename_still_fpmrs_heading",
            .data$publication_year < focal_year &
              .data$n_urps_mesh > 0L   ~ "pre_rename_urps_heading",
            TRUE                       ~ "expected"
          )
        )

      n_lagged <- sum(mesh_summary$indexing_lag_flag ==
                        "post_rename_still_fpmrs_heading", na.rm=TRUE)
      if (n_lagged > 0L) {
        .log_step(sprintf(paste(
          "[MESH TRANS] %d year(s) post-%d still show FPMRS[MeSH] heading",
          "— NLM indexing lag detected."), n_lagged, focal_year), verbose)
      }

      base_result$mesh_indexing_summary <- mesh_summary
      return(base_result)
    }
  }

  # Fallback: no MeSH index table — return base result with
  # a placeholder summary derived from free-text counts
  .log_step(paste(
    "[MESH TRANS] No mesh_index_tbl supplied.",
    "Returning free-text transition only.",
    "For MeSH-level analysis, supply a mesh_index_tbl from",
    "rentrez::entrez_fetch() or a pre-fetched PMID-heading table."
  ), verbose)

  base_result$mesh_indexing_summary <- dplyr::tibble(
    publication_year = integer(),
    n_fpmrs_mesh     = integer(),
    n_urps_mesh      = integer(),
    n_freetext_only  = integer(),
    n_total          = integer(),
    indexing_lag_flag = character()
  )

  base_result
}


# ============================================================
# ============================================================
#
# SECTION 13: ZERO-INFLATED AND HURDLE COUNT MODELS
#
# Methodological motivation (AltmetricAnalysis two-part outcome):
#   AltmetricAnalysis stratifies outcomes into zero vs. positive
#   scores (logistic) then log-score among positives (linear).
#   The equivalent for FPMRS is the pre-1990 era where many
#   subspecialties have annual publication counts of 0 — a
#   standard negative binomial GLM underestimates zero probability
#   (overdispersion is present but zero-inflation is separate).
#
#   A hurdle model (Mullahy 1986, implemented in pscl::hurdle())
#   is the cleaner specification for count data where zeros arise
#   from a distinct process (e.g. no FPMRS journal existed before
#   a certain year) vs. rare positive events. It mirrors the
#   AltmetricAnalysis two-part model exactly: P(zero) via a
#   binomial part; E(count | count > 0) via a truncated-NB part.
#
# ============================================================
# ============================================================


#' Select the Best Count Model Specification for a Time Series
#'
#' @description
#' Tests four nested specifications for annual publication count data:
#' \enumerate{
#'   \item Standard Poisson GLM
#'   \item Negative Binomial GLM (\code{MASS::glm.nb}) — current pipeline
#'   \item Hurdle model (\code{pscl::hurdle}) — two-part; recommended
#'         when the zero-generating process is distinct from the
#'         count-generating process (e.g. pre-specialty years)
#'   \item Zero-inflated NB (\code{pscl::zeroinfl}) — mixture; recommended
#'         when zeros arise from two processes simultaneously
#' }
#' Returns an AIC comparison table and a recommendation string.
#' Requires \pkg{pscl}; gracefully degrades to NB-only if absent.
#'
#' @param annual_trends   Data frame with \code{publication_year} and
#'   \code{publication_count}.
#' @param year_start      Integer. Used to scale the year predictor.
#' @param zero_threshold  Integer. If the proportion of zero-count years
#'   exceeds this fraction (default 0.15), hurdle/ZINB are tested.
#' @param verbose         Logical.
#'
#' @return Named list:
#' \describe{
#'   \item{comparison}{Tibble: model, AIC, BIC, delta_AIC, n_zero,
#'     pct_zero, zero_inflation_test_pval, recommended.}
#'   \item{best_model}{The fitted model object with the lowest AIC.}
#'   \item{best_model_name}{Character.}
#'   \item{recommendation}{Character. Plain-English recommendation.}
#' }
#'
#' @examples
#' set.seed(1L)
#' # Simulate pre-1990 low-output series with structural zeros
#' ann <- data.frame(
#'   publication_year  = 1975L:2010L,
#'   publication_count = c(rep(0L, 8L),
#'     as.integer(abs(rnorm(28L, 15, 8))))
#' )
#' res <- select_count_model(ann, year_start=1975L, verbose=FALSE)
#' is.character(res$recommendation)
#'
#' # Non-zero series: NB should dominate
#' ann2 <- data.frame(
#'   publication_year  = 1990L:2020L,
#'   publication_count = as.integer(abs(rnorm(31L, 50L, 12L)))
#' )
#' res2 <- select_count_model(ann2, verbose=FALSE)
#' !is.null(res2$best_model)
#'
#' # Check comparison tibble structure
#' res3 <- select_count_model(ann, verbose=FALSE)
#' all(c("model","AIC","delta_AIC","recommended") %in% names(res3$comparison))
#'
#' @importFrom assertthat assert_that is.flag is.count
#' @importFrom dplyr filter mutate bind_rows arrange tibble
#' @importFrom purrr compact map_chr
#' @importFrom MASS glm.nb
#' @export
select_count_model <- function(
    annual_trends,
    year_start       = 1975L,
    zero_threshold   = 0.15,
    verbose          = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(
    all(c("publication_year","publication_count") %in% names(annual_trends)))
  assertthat::assert_that(assertthat::is.flag(verbose))

  df <- dplyr::filter(annual_trends,
                      !is.na(.data$publication_count),
                      .data$publication_count >= 0L) |>
    dplyr::mutate(yr_s = (.data$publication_year - year_start) / 10)

  assertthat::assert_that(nrow(df) >= 5L,
    msg = "Need >= 5 non-NA observations for model selection.")

  n_total     <- nrow(df)
  n_zero      <- sum(df$publication_count == 0L)
  pct_zero    <- n_zero / n_total
  has_pscl    <- requireNamespace("pscl", quietly = TRUE)
  # pscl::hurdle and zeroinfl require >= 1 actual zero in the response.
  # Guard: threshold comparison AND n_zero >= 1 (0 zeros = no zero inflation).
  test_hurdle <- (pct_zero >= zero_threshold) && (n_zero >= 1L)

  .log_step(sprintf(
    "[MODEL SEL] %d obs | %d zeros (%.0f%%) | hurdle=%s | pscl=%s",
    n_total, n_zero, pct_zero * 100,
    if (test_hurdle) "YES" else "NO (< threshold)",
    if (has_pscl) "available" else "MISSING — install pscl"
  ), verbose)

  models <- list()

  # M1: Poisson
  models[["M1_poisson"]] <- tryCatch({
    fit <- glm(publication_count ~ yr_s, data = df,
               family = poisson(link = "log"))
    list(fit = fit, AIC = AIC(fit), BIC = BIC(fit),
         family = "poisson", converged = fit$converged)
  }, error = function(e) NULL)

  # M2: Negative Binomial (current pipeline standard)
  models[["M2_nb"]] <- tryCatch({
    fit <- suppressWarnings(MASS::glm.nb(publication_count ~ yr_s, data = df))
    list(fit = fit, AIC = AIC(fit), BIC = BIC(fit),
         family = "neg_binomial", converged = fit$converged)
  }, error = function(e) NULL)

  # M3: Hurdle NB (only when pscl available and enough zeros)
  if (test_hurdle && has_pscl) {
    models[["M3_hurdle_nb"]] <- tryCatch({
      fit <- pscl::hurdle(
        publication_count ~ yr_s | yr_s,   # count part | zero part
        data = df, dist = "negbin", zero.dist = "binomial"
      )
      list(fit = fit, AIC = AIC(fit), BIC = BIC(fit),
           family = "hurdle_nb", converged = TRUE)
    }, error = function(e) {
      .log_step(sprintf("[MODEL SEL] Hurdle NB failed: %s", e$message), verbose)
      NULL
    })
  }

  # M4: Zero-Inflated NB (only when pscl available and enough zeros)
  if (test_hurdle && has_pscl) {
    models[["M4_zinb"]] <- tryCatch({
      fit <- pscl::zeroinfl(
        publication_count ~ yr_s | yr_s,
        data = df, dist = "negbin"
      )
      list(fit = fit, AIC = AIC(fit), BIC = BIC(fit),
           family = "zero_inflated_nb", converged = TRUE)
    }, error = function(e) {
      .log_step(sprintf("[MODEL SEL] ZINB failed: %s", e$message), verbose)
      NULL
    })
  }

  models <- purrr::compact(models)

  if (length(models) == 0L) {
    .log_step("[MODEL SEL] All models failed.", verbose)
    return(list(comparison = dplyr::tibble(model = character()),
                best_model = NULL, best_model_name = NA_character_,
                recommendation = "All models failed to converge."))
  }

  # Build comparison table
  aic_vals <- purrr::map_dbl(models, "AIC")
  min_aic  <- min(aic_vals, na.rm = TRUE)

  cmp <- dplyr::bind_rows(
    purrr::imap(models, function(m, nm) {
      dplyr::tibble(
        model       = nm,
        family      = m$family,
        AIC         = round(m$AIC, 2L),
        BIC         = round(m$BIC, 2L),
        delta_AIC   = round(m$AIC - min_aic, 2L),
        n_zero      = n_zero,
        pct_zero    = round(pct_zero * 100, 1L),
        recommended = m$AIC == min_aic
      )
    })
  ) |> dplyr::arrange(.data$AIC)

  best_nm  <- cmp$model[[1L]]
  best_fit <- models[[best_nm]]$fit

  # Recommendation text
  recommendation <- if (best_nm %in% c("M3_hurdle_nb","M4_zinb")) {
    sprintf(paste(
      "%s selected (AIC=%.1f, ΔAIC vs NB=%.1f).",
      "%.0f%% of years have zero publications.",
      "Two-part model appropriate: zeros arise from a distinct process",
      "(no publications possible before specialty existed).",
      "Report both the binomial zero-part and the NB count-part in methods."),
      best_nm, min_aic,
      cmp$delta_AIC[cmp$model == "M2_nb"],
      pct_zero * 100)
  } else if (best_nm == "M2_nb") {
    sprintf(paste(
      "Negative Binomial GLM (M2_nb) selected (AIC=%.1f).",
      "%.0f%% zero years — below hurdle threshold.",
      "Standard NB specification is appropriate."),
      min_aic, pct_zero * 100)
  } else {
    sprintf("Poisson GLM selected (AIC=%.1f). Check for overdispersion.", min_aic)
  }

  if (verbose) {
    .log_step("[MODEL SEL] Results:", TRUE)
    for (i in seq_len(nrow(cmp))) {
      message(sprintf("  %-18s AIC=%8.1f  ΔAIC=%5.1f  %s",
        cmp$model[[i]], cmp$AIC[[i]], cmp$delta_AIC[[i]],
        if (cmp$recommended[[i]]) "<-- BEST" else ""))
    }
    message(sprintf("[MODEL SEL] %s", recommendation))
  }

  list(comparison = cmp, best_model = best_fit,
       best_model_name = best_nm, recommendation = recommendation)
}


#' Fit Hurdle or Zero-Inflated NB Models Across All Subspecialty Strata
#'
#' @description
#' Applies \code{select_count_model()} per subspecialty via
#' \code{fit_models_per_stratum()}, automatically selecting the best
#' count model specification for each stratum independently. Subspecialties
#' with few structural zeros (post-2000 data) get standard NB; low-output
#' or pre-1990 subspecialties get hurdle or ZINB automatically.
#'
#' This mirrors the AltmetricAnalysis two-part outcome pattern:
#' their binary (bioRxiv / non-bioRxiv) × outcome (zero / positive)
#' stratification becomes your zero-inflation × subspecialty stratification.
#'
#' @param bibliography     Combined bibliography with \code{subspecialty}
#'   and \code{publication_year} columns.
#' @param annual_trends_list Named list of per-subspecialty annual trends
#'   (output of \code{.compute_annual_publication_trends()} per stratum).
#'   If \code{NULL}, computed from \code{bibliography}.
#' @param year_start       Integer.
#' @param zero_threshold   Numeric. Passed to \code{select_count_model()}.
#' @param cache_dir        Character.
#' @param force_rerun      Logical.
#' @param verbose          Logical.
#'
#' @return Named list: one element per subspecialty, each being the
#'   output of \code{select_count_model()}.
#'
#' @examples
#' set.seed(42L)
#' bib <- data.frame(
#'   subspecialty     = rep(c("FPMRS","REI"), each=40L),
#'   publication_year = rep(c(rep(1980L:1984L, each=2L),
#'                             rep(1985L:2019L, each=1L)), 2L),
#'   publication_count = c(
#'     c(rep(0L,6L), as.integer(abs(rnorm(34L,20L,6L)))),
#'     as.integer(abs(rnorm(40L, 40L, 10L)))
#'   ),
#'   stringsAsFactors = FALSE
#' )
#' res <- fit_count_models_all_strata(bib, verbose=FALSE,
#'   cache_dir=tempdir())
#' all(c("FPMRS","REI") %in% names(res))
#'
#' # Each element has comparison + recommendation
#' all(purrr::map_lgl(res, ~"recommendation" %in% names(.x)))
#'
#' # FPMRS should flag zero-inflation, REI should not
#' length(res) == 2L
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr filter group_by group_split
#' @importFrom purrr map set_names compact
#' @export
fit_count_models_all_strata <- function(
    bibliography,
    annual_trends_list = NULL,
    year_start         = 1975L,
    zero_threshold     = 0.15,
    cache_dir          = file.path("data","cache","rds","count_models"),
    force_rerun        = FALSE,
    verbose            = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.flag(verbose))

  # Build annual trends if not supplied
  if (is.null(annual_trends_list)) {
    .log_step("[COUNT MODELS] Building annual trends per stratum ...", verbose)
    assertthat::assert_that(
      all(c("subspecialty","publication_year") %in% names(bibliography)))

    strata_bibs <- bibliography |>
      dplyr::filter(!is.na(.data$subspecialty)) |>
      dplyr::group_by(.data$subspecialty) |>
      dplyr::group_split(.keep = TRUE)

    strata_names <- purrr::map_chr(strata_bibs,
      ~as.character(.x$subspecialty[[1L]]))

    annual_trends_list <- purrr::map(strata_bibs, function(sb) {
      sb |>
        dplyr::count(.data$publication_year,
                     name = "publication_count") |>
        dplyr::mutate(publication_count = as.integer(.data$publication_count))
    }) |> purrr::set_names(strata_names)
  }

  strata_names <- names(annual_trends_list)
  .log_step(sprintf("[COUNT MODELS] Selecting model for %d strata: %s",
    length(strata_names), paste(sort(strata_names), collapse=", ")), verbose)

  fit_fn <- function(df) {
    # df is a single-stratum annual_trends tibble (from group_split)
    # pull the stratum name from subspecialty col if present,
    # otherwise just use the data frame directly
    ann_df <- if ("publication_count" %in% names(df)) df
              else dplyr::count(df, .data$publication_year,
                                name = "publication_count")
    select_count_model(ann_df, year_start = year_start,
                       zero_threshold = zero_threshold, verbose = FALSE)
  }

  # Wrap annual_trends_list as a list of data frames for fit_models_per_stratum
  combined_df <- dplyr::bind_rows(
    purrr::imap(annual_trends_list, function(at, nm) {
      dplyr::mutate(at, subspecialty = nm)
    })
  )

  fit_models_per_stratum(
    bibliography = combined_df,
    strat_col    = "subspecialty",
    fit_fn       = fit_fn,
    model_type   = "count_model_selection",
    cache_dir    = cache_dir,
    force_rerun  = force_rerun,
    verbose      = verbose
  )
}


# ============================================================
# ============================================================
#
# SECTION 14: GENDER PACKAGE INTEGRATION
#
# Augments the existing internal lookup table in
# .infer_gender_vectorized() with optional external packages:
#   - gender package (US SSA data 1880-2012, Mullen 2014)
#   - genderizeR package (probabilistic API)
#
# The cascade is: internal lookup → gender package →
#   genderizeR → NA. This preserves the O(1) vectorised
#   lookup for common names while filling gaps for rare or
#   international first names that the hard-coded list misses.
#
# ============================================================
# ============================================================


#' Infer Gender with External Package Cascade
#'
#' @description
#' Extends the pipeline's internal first-name gender lookup with a
#' cascade through the \pkg{gender} package (US Social Security
#' Administration baby name data 1880–2012) and optionally
#' \pkg{genderizeR} (probabilistic REST API). The three-level cascade
#' maximises classification rate for the full 1975–2025 authorship
#' record while keeping API calls only for names that genuinely
#' need them.
#'
#' Cascade order (stops at first non-NA result):
#' \enumerate{
#'   \item Internal hard-coded lookup (O(1), offline, fastest)
#'   \item \code{gender::gender()} — SSA 1932-2012 data (offline)
#'   \item \code{genderizeR::genderizeAPI()} — REST API (online, rate-limited)
#' }
#'
#' @param first_names     Character vector of first names (lowercase or
#'   mixed case; NA entries return NA).
#' @param use_gender_pkg  Logical. Use \pkg{gender} package as fallback.
#' @param use_genderizeR  Logical. Use \pkg{genderizeR} API as fallback
#'   (requires internet; rate limits apply; set FALSE for offline use).
#' @param probability_min Numeric (0–1). For \pkg{genderizeR} results,
#'   only accept classifications above this probability. Default 0.80.
#' @param birth_year_range Integer(2). Year range for \pkg{gender} SSA
#'   lookup. Default \code{c(1940L, 1985L)} (covers most active authors
#'   in 1975–2024 literature).
#' @param verbose         Logical.
#'
#' @return Character vector, same length as \code{first_names}, with
#'   values \code{"Male"}, \code{"Female"}, or \code{NA_character_}.
#'
#' @examples
#' names_vec <- c("Jane", "Robert", "Kelly", "Anita", "Wei", NA)
#' g <- infer_gender_cascade(names_vec, use_genderizeR=FALSE, verbose=FALSE)
#' length(g) == length(names_vec)
#'
#' g2 <- infer_gender_cascade(c("Emily","Michael"),
#'   use_gender_pkg=TRUE, use_genderizeR=FALSE, verbose=FALSE)
#' all(g2 %in% c("Male","Female",NA_character_))
#'
#' # NA passes through as NA
#' g3 <- infer_gender_cascade(c("John", NA_character_),
#'   use_genderizeR=FALSE, verbose=FALSE)
#' is.na(g3[[2L]])
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr coalesce
#' @importFrom purrr map_chr
#' @export
infer_gender_cascade <- function(
    first_names,
    use_gender_pkg  = TRUE,
    use_genderizeR  = FALSE,
    probability_min = 0.80,
    birth_year_range = c(1940L, 1985L),
    verbose          = TRUE
) {
  assertthat::assert_that(is.character(first_names))
  assertthat::assert_that(assertthat::is.flag(use_gender_pkg))
  assertthat::assert_that(assertthat::is.flag(use_genderizeR))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    is.numeric(probability_min) && probability_min >= 0 && probability_min <= 1)

  n <- length(first_names)
  result <- rep(NA_character_, n)

  # ── Level 1: internal vectorised lookup ─────────────────────
  # .infer_gender_vectorized() expects AF strings like "Last, First M",
  # not bare first names. Wrap each name as "PLACEHOLDER, <name>" so the
  # WoS parsing path extracts it correctly regardless of case.
  af_wrapped <- dplyr::if_else(
    is.na(first_names),
    NA_character_,
    paste0("PLACEHOLDER, ", first_names)
  )
  internal_genders <- .infer_gender_vectorized(af_wrapped)
  result <- dplyr::coalesce(internal_genders, result)
  n_classified_l1 <- sum(!is.na(result))

  .log_step(sprintf(
    "[GENDER CASCADE] L1 internal: %d/%d classified (%.0f%%)",
    n_classified_l1, n, n_classified_l1 / max(n, 1L) * 100), verbose)

  still_na <- which(is.na(result))

  # ── Level 2: gender package (SSA data, offline) ─────────────
  if (use_gender_pkg && length(still_na) > 0L) {
    if (!requireNamespace("gender", quietly = TRUE)) {
      .log_step(paste("[GENDER CASCADE] 'gender' package not installed.",
                      "Install: install.packages('gender')"), verbose)
    } else {
      unique_na_names <- unique(tolower(trimws(first_names[still_na])))
      unique_na_names <- unique_na_names[!is.na(unique_na_names) &
                                           nchar(unique_na_names) > 0L]

      gender_results <- tryCatch({
        suppressMessages(gender::gender(
          unique_na_names,
          years = birth_year_range,
          method = "ssa"
        ))
      }, error = function(e) {
        .log_step(sprintf("[GENDER CASCADE] gender::gender() failed: %s",
                          e$message), verbose)
        NULL
      })

      if (!is.null(gender_results) && nrow(gender_results) > 0L) {
        # gender package returns proportion_female; threshold at 0.5
        gender_map <- stats::setNames(
          dplyr::if_else(gender_results$proportion_female >= 0.5,
                         "Female", "Male"),
          tolower(gender_results$name)
        )
        l2_fill <- gender_map[tolower(trimws(first_names[still_na]))]
        result[still_na[!is.na(l2_fill)]] <- l2_fill[!is.na(l2_fill)]
        n_l2 <- sum(!is.na(l2_fill), na.rm = TRUE)
        still_na <- which(is.na(result))
        .log_step(sprintf(
          "[GENDER CASCADE] L2 gender pkg: +%d classified (%d remaining NA)",
          n_l2, length(still_na)), verbose)
      }
    }
  }

  # ── Level 3: genderizeR REST API (online, rate-limited) ────
  if (use_genderizeR && length(still_na) > 0L) {
    if (!requireNamespace("genderizeR", quietly = TRUE)) {
      .log_step(paste("[GENDER CASCADE] 'genderizeR' not installed.",
                      "Install: install.packages('genderizeR')"), verbose)
    } else {
      unique_na_names <- unique(trimws(first_names[still_na]))
      unique_na_names <- unique_na_names[!is.na(unique_na_names) &
                                           nchar(unique_na_names) > 0L]
      .log_step(sprintf(
        "[GENDER CASCADE] L3 genderizeR: querying API for %d unique names ...",
        length(unique_na_names)), verbose)

      api_results <- tryCatch(
        genderizeR::genderizeAPI(unique_na_names),
        error = function(e) {
          .log_step(sprintf("[GENDER CASCADE] genderizeR API failed: %s",
                            e$message), verbose)
          NULL
        }
      )

      if (!is.null(api_results) && nrow(api_results) > 0L) {
        # genderizeR returns: name, gender, probability, count
        accepted <- dplyr::filter(api_results,
          !is.na(.data$gender),
          !is.na(.data$probability),
          .data$probability >= probability_min
        )
        if (nrow(accepted) > 0L) {
          gz_map <- stats::setNames(
            dplyr::if_else(accepted$gender == "female", "Female", "Male"),
            tolower(accepted$name)
          )
          l3_fill <- gz_map[tolower(trimws(first_names[still_na]))]
          result[still_na[!is.na(l3_fill)]] <- l3_fill[!is.na(l3_fill)]
          n_l3 <- sum(!is.na(l3_fill), na.rm = TRUE)
          .log_step(sprintf(
            "[GENDER CASCADE] L3 genderizeR: +%d classified (prob >= %.2f)",
            n_l3, probability_min), verbose)
        }
      }
    }
  }

  n_final_na <- sum(is.na(result))
  .log_step(sprintf(
    "[GENDER CASCADE] Final: %d/%d classified | %d unresolved (%.0f%%)",
    n - n_final_na, n, n_final_na,
    n_final_na / max(n, 1L) * 100), verbose)

  result
}


#' Build Gender Parity Trajectory for OB/GYN Subspecialties
#'
#' @description
#' Computes annual first-author female proportion for each subspecialty
#' using the three-level gender cascade, then models the trend in female
#' authorship over time using a logistic (proportion) regression.
#' Gender parity in OB/GYN authorship 1975–2024 is a publishable
#' secondary finding requiring no additional data collection.
#'
#' @param bibliography   Combined data frame with \code{subspecialty},
#'   \code{publication_year}, and \code{AF} (author full names).
#' @param use_gender_pkg Logical. Passed to \code{infer_gender_cascade()}.
#' @param use_genderizeR Logical. Passed to \code{infer_gender_cascade()}.
#' @param min_papers_per_year Integer. Minimum papers in a year for that
#'   year to be included in the trend model.
#' @param verbose        Logical.
#'
#' @return Named list:
#' \describe{
#'   \item{annual_parity}{Tibble: subspecialty, publication_year,
#'     n_female, n_male, n_unclassified, n_total, pct_female.}
#'   \item{trend_models}{Named list of \code{glm} objects (binomial),
#'     one per subspecialty.}
#'   \item{trend_summary}{Tibble: subspecialty, slope_per_decade,
#'     p_value, ci_lo, ci_hi, pct_female_1975, pct_female_2024,
#'     parity_year (year when pct_female crosses 50%).}
#'   \item{figure}{ggplot object: faceted trend lines with 95% CI.}
#' }
#'
#' @examples
#' set.seed(1L)
#' bib <- data.frame(
#'   subspecialty     = rep(c("FPMRS","REI"), each=60L),
#'   publication_year = rep(rep(1990L:2019L, each=2L), 2L),
#'   AF = rep(c("Smith Jane A;Jones Robert B",
#'              "Lee Michael C;Park Anna D"), 60L),
#'   stringsAsFactors = FALSE
#' )
#' res <- compute_gender_parity_trajectory(bib, verbose=FALSE)
#' "annual_parity" %in% names(res)
#'
#' nrow(res$annual_parity) >= 1L
#'
#' all(c("trend_models","trend_summary","figure") %in% names(res))
#'
#' @importFrom assertthat assert_that is.flag is.count
#' @importFrom dplyr filter mutate group_by summarise left_join
#'   arrange bind_rows n
#' @importFrom purrr map imap map_dfr set_names
#' @importFrom tibble tibble
#' @importFrom ggplot2 ggplot aes geom_line geom_ribbon geom_hline
#'   facet_wrap scale_y_continuous labs theme_bw theme element_text
#' @export
compute_gender_parity_trajectory <- function(
    bibliography,
    use_gender_pkg       = TRUE,
    use_genderizeR       = FALSE,
    min_papers_per_year  = 3L,
    verbose              = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(
    all(c("subspecialty","publication_year","AF") %in% names(bibliography)),
    msg = "bibliography must contain: subspecialty, publication_year, AF")

  .log_step(sprintf(
    "[PARITY] Computing gender parity trajectory for %d papers ...",
    nrow(bibliography)), verbose)

  # Extract first author from each AF field
  first_names <- purrr::map_chr(bibliography$AF, function(af) {
    if (is.na(af) || nchar(trimws(af)) == 0L) return(NA_character_)
    first_au <- stringr::str_extract(af, "^[^;]+")
    # Handle WoS "Last, First" or PubMed "Last First" formats
    if (grepl(",", first_au, fixed = TRUE)) {
      stringr::str_extract(first_au, "(?<=,\\s{0,5})[A-Za-z]+")
    } else {
      stringr::str_extract(first_au, "^[^\\s]+\\s+([A-Za-z]+)", group = 1L)
    }
  })

  genders <- infer_gender_cascade(
    first_names,
    use_gender_pkg  = use_gender_pkg,
    use_genderizeR  = use_genderizeR,
    verbose         = verbose
  )

  bib_gen <- dplyr::mutate(bibliography,
    first_author_gender = genders,
    is_female = as.integer(.data$first_author_gender == "Female"),
    is_male   = as.integer(.data$first_author_gender == "Male")
  )

  # Annual parity by subspecialty
  annual_parity <- bib_gen |>
    dplyr::filter(!is.na(.data$subspecialty),
                  !is.na(.data$publication_year)) |>
    dplyr::group_by(.data$subspecialty, .data$publication_year) |>
    dplyr::summarise(
      n_female       = sum(.data$is_female, na.rm = TRUE),
      n_male         = sum(.data$is_male,   na.rm = TRUE),
      n_unclassified = sum(is.na(.data$first_author_gender)),
      n_total        = dplyr::n(),
      .groups        = "drop"
    ) |>
    dplyr::mutate(
      pct_female = dplyr::if_else(
        (.data$n_female + .data$n_male) > 0L,
        round(.data$n_female / (.data$n_female + .data$n_male) * 100, 1L),
        NA_real_
      )
    ) |>
    dplyr::arrange(.data$subspecialty, .data$publication_year)

  # Logistic trend models per subspecialty
  subspecialties <- sort(unique(annual_parity$subspecialty))

  trend_models <- purrr::map(subspecialties, function(sp) {
    sp_data <- dplyr::filter(annual_parity,
      .data$subspecialty == sp,
      !is.na(.data$pct_female),
      (.data$n_female + .data$n_male) >= min_papers_per_year
    ) |>
      dplyr::mutate(
        yr_s     = (.data$publication_year - 1975L) / 10,
        n_class  = .data$n_female + .data$n_male
      )

    if (nrow(sp_data) < 5L) return(NULL)

    tryCatch(
      glm(cbind(n_female, n_male) ~ yr_s,
          data = sp_data, family = binomial(link = "logit")),
      error = function(e) NULL
    )
  }) |> purrr::set_names(subspecialties)

  # Trend summary
  trend_summary <- purrr::map_dfr(subspecialties, function(sp) {
    fit <- trend_models[[sp]]
    if (is.null(fit)) {
      return(dplyr::tibble(subspecialty=sp, slope_per_decade=NA_real_,
        p_value=NA_real_, ci_lo=NA_real_, ci_hi=NA_real_,
        pct_female_start=NA_real_, pct_female_end=NA_real_,
        parity_year=NA_integer_))
    }
    coef_est <- coef(fit)[["yr_s"]]
    ci <- tryCatch(suppressMessages(confint(fit, "yr_s", level=0.95)),
                   error=function(e) c(NA_real_, NA_real_))
    p_val <- summary(fit)$coefficients["yr_s","Pr(>|z|)"]

    # Predicted pct_female at start and end years
    sp_data <- dplyr::filter(annual_parity, .data$subspecialty == sp,
                              !is.na(.data$pct_female))
    yr_range <- range(sp_data$publication_year, na.rm=TRUE)
    pred_start <- 100 * stats::plogis(
      coef(fit)[["(Intercept)"]] +
      coef(fit)[["yr_s"]] * (yr_range[[1L]] - 1975L) / 10)
    pred_end <- 100 * stats::plogis(
      coef(fit)[["(Intercept)"]] +
      coef(fit)[["yr_s"]] * (yr_range[[2L]] - 1975L) / 10)

    # Year parity is predicted to reach 50%
    # logit(0.5) = 0; solve: intercept + slope * yr_s = 0
    parity_yr <- if (!is.na(coef_est) && coef_est > 0) {
      as.integer(round(1975L - coef(fit)[["(Intercept)"]] /
                       coef(fit)[["yr_s"]] * 10))
    } else NA_integer_

    dplyr::tibble(
      subspecialty      = sp,
      slope_per_decade  = round(coef_est, 4L),
      p_value           = round(p_val, 5L),
      ci_lo             = round(ci[[1L]], 4L),
      ci_hi             = round(ci[[2L]], 4L),
      pct_female_start  = round(pred_start, 1L),
      pct_female_end    = round(pred_end, 1L),
      parity_year       = parity_yr
    )
  })

  # Figure: faceted parity trends
  fig <- ggplot2::ggplot(
    dplyr::filter(annual_parity, !is.na(.data$pct_female)),
    ggplot2::aes(x = .data$publication_year, y = .data$pct_female)
  ) +
    ggplot2::geom_line(colour = "#e31a1c", linewidth = 0.7, na.rm = TRUE) +
    ggplot2::geom_hline(yintercept = 50, linetype = "dashed",
                        colour = "grey50", linewidth = 0.5) +
    ggplot2::facet_wrap(~subspecialty, scales = "free_y") +
    ggplot2::scale_y_continuous(
      labels = function(x) paste0(x, "%"),
      limits = c(0, 100)
    ) +
    ggplot2::labs(
      title    = "First-Author Female Proportion by Subspecialty",
      subtitle = "Dashed line = 50% parity; gender inferred via cascade lookup",
      x        = NULL,
      y        = "% First-Author Female"
    ) +
    ggplot2::theme_bw(base_size = 10) +
    ggplot2::theme(
      strip.background = ggplot2::element_blank(),
      strip.text       = ggplot2::element_text(face = "bold"),
      panel.grid.minor = ggplot2::element_blank()
    )

  list(
    annual_parity = annual_parity,
    trend_models  = trend_models,
    trend_summary = trend_summary,
    figure        = fig
  )
}


# ============================================================
# ============================================================
#
# SECTION 15: PHASE 1 — STRUCTURAL PATTERNS
#   #8  Canonical directory structure helpers
#   #9  Time-series diagnostic suite
#   #10 Event-marker visualisation layer
#
# Source repos: mkiang/excess_physician_mortality,
#               dgarcia-eu/EATLancetR
# ============================================================
# ============================================================


# ── #8 Directory helpers ─────────────────────────────────────

#' Create Canonical FPMRS Project Directory Structure
#'
#' @description
#' Builds the mkiang-convention scaffold used across 15+ published
#' reproducible-research repos. Creates every directory idempotently
#' (no error if already present) and writes a \code{.here} sentinel
#' file so \code{here::here()} resolves correctly from any working
#' directory.
#'
#' Directory map:
#' \describe{
#'   \item{code/}{Numbered stage scripts (\code{01_*.R}, …)}
#'   \item{data_raw/}{Downloaded immutable inputs (git-ignored)}
#'   \item{data/}{Processed / cached files (DuckDB, .rds)}
#'   \item{output/}{All generated artefacts}
#'   \item{plots/}{Figures (PDF, PNG, SVG)}
#'   \item{rmds/}{RMarkdown manuscript source}
#'   \item{logs/}{Per-run log files}
#'   \item{tests/testthat/}{testthat unit tests}
#' }
#'
#' @param root    Character. Project root (default: \code{here::here()}).
#' @param verbose Logical.
#'
#' @return Invisibly, a character vector of created directories.
#'
#' @examples
#' tmp <- tempdir()
#' dirs <- create_project_structure(root = tmp, verbose = FALSE)
#' all(dir.exists(dirs))
#'
#' # Idempotent — safe to call twice
#' dirs2 <- create_project_structure(root = tmp, verbose = FALSE)
#' identical(dirs, dirs2)
#'
#' # Returns canonical paths
#' all(startsWith(dirs, tmp))
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @export
create_project_structure <- function(
    root    = here::here(),
    verbose = TRUE
) {
  assertthat::assert_that(assertthat::is.string(root))
  assertthat::assert_that(assertthat::is.flag(verbose))

  dirs <- file.path(root, c(
    "code",
    "data_raw", "data_raw/pubmed", "data_raw/wos",
    "data",     "data/cache",     "data/processed",
    "output",   "output/tables",  "output/manifests",
    "plots",    "plots/single",   "plots/comparison",
    "plots/geographic",
    "rmds",
    "logs",
    "tests", "tests/testthat"
  ))

  purrr::walk(dirs, function(d) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE, showWarnings = FALSE)
      .log_step(sprintf("[SCAFFOLD] Created: %s", d), verbose)
    }
  })

  # Write .here sentinel so here::here() resolves to root
  here_file <- file.path(root, ".here")
  if (!file.exists(here_file)) {
    writeLines(character(0L), here_file)
    .log_step(sprintf("[SCAFFOLD] Wrote .here sentinel: %s", here_file), verbose)
  }

  .log_step(sprintf("[SCAFFOLD] Project structure ready under: %s", root), verbose)
  invisible(dirs)
}


# ── #9 Time-series diagnostic suite ─────────────────────────

#' Run a Full Time-Series Diagnostic Suite on Annual Trends
#'
#' @description
#' Applies a battery of pre-modelling diagnostics adapted from the
#' dgarcia-eu/EATLancetR and mkiang/excess_physician_mortality
#' workflows. Results should be reviewed before selecting a trend
#' model — non-stationarity, overdispersion, or autocorrelation
#' each call for different model specifications.
#'
#' Tests performed:
#' \describe{
#'   \item{ADF (Augmented Dickey-Fuller)}{Stationarity: H0 = unit root.
#'     Rejection (p < 0.05) means series is stationary.}
#'   \item{KPSS}{Stationarity (complementary): H0 = stationary.
#'     Rejection means non-stationary.}
#'   \item{Ljung-Box}{Autocorrelation in residuals from a linear trend
#'     fit: H0 = no autocorrelation.}
#'   \item{Jarque-Bera}{Normality of first differences:
#'     H0 = normally distributed.}
#'   \item{Overdispersion}{Variance/mean ratio of counts. Ratio > 2
#'     strongly favours NB over Poisson.}
#'   \item{Mann-Kendall}{Monotone trend (reinforces existing pipeline
#'     output for convenient comparison).}
#' }
#'
#' @param annual_trends Data frame with \code{publication_year} and
#'   \code{publication_count}.
#' @param alpha Numeric. Significance threshold (default 0.05).
#' @param verbose Logical.
#'
#' @return A tibble with columns \code{test}, \code{statistic},
#'   \code{p_value}, \code{conclusion}, and \code{recommendation}.
#'
#' @examples
#' set.seed(1L)
#' ann <- data.frame(
#'   publication_year  = 1990L:2020L,
#'   publication_count = as.integer(abs(rnorm(31L, 50L, 15L)))
#' )
#' diag <- run_ts_diagnostics(ann, verbose = FALSE)
#' nrow(diag) >= 4L
#'
#' all(c("test","p_value","recommendation") %in% names(diag))
#'
#' # Works on short series (≥ 5 obs)
#' run_ts_diagnostics(ann[1:8, ], verbose = FALSE) |> nrow() >= 1L
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr tibble bind_rows mutate case_when
#' @importFrom stats lm resid var mean
#' @export
run_ts_diagnostics <- function(
    annual_trends,
    alpha   = 0.05,
    verbose = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(
    all(c("publication_year","publication_count") %in% names(annual_trends)))
  assertthat::assert_that(assertthat::is.flag(verbose))

  df <- dplyr::filter(annual_trends,
    !is.na(.data$publication_count),
    .data$publication_count >= 0L) |>
    dplyr::arrange(.data$publication_year)

  if (nrow(df) < 5L) {
    .log_step("[TS DIAG] Fewer than 5 obs — returning empty diagnostics.",
              verbose)
    return(dplyr::tibble(test=character(), statistic=numeric(),
                          p_value=numeric(), conclusion=character(),
                          recommendation=character()))
  }

  counts    <- df$publication_count
  y_ts      <- ts(counts)
  y_diff    <- diff(counts)
  disp_ratio <- var(counts) / max(mean(counts), 0.01)

  results <- list()

  # ADF test (requires tseries)
  if (requireNamespace("tseries", quietly = TRUE)) {
    adf <- tryCatch(tseries::adf.test(y_ts), error = function(e) NULL)
    if (!is.null(adf)) {
      results[["ADF"]] <- dplyr::tibble(
        test       = "ADF (stationarity)",
        statistic  = round(as.numeric(adf$statistic), 3L),
        p_value    = round(adf$p.value, 4L),
        conclusion = if (adf$p.value < alpha) "Stationary (reject unit root)"
                     else "Non-stationary (unit root likely)",
        recommendation = if (adf$p.value < alpha)
          "OK for trend modelling"
        else
          "Consider first-differencing or detrending before ITS"
      )
    }

    # KPSS test (complementary)
    kpss <- tryCatch(tseries::kpss.test(y_ts), error = function(e) NULL)
    if (!is.null(kpss)) {
      results[["KPSS"]] <- dplyr::tibble(
        test       = "KPSS (stationarity)",
        statistic  = round(as.numeric(kpss$statistic), 3L),
        p_value    = round(kpss$p.value, 4L),
        conclusion = if (kpss$p.value < alpha) "Non-stationary (reject H0)"
                     else "Stationary (fail to reject H0)",
        recommendation = if (kpss$p.value >= alpha)
          "OK for trend modelling"
        else
          "Consistent with ADF: consider differencing"
      )
    }

    # Jarque-Bera on first differences
    if (length(y_diff) >= 4L) {
      jb <- tryCatch(tseries::jarque.bera.test(y_diff),
                     error = function(e) NULL)
      if (!is.null(jb)) {
        results[["JB"]] <- dplyr::tibble(
          test       = "Jarque-Bera (normality of diffs)",
          statistic  = round(as.numeric(jb$statistic), 3L),
          p_value    = round(jb$p.value, 4L),
          conclusion = if (jb$p.value < alpha) "Non-normal differences"
                       else "Approximately normal differences",
          recommendation = if (jb$p.value >= alpha)
            "Linear trend residuals acceptable"
          else
            "Use robust (Theil-Sen) slope estimator"
        )
      }
    }
  }

  # Ljung-Box autocorrelation in OLS residuals
  ols_fit <- tryCatch(
    lm(publication_count ~ publication_year, data = df),
    error = function(e) NULL)
  if (!is.null(ols_fit)) {
    lb <- tryCatch(
      stats::Box.test(stats::resid(ols_fit), lag = min(10L, nrow(df) - 2L),
                       type = "Ljung-Box"),
      error = function(e) NULL)
    if (!is.null(lb)) {
      results[["LB"]] <- dplyr::tibble(
        test       = "Ljung-Box (autocorrelation in OLS residuals)",
        statistic  = round(lb$statistic, 3L),
        p_value    = round(lb$p.value, 4L),
        conclusion = if (lb$p.value < alpha) "Significant autocorrelation"
                     else "No significant autocorrelation",
        recommendation = if (lb$p.value >= alpha)
          "OLS residuals are approximately independent"
        else
          "Add AR(1) term or use HAC-robust standard errors"
      )
    }
  }

  # Overdispersion
  results[["OD"]] <- dplyr::tibble(
    test       = "Overdispersion (var/mean ratio)",
    statistic  = round(disp_ratio, 2L),
    p_value    = NA_real_,
    conclusion = dplyr::case_when(
      disp_ratio > 4  ~ "Severe overdispersion",
      disp_ratio > 2  ~ "Moderate overdispersion",
      disp_ratio > 1.5 ~ "Mild overdispersion",
      TRUE             ~ "Equidispersed"
    ),
    recommendation = dplyr::case_when(
      disp_ratio > 2  ~ "Negative binomial or ZINB strongly preferred over Poisson",
      disp_ratio > 1.5 ~ "Negative binomial preferred; check hurdle model",
      TRUE             ~ "Poisson may be adequate; verify with AIC comparison"
    )
  )

  # Mann-Kendall (reuses existing pipeline)
  if (nrow(df) >= 4L) {
    mk_res <- tryCatch(
      .compute_mann_kendall_trend(df, verbose = FALSE),
      error = function(e) NULL)
    if (!is.null(mk_res) && "mk_tau" %in% names(mk_res)) {
      results[["MK"]] <- dplyr::tibble(
        test       = "Mann-Kendall (monotone trend)",
        statistic  = round(mk_res$mk_tau[[1L]], 3L),
        p_value    = round(mk_res$mk_p[[1L]], 4L),
        conclusion = if (mk_res$mk_p[[1L]] < alpha)
          paste0("Significant ", if (mk_res$mk_tau[[1L]] > 0) "increasing"
                 else "decreasing", " trend")
        else "No significant monotone trend",
        recommendation = if (mk_res$mk_p[[1L]] < alpha)
          "Trend modelling warranted; use Theil-Sen for slope estimate"
        else
          "Examine scatter plot before concluding no trend"
      )
    }
  }

  out <- dplyr::bind_rows(results)

  if (verbose && nrow(out) > 0L) {
    .log_step("[TS DIAG] Diagnostic summary:", TRUE)
    purrr::walk(seq_len(nrow(out)), function(i) {
      r <- out[i, ]
      message(sprintf("  %-42s  p=%s  → %s",
        r$test,
        if (is.na(r$p_value)) "N/A  " else sprintf("%.4f", r$p_value),
        r$conclusion))
    })
  }

  out
}


# ── #10 Event-marker layer ───────────────────────────────────

#' Add Policy-Event Markers to an Existing Time-Series ggplot
#'
#' @description
#' Appends vertical reference lines and annotation labels for
#' named policy events — adapting the \code{geom_vline + annotate}
#' pattern from dgarcia-eu/EATLancetR. Particularly useful for
#' marking FDA mesh withdrawal (2019), FPMRS→URPS rename (2024),
#' and PubMed open access (2000) on existing trend plots.
#'
#' @param plot        An existing \code{ggplot} object.
#' @param events      Named numeric vector of event years, e.g.
#'   \code{c("FDA mesh withdrawal" = 2019, "URPS rename" = 2024)}.
#'   If \code{NULL}, uses the built-in FPMRS event table.
#' @param y_frac      Numeric (0–1). Vertical position of label as a
#'   fraction of the y-axis range. Default 0.92.
#' @param line_colour Character. Colour for the vertical lines.
#' @param line_lty    Integer. Line type (1=solid, 2=dashed).
#' @param label_size  Numeric. Text size for annotations.
#' @param label_angle Numeric. Label rotation in degrees.
#' @param verbose     Logical.
#'
#' @return The original \code{ggplot} object with event markers added.
#'
#' @examples
#' library(ggplot2)
#' df <- data.frame(year=1990:2025, n=cumsum(rpois(36, 30)))
#' p  <- ggplot(df, aes(year, n)) + geom_line()
#'
#' # Default FPMRS events
#' p1 <- add_event_markers(p, verbose = FALSE)
#' inherits(p1, "gg")
#'
#' # Custom events
#' p2 <- add_event_markers(p,
#'   events = c("My event" = 2010, "Another" = 2018),
#'   verbose = FALSE)
#' inherits(p2, "gg")
#'
#' # Adjust appearance
#' p3 <- add_event_markers(p, line_colour = "steelblue",
#'   line_lty = 1L, label_size = 2.5, verbose = FALSE)
#' inherits(p3, "gg")
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom ggplot2 geom_vline annotate
#' @export
add_event_markers <- function(
    plot,
    events      = NULL,
    y_frac      = 0.92,
    line_colour = "#c0392b",
    line_lty    = 2L,
    label_size  = 2.8,
    label_angle = 90,
    verbose     = TRUE
) {
  assertthat::assert_that(inherits(plot, "gg"),
    msg = "plot must be a ggplot object.")
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(y_frac > 0 && y_frac <= 1)

  # Default FPMRS policy-event table
  default_events <- c(
    "PubMed open access"      = 2000L,
    "FDA mesh Class III"      = 2016L,
    "FDA mesh withdrawal"     = 2019L,
    "COVID-19"                = 2020L,
    "FPMRS \u2192 URPS rename" = 2024L
  )
  event_vec <- if (is.null(events)) default_events else events

  assertthat::assert_that(
    is.numeric(event_vec),
    msg = "events must be a named numeric vector of years.")

  # Extract y-axis limits from the built plot
  built    <- ggplot2::ggplot_build(plot)
  y_range  <- built$layout$panel_params[[1L]]$y.range
  if (is.null(y_range)) y_range <- c(0, 1)
  y_label  <- y_range[[1L]] + y_frac * diff(y_range)

  for (nm in names(event_vec)) {
    yr <- event_vec[[nm]]
    plot <- plot +
      ggplot2::geom_vline(
        xintercept = yr,
        colour     = line_colour,
        linetype   = line_lty,
        linewidth  = 0.5
      ) +
      ggplot2::annotate(
        "text",
        x      = yr + 0.3,
        y      = y_label,
        label  = nm,
        hjust  = 0,
        vjust  = 0.5,
        size   = label_size,
        angle  = label_angle,
        colour = line_colour
      )
    .log_step(sprintf("[EVENTS] Marked: %s (%d)", nm, as.integer(yr)),
              verbose)
  }

  plot
}


# ============================================================
# ============================================================
#
# SECTION 16: PHASE 2 — NEW ANALYTICAL CAPABILITIES
#   #1  Parallel bootstrap CIs via furrr/future
#   #5  Joinpoint regression via segmented
#   #6  Sensitivity analysis pipeline
#   #11 Synthetic bibliography generation
#
# ============================================================
# ============================================================


# ── #1 Parallel bootstrap CIs ───────────────────────────────

#' Compute Bootstrap Confidence Intervals for Theil-Sen Slopes
#'
#' @description
#' Runs \code{n_boot} resampled Theil-Sen fits in parallel using
#' \code{furrr::future_map_dbl()} (mkiang/excess_physician_mortality
#' pattern). Returns the slope point estimate plus 2.5th/97.5th
#' percentile bootstrap CIs.
#'
#' Falls back to sequential \code{purrr::map_dbl()} when \code{furrr}
#' is not installed, so the function always produces a result.
#'
#' @param annual_trends Data frame with \code{publication_year} and
#'   \code{publication_count}.
#' @param n_boot       Integer. Bootstrap replicates (default 2000;
#'   10 000 for publication).
#' @param conf_level   Numeric. Confidence level (default 0.95).
#' @param n_workers    Integer or NULL. CPU cores. NULL = half of
#'   available cores.
#' @param seed         Integer. RNG seed passed to
#'   \code{furrr_options(seed = TRUE)}.
#' @param verbose      Logical.
#'
#' @return Named list: \code{slope} (point estimate from full data),
#'   \code{ci_lo}, \code{ci_hi}, \code{ci_level}, \code{n_boot},
#'   \code{n_valid_boots}, \code{parallel} (logical — whether furrr
#'   was used), \code{method}.
#'
#' @examples
#' set.seed(1L)
#' ann <- data.frame(
#'   publication_year  = 1990L:2020L,
#'   publication_count = as.integer(abs(rnorm(31L, 40L, 8L)))
#' )
#' res <- compute_theilsen_bootstrap_ci(ann, n_boot=200L, verbose=FALSE)
#' all(c("slope","ci_lo","ci_hi") %in% names(res))
#'
#' # CI brackets the slope
#' res$ci_lo <= res$slope && res$slope <= res$ci_hi
#'
#' # Wider CI with fewer boots — just checking structure
#' res2 <- compute_theilsen_bootstrap_ci(ann, n_boot=50L,
#'   conf_level=0.99, verbose=FALSE)
#' res2$ci_level == 0.99
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom purrr map_dbl
#' @importFrom MASS glm.nb
#' @export
compute_theilsen_bootstrap_ci <- function(
    annual_trends,
    n_boot     = 2000L,
    conf_level = 0.95,
    n_workers  = NULL,
    seed       = 42L,
    verbose    = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(
    all(c("publication_year","publication_count") %in% names(annual_trends)))
  assertthat::assert_that(assertthat::is.count(n_boot))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(conf_level > 0 && conf_level < 1)

  has_mblm <- requireNamespace("mblm", quietly = TRUE)
  if (!has_mblm) {
    .log_step(paste(
      "[BOOTSTRAP] mblm not installed; falling back to OLS (lm) for slope.",
      "Install mblm for true Theil-Sen: install.packages('mblm')"), verbose)
  }

  df <- dplyr::filter(annual_trends,
    !is.na(.data$publication_count),
    .data$publication_count >= 0L)
  n <- nrow(df)
  assertthat::assert_that(n >= 4L,
    msg = "Need >= 4 observations for Theil-Sen bootstrap.")

  # Point estimate on full data
  .fit_slope <- function(d) {
    if (has_mblm) {
      coef(suppressWarnings(
        mblm::mblm(publication_count ~ publication_year,
                   dataframe = d, repeated = FALSE)))[["publication_year"]]
    } else {
      coef(lm(publication_count ~ publication_year, data = d))[["publication_year"]]
    }
  }
  slope_est <- tryCatch(.fit_slope(df), error = function(e) NA_real_)

  alpha_tail  <- (1 - conf_level) / 2
  use_furrr   <- requireNamespace("furrr",  quietly = TRUE) &&
                 requireNamespace("future", quietly = TRUE)

  boot_one <- function(i) {
    boot_df <- df[sample.int(n, n, replace = TRUE), ]
    tryCatch(.fit_slope(boot_df), error = function(e) NA_real_)
  }

  .log_step(sprintf(
    "[BOOTSTRAP] %d replicates | parallel=%s | seed=%d",
    n_boot, use_furrr, seed), verbose)

  if (use_furrr) {
    workers <- n_workers %||% max(1L, floor(parallel::detectCores() / 2L))
    future::plan(future::multisession, workers = workers)
    on.exit(future::plan(future::sequential), add = TRUE)
    boot_slopes <- furrr::future_map_dbl(
      seq_len(n_boot), boot_one,
      .options = furrr::furrr_options(seed = seed))
  } else {
    set.seed(seed)
    boot_slopes <- purrr::map_dbl(seq_len(n_boot), boot_one)
  }

  valid_slopes <- boot_slopes[!is.na(boot_slopes)]
  n_valid      <- length(valid_slopes)

  if (n_valid < n_boot * 0.8) {
    warning(sprintf(
      "[BOOTSTRAP] Only %d/%d replicates converged (%.0f%%).",
      n_valid, n_boot, n_valid / n_boot * 100), call. = FALSE)
  }

  ci <- quantile(valid_slopes, probs = c(alpha_tail, 1 - alpha_tail),
                 na.rm = TRUE)

  .log_step(sprintf(
    "[BOOTSTRAP] slope=%.4f  %d%%CI [%.4f, %.4f]  (n_valid=%d)",
    slope_est, as.integer(conf_level * 100), ci[[1L]], ci[[2L]], n_valid),
    verbose)

  list(
    slope        = slope_est,
    ci_lo        = unname(ci[[1L]]),
    ci_hi        = unname(ci[[2L]]),
    ci_level     = conf_level,
    n_boot       = n_boot,
    n_valid_boots = n_valid,
    parallel     = use_furrr,
    method       = "Theil-Sen (mblm::mblm, repeated=FALSE)"
  )
}


#' Compute Bootstrap CIs Across All Subspecialty Strata
#'
#' @description
#' Applies \code{compute_theilsen_bootstrap_ci()} to each stratum of
#' a multi-subspecialty bibliography using \code{fit_models_per_stratum()},
#' so each subspecialty has its own fingerprinted cached CI object.
#'
#' @param bibliography Data frame with \code{subspecialty} and
#'   \code{publication_year} columns.
#' @param n_boot    Integer.
#' @param cache_dir Character.
#' @param force_rerun Logical.
#' @param verbose   Logical.
#'
#' @return Named list of \code{compute_theilsen_bootstrap_ci()} results.
#'
#' @examples
#' set.seed(1L)
#' bib <- data.frame(
#'   subspecialty = rep(c("FPMRS","REI"), each=30L),
#'   publication_year = rep(1990:2019, 2L),
#'   publication_count = as.integer(abs(rnorm(60L, 40L, 8L)))
#' )
#' res <- compute_all_strata_bootstrap_ci(bib, n_boot=50L,
#'   cache_dir=tempdir(), verbose=FALSE)
#' all(c("FPMRS","REI") %in% names(res))
#'
#' all(purrr::map_lgl(res, ~all(c("slope","ci_lo","ci_hi") %in% names(.x))))
#'
#' # CIs bracket their slopes
#' all(purrr::map_lgl(res, ~.x$ci_lo <= .x$slope && .x$slope <= .x$ci_hi))
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr filter count mutate
#' @importFrom purrr map
#' @export
compute_all_strata_bootstrap_ci <- function(
    bibliography,
    n_boot     = 2000L,
    cache_dir  = file.path("data","cache","rds","bootstrap_ci"),
    force_rerun = FALSE,
    verbose    = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(
    all(c("subspecialty","publication_year") %in% names(bibliography)))

  # Build annual trends per subspecialty then run bootstrap
  fit_models_per_stratum(
    bibliography = bibliography,
    strat_col    = "subspecialty",
    fit_fn       = function(sp_bib) {
      ann <- dplyr::count(sp_bib, .data$publication_year,
                          name = "publication_count") |>
        dplyr::mutate(publication_count =
                        as.integer(.data$publication_count))
      compute_theilsen_bootstrap_ci(ann, n_boot = n_boot,
                                     verbose = FALSE)
    },
    model_type   = "theilsen_bootstrap",
    cache_dir    = cache_dir,
    force_rerun  = force_rerun,
    verbose      = verbose
  )
}


# ── #5 Joinpoint regression ──────────────────────────────────

#' Fit Joinpoint Regression to Detect Slope Change-Points
#'
#' @description
#' Wraps \code{segmented::segmented()} to detect where publication
#' trend slopes change — directly addressing the joinpoint gap
#' identified from mkiang/dynamic_inequality. Up to \code{max_breakpoints}
#' joinpoints are tested; the model with lowest BIC is returned.
#'
#' @param annual_trends Data frame with \code{publication_year} and
#'   \code{publication_count}.
#' @param max_breakpoints Integer. Maximum joinpoints to test (default 3).
#' @param year_start  Integer. Used to scale the year predictor.
#' @param verbose     Logical.
#'
#' @return Named list: \code{breakpoints} (year(s) of detected slope
#'   change), \code{slopes} (Theil-Sen slope per segment),
#'   \code{model} (the selected \code{segmented} object),
#'   \code{n_breakpoints}, \code{bic_comparison} (tibble),
#'   \code{figure} (ggplot).
#'
#' @examples
#' set.seed(1L)
#' # Simulate a series with one inflection around 2005
#' ann <- data.frame(
#'   publication_year  = 1990L:2020L,
#'   publication_count = as.integer(c(
#'     seq(5, 30, length.out=16),
#'     seq(32, 80, length.out=15)
#'   ) + rnorm(31, 0, 3))
#' )
#' res <- fit_joinpoint_regression(ann, max_breakpoints=2L, verbose=FALSE)
#' is.list(res) && "breakpoints" %in% names(res)
#'
#' # Returns at least one segment slope
#' length(res$slopes) >= 1L
#'
#' # Figure is a ggplot
#' inherits(res$figure, "gg")
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr tibble bind_rows arrange mutate
#' @importFrom ggplot2 ggplot aes geom_point geom_line geom_vline
#'   labs theme_bw
#' @export
fit_joinpoint_regression <- function(
    annual_trends,
    max_breakpoints = 3L,
    year_start      = 1975L,
    verbose         = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(
    all(c("publication_year","publication_count") %in% names(annual_trends)))
  assertthat::assert_that(assertthat::is.count(max_breakpoints))
  assertthat::assert_that(assertthat::is.flag(verbose))

  if (!requireNamespace("segmented", quietly = TRUE)) {
    .log_step(paste(
      "[JOINPOINT] 'segmented' package not installed.",
      "Install: install.packages('segmented').",
      "Returning single-segment linear model."), verbose)
    lm_only <- tryCatch(
      lm(publication_count ~ publication_year, data =
         dplyr::filter(annual_trends, !is.na(.data$publication_count),
                       .data$publication_count >= 0L)),
      error = function(e) NULL)
    return(list(breakpoints = integer(0L),
                slopes = if (!is.null(lm_only)) unname(coef(lm_only)[2L]) else NA_real_,
                model  = lm_only, n_breakpoints = 0L,
                bic_comparison = dplyr::tibble(n_breakpoints=0L, BIC=if(!is.null(lm_only)) BIC(lm_only) else NA_real_, selected=TRUE),
                figure = ggplot2::ggplot() + ggplot2::labs(title="segmented not installed")))
  }

  df <- dplyr::filter(annual_trends,
    !is.na(.data$publication_count),
    .data$publication_count >= 0L) |>
    dplyr::arrange(.data$publication_year) |>
    dplyr::mutate(yr_s = (.data$publication_year - year_start) / 10)

  assertthat::assert_that(nrow(df) >= 6L,
    msg = "Need >= 6 observations for joinpoint regression.")

  # Base linear model for segmented()
  lm_base <- lm(publication_count ~ yr_s, data = df)

  best_model  <- lm_base
  best_bic    <- BIC(lm_base)
  best_npsi   <- 0L
  bic_rows    <- list(dplyr::tibble(n_breakpoints = 0L,
                                     BIC = round(best_bic, 2L),
                                     selected = TRUE))

  for (npsi in seq_len(min(max_breakpoints, floor(nrow(df) / 4L)))) {
    fit <- tryCatch(
      segmented::segmented(lm_base, seg.Z = ~yr_s, npsi = npsi),
      error   = function(e) NULL,
      warning = function(w) tryCatch(
        segmented::segmented(lm_base, seg.Z = ~yr_s, npsi = npsi),
        error = function(e2) NULL)
    )
    if (is.null(fit)) next

    b <- tryCatch(BIC(fit), error = function(e) Inf)
    bic_rows[[length(bic_rows) + 1L]] <- dplyr::tibble(
      n_breakpoints = npsi,
      BIC           = round(b, 2L),
      selected      = FALSE)

    if (b < best_bic - 2) {   # require at least 2 BIC units improvement
      best_bic    <- b
      best_model  <- fit
      best_npsi   <- npsi
    }
  }

  bic_tbl <- dplyr::bind_rows(bic_rows) |>
    dplyr::mutate(selected = .data$n_breakpoints == best_npsi)

  # Extract breakpoints (back-transform from yr_s to year)
  breakpoint_years <- if (best_npsi > 0L) {
    bps <- tryCatch(
      segmented::confint.segmented(best_model)[, "Est."],
      error = function(e) best_model$psi[, "Est."]
    )
    as.integer(round(bps * 10 + year_start))
  } else integer(0L)

  # Segment slopes via slope() accessor
  seg_slopes <- tryCatch({
    sl <- segmented::slope(best_model)
    unname(sl$yr_s[, "Est."])
  }, error = function(e) {
    unname(coef(best_model)[["yr_s"]])
  })

  .log_step(sprintf(
    "[JOINPOINT] %d breakpoint(s) selected (BIC=%.1f). Breakpoints: %s",
    best_npsi,
    best_bic,
    if (length(breakpoint_years) == 0L) "none"
    else paste(breakpoint_years, collapse = ", ")
  ), verbose)

  # Figure
  pred_df <- dplyr::mutate(df,
    fitted = predict(best_model, newdata = df))

  fig <- ggplot2::ggplot(pred_df,
    ggplot2::aes(x = .data$publication_year)) +
    ggplot2::geom_point(ggplot2::aes(y = .data$publication_count),
                        colour = "grey50", size = 1.5, alpha = 0.7) +
    ggplot2::geom_line(ggplot2::aes(y = .data$fitted),
                       colour = "#2c7bb6", linewidth = 1.1) +
    ggplot2::labs(title = "Joinpoint Regression",
                  subtitle = sprintf("%d breakpoint(s) | BIC=%.1f",
                                     best_npsi, best_bic),
                  x = NULL, y = "Annual publications") +
    ggplot2::theme_bw(base_size = 11)

  if (length(breakpoint_years) > 0L) {
    fig <- fig +
      ggplot2::geom_vline(
        xintercept = breakpoint_years,
        colour = "#e31a1c", linetype = 2L, linewidth = 0.6)
  }

  list(
    breakpoints    = breakpoint_years,
    slopes         = seg_slopes,
    model          = best_model,
    n_breakpoints  = best_npsi,
    bic_comparison = bic_tbl,
    figure         = fig
  )
}


# ── #6 Sensitivity analysis pipeline ────────────────────────

#' Run a Sensitivity Analysis Across Alternative Model Specifications
#'
#' @description
#' Re-runs the core count-model analysis under a set of alternative
#' specifications and returns a comparison table — implementing the
#' carlesmila/LCDE2024_wildfires \code{6_HIA_sens.R} pattern.
#'
#' Built-in sensitivity dimensions:
#' \enumerate{
#'   \item Distribution family: Poisson / NB / ZINB
#'   \item Year range: full range / post-2000 only / post-2010 only
#'   \item Predictor scaling: raw year / decade-scaled year
#' }
#'
#' @param annual_trends Data frame with \code{publication_year} and
#'   \code{publication_count}.
#' @param year_start Integer.
#' @param specs Named list of alternative specifications. Each element
#'   is a list with \code{family} (\code{"nb"}, \code{"poisson"}, or
#'   \code{"zinb"}), \code{year_min} (integer or NULL), and optionally
#'   \code{label} (character). If NULL, uses the built-in grid.
#' @param verbose Logical.
#'
#' @return A tibble with columns \code{spec_label}, \code{family},
#'   \code{year_min}, \code{n_obs}, \code{slope_per_decade},
#'   \code{slope_se}, \code{AIC}, and \code{converged}.
#'
#' @examples
#' set.seed(1L)
#' ann <- data.frame(
#'   publication_year  = 1990L:2020L,
#'   publication_count = as.integer(abs(rnorm(31L, 50L, 12L)))
#' )
#' sens <- run_model_sensitivity(ann, verbose = FALSE)
#' is.data.frame(sens) && nrow(sens) >= 3L
#'
#' all(c("spec_label","slope_per_decade","AIC") %in% names(sens))
#'
#' # Custom specs
#' custom <- list(
#'   s1 = list(family="nb",      year_min=NULL, label="NB full"),
#'   s2 = list(family="poisson", year_min=2000L, label="Poisson post-2000")
#' )
#' sens2 <- run_model_sensitivity(ann, specs=custom, verbose=FALSE)
#' nrow(sens2) == 2L
#'
#' @importFrom assertthat assert_that is.flag
#' @importFrom dplyr tibble bind_rows filter mutate
#' @importFrom purrr imap_dfr
#' @importFrom MASS glm.nb
#' @export
run_model_sensitivity <- function(
    annual_trends,
    year_start = 1975L,
    specs      = NULL,
    verbose    = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(assertthat::is.flag(verbose))

  # Built-in sensitivity grid
  default_specs <- list(
    nb_full      = list(family="nb",      year_min=NULL,  label="NB (full range)"),
    nb_post2000  = list(family="nb",      year_min=2000L, label="NB (2000+)"),
    nb_post2010  = list(family="nb",      year_min=2010L, label="NB (2010+)"),
    pois_full    = list(family="poisson", year_min=NULL,  label="Poisson (full)"),
    pois_post2000= list(family="poisson", year_min=2000L, label="Poisson (2000+)")
  )
  spec_list <- specs %||% default_specs

  .log_step(sprintf("[SENSITIVITY] Running %d specifications ...",
                    length(spec_list)), verbose)

  purrr::imap_dfr(spec_list, function(sp, nm) {
    df <- dplyr::filter(annual_trends,
      !is.na(.data$publication_count),
      .data$publication_count >= 0L)
    if (!is.null(sp$year_min))
      df <- dplyr::filter(df, .data$publication_year >= sp$year_min)
    if (nrow(df) < 4L) return(NULL)

    df <- dplyr::mutate(df,
      yr_s = (.data$publication_year - year_start) / 10)
    label <- sp$label %||% nm

    result <- tryCatch({
      if (sp$family == "nb") {
        fit <- suppressWarnings(MASS::glm.nb(publication_count~yr_s, data=df))
        list(slope = coef(fit)[["yr_s"]],
             se    = sqrt(diag(vcov(fit)))[["yr_s"]],
             aic   = AIC(fit), conv = fit$converged)

      } else if (sp$family == "zinb" &&
                 requireNamespace("pscl", quietly=TRUE) &&
                 sum(df$publication_count==0L) >= 1L) {
        fit <- pscl::zeroinfl(
          publication_count ~ yr_s | yr_s, data=df, dist="negbin")
        list(slope = coef(fit, model="count")[["yr_s"]],
             se    = sqrt(diag(vcov(fit)))[["yr_s"]],
             aic   = AIC(fit), conv = TRUE)

      } else {
        fit <- glm(publication_count ~ yr_s,
                   data=df, family=poisson(link="log"))
        list(slope = coef(fit)[["yr_s"]],
             se    = sqrt(diag(vcov(fit)))[["yr_s"]],
             aic   = AIC(fit), conv = fit$converged)
      }
    }, error = function(e) {
      list(slope=NA_real_, se=NA_real_, aic=NA_real_, conv=FALSE)
    })

    dplyr::tibble(
      spec_label       = label,
      family           = sp$family,
      year_min         = sp$year_min %||% NA_integer_,
      n_obs            = nrow(df),
      slope_per_decade = round(result$slope, 5L),
      slope_se         = round(result$se, 5L),
      AIC              = round(result$aic, 2L),
      converged        = result$conv
    )
  })
}


# ── #11 Synthetic bibliography generation ───────────────────

#' Generate a Synthetic PubMed-Format Bibliography for Testing
#'
#' @description
#' Creates a fake bibliography tibble with the column names and
#' data types that the pipeline expects from a real PubMed or
#' Web of Science export — implementing the
#' alxsrobert/measles_england_sir synthetic data pattern.
#'
#' The resulting tibble can be used to test every pipeline function
#' without requiring API access or real patient/publication data,
#' making smoke tests fully offline.
#'
#' @param n_papers      Integer. Total number of papers.
#' @param n_subspecialties Integer. Number of specialties.
#' @param year_start    Integer.
#' @param year_end      Integer.
#' @param pct_female    Numeric (0–1). Target first-author female
#'   proportion (with ±10\% random noise per year).
#' @param mean_citations Numeric. Mean citations per paper (NB-distributed).
#' @param seed          Integer.
#' @param verbose       Logical.
#'
#' @return A tibble with columns matching the pipeline's expected
#'   bibliography format: \code{UT}, \code{publication_year},
#'   \code{TI}, \code{AB}, \code{AU}, \code{AF}, \code{SO},
#'   \code{TC}, \code{PY}, \code{DE}, \code{LA}, \code{DT},
#'   \code{subspecialty}.
#'
#' @examples
#' bib <- generate_synthetic_bibliography(n_papers=100L, seed=1L,
#'   verbose=FALSE)
#' nrow(bib) == 100L
#'
#' # All required columns present
#' req <- c("UT","publication_year","TI","AF","TC","subspecialty")
#' all(req %in% names(bib))
#'
#' # Years in range
#' all(bib$publication_year >= 1990L & bib$publication_year <= 2024L)
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom tibble tibble
#' @importFrom stats rnbinom sample runif
#' @export
generate_synthetic_bibliography <- function(
    n_papers         = 500L,
    n_subspecialties = 4L,
    year_start       = 1990L,
    year_end         = 2024L,
    pct_female       = 0.45,
    mean_citations   = 25L,
    seed             = 42L,
    verbose          = TRUE
) {
  assertthat::assert_that(assertthat::is.count(n_papers))
  assertthat::assert_that(assertthat::is.flag(verbose))
  assertthat::assert_that(pct_female >= 0 && pct_female <= 1)

  set.seed(seed)

  sp_names <- c("FPMRS","REI","MFM","GynOnc","MIGS","CFP","PAG","Urology")
  sp_use   <- sp_names[seq_len(min(n_subspecialties, length(sp_names)))]

  female_names <- c("Jennifer","Sarah","Emily","Lisa","Michelle","Karen",
                    "Jessica","Ashley","Amanda","Lauren","Samantha","Rachel",
                    "Stephanie","Amy","Jane","Anna","Maria","Elena","Claire")
  male_names   <- c("Michael","Christopher","Matthew","Joshua","Daniel",
                    "David","Andrew","Ryan","John","Robert","Thomas","James",
                    "Charles","William","Joseph","Mark","Steven","Brian")
  journals <- c("Am J Obstet Gynecol","Int Urogynecol J","Obstet Gynecol",
                "J Urol","Female Pelvic Med Reconstr Surg","Neurourol Urodyn")
  kw_pool  <- c("pelvic floor","urinary incontinence","pelvic organ prolapse",
                "sacrocolpopexy","suburethral sling","stress incontinence",
                "overactive bladder","fecal incontinence","urogynecology","FPMRS")

  pubs_per_yr <- round(n_papers / (year_end - year_start + 1L))

  rows <- lapply(seq(year_start, year_end), function(yr) {
    n_yr <- rpois(1L, pubs_per_yr)
    if (n_yr == 0L) return(NULL)
    pct_f_yr <- pmin(1, pmax(0, pct_female + runif(1L, -0.1, 0.1)))
    is_female <- runif(n_yr) < pct_f_yr
    first_nm  <- ifelse(is_female,
      sample(female_names, n_yr, replace=TRUE),
      sample(male_names,   n_yr, replace=TRUE))
    last_nm   <- paste0(sample(c("Smith","Jones","Lee","Chen","Patel",
      "Wang","Kim","Garcia","Brown","Wilson"), n_yr, replace=TRUE))
    af_str    <- paste0(last_nm, " ", first_nm, " A")

    tibble::tibble(
      UT               = paste0("SYN", yr, "_", seq_len(n_yr)),
      publication_year = yr,
      TI               = paste("Study on", sample(kw_pool, n_yr, replace=TRUE)),
      AB               = paste("Background. Methods. Results. Conclusions."),
      AU               = paste0(toupper(last_nm), " ", toupper(substr(first_nm,1L,1L))),
      AF               = af_str,
      SO               = sample(journals, n_yr, replace=TRUE),
      TC               = as.character(rnbinom(n_yr, mu=mean_citations, size=2L)),
      PY               = paste0(yr, "-06"),
      DE               = sample(kw_pool, n_yr, replace=TRUE),
      LA               = "English",
      DT               = "Article",
      subspecialty     = sample(sp_use, n_yr, replace=TRUE)
    )
  })

  bib <- dplyr::bind_rows(rows[!sapply(rows, is.null)])
  if (nrow(bib) > n_papers) bib <- bib[seq_len(n_papers), ]

  .log_step(sprintf(
    "[SYNTH] Generated %d synthetic papers (%d-%d, %d subspecialties)",
    nrow(bib), year_start, year_end, n_subspecialties), verbose)

  bib
}


# ============================================================
# ============================================================
#
# SECTION 17: PHASE 3 — HIGH-EFFORT ANALYTICAL ADDITIONS
#   #2  Bayesian NB via brms with LOO-CV
#   #4  Difference-in-differences for policy events
#   #7  Survival: "time to adoption" of surgical techniques
#   #12 Simulation-based calibration (SBC) for model validation
#
# ============================================================
# ============================================================


# ── #2 Bayesian NB via brms ──────────────────────────────────

#' Fit a Bayesian Negative Binomial Model via brms
#'
#' @description
#' Wraps \code{brms::brm()} to fit a Bayesian negative binomial
#' count regression, following the cas-bioinf/covid19retrospective
#' \code{analysis_brms.Rmd} pattern. Adds LOO-CV for model
#' comparison and posterior predictive checks.
#'
#' @param annual_trends  Data frame with \code{publication_year} and
#'   \code{publication_count}. May include a \code{subspecialty}
#'   column for mixed-effects models.
#' @param formula_str    Character. brms formula string. Default fits
#'   a linear year trend with random subspecialty intercept when the
#'   column is present.
#' @param prior_b        Numeric. SD for Normal(0, prior_b) prior on
#'   regression coefficients (default 5 = weakly informative).
#' @param n_chains       Integer. MCMC chains (default 4).
#' @param n_iter         Integer. Iterations per chain (default 2000).
#' @param add_loo        Logical. Compute LOO-CV (adds ~30s).
#' @param add_pp_check   Logical. Draw posterior predictive check.
#' @param seed           Integer.
#' @param verbose        Logical.
#'
#' @return Named list: \code{model} (brmsfit), \code{loo} (if
#'   \code{add_loo}), \code{pp_check_fig} (ggplot, if
#'   \code{add_pp_check}), \code{summary_tbl}, \code{formula_used}.
#'
#' @examples
#' \dontrun{
#' # Requires brms + CmdStan or rstan
#' ann <- data.frame(
#'   publication_year  = 1990L:2020L,
#'   publication_count = as.integer(abs(rnorm(31L, 40L, 8L)))
#' )
#' res <- fit_bayesian_nb(ann, n_chains=2L, n_iter=500L,
#'   add_loo=FALSE, verbose=FALSE)
#' inherits(res$model, "brmsfit")
#' }
#'
#' @importFrom assertthat assert_that is.flag is.count
#' @export
fit_bayesian_nb <- function(
    annual_trends,
    formula_str  = NULL,
    prior_b      = 5,
    n_chains     = 4L,
    n_iter       = 2000L,
    add_loo      = TRUE,
    add_pp_check = TRUE,
    seed         = 42L,
    verbose      = TRUE
) {
  assertthat::assert_that(is.data.frame(annual_trends))
  assertthat::assert_that(assertthat::is.flag(verbose))

  if (!requireNamespace("brms", quietly = TRUE))
    stop("Package 'brms' required. Install: install.packages('brms')")

  df <- dplyr::filter(annual_trends,
    !is.na(.data$publication_count),
    .data$publication_count >= 0L) |>
    dplyr::mutate(yr_s = (.data$publication_year - 1975L) / 10)

  has_sp <- "subspecialty" %in% names(df) &&
            dplyr::n_distinct(df$subspecialty, na.rm=TRUE) > 1L

  fml_str <- formula_str %||% (
    if (has_sp) "publication_count ~ yr_s + (1 | subspecialty)"
    else        "publication_count ~ yr_s"
  )
  fml <- stats::as.formula(fml_str)

  priors <- brms::prior(normal(0, prior_b), class = "b")

  .log_step(sprintf(
    "[BAYES NB] Fitting: %s | chains=%d | iter=%d | seed=%d",
    fml_str, n_chains, n_iter, seed), verbose)

  model <- suppressMessages(
    brms::brm(
      formula = brms::bf(fml, family = brms::negbinomial()),
      data    = df,
      prior   = priors,
      chains  = n_chains,
      iter    = n_iter,
      seed    = seed,
      silent  = if (verbose) 0L else 2L
    )
  )

  # Summary table
  summary_tbl <- brms::fixef(model) |>
    as.data.frame() |>
    tibble::rownames_to_column("parameter") |>
    tibble::as_tibble() |>
    dplyr::mutate(dplyr::across(where(is.numeric), ~round(.x, 4L)))

  result <- list(model = model, summary_tbl = summary_tbl,
                 formula_used = fml_str)

  # LOO-CV
  if (add_loo) {
    .log_step("[BAYES NB] Computing LOO-CV ...", verbose)
    result$loo <- tryCatch(
      brms::add_criterion(model, "loo")$criteria$loo,
      error = function(e) {
        .log_step(sprintf("[BAYES NB] LOO failed: %s", e$message), TRUE)
        NULL
      }
    )
  }

  # Posterior predictive check
  if (add_pp_check) {
    result$pp_check_fig <- tryCatch(
      brms::pp_check(model, ndraws = 100L),
      error = function(e) NULL
    )
  }

  .log_step(sprintf(
    "[BAYES NB] Done. Rhat max = %.3f",
    max(brms::rhat(model), na.rm = TRUE)), verbose)

  result
}


# ── #4 Difference-in-differences ────────────────────────────

#' Estimate Policy-Event Effect via Difference-in-Differences
#'
#' @description
#' Implements the markowskijustin/APMs staggered DiD pattern for
#' evaluating the causal effect of a policy event on publication
#' counts. The canonical FPMRS application is the 2019 FDA mesh
#' withdrawal: FPMRS is the treatment group; other subspecialties
#' serve as controls.
#'
#' Uses \code{fixest::feols()} with two-way fixed effects (subspecialty
#' + year) and clustered standard errors. Event-study coefficients
#' give the dynamic treatment effect at each relative year.
#'
#' @param panel_data       Data frame in panel format (one row per
#'   subspecialty × year) with columns: \code{subspecialty},
#'   \code{publication_year}, \code{publication_count},
#'   \code{treated} (integer 0/1 — is this subspecialty treated?).
#' @param treatment_year   Integer. Year the policy event occurred.
#' @param pre_window       Integer. Years of pre-period to include
#'   (default 5).
#' @param post_window      Integer. Years of post-period (default 5).
#' @param cluster_var      Character. Variable to cluster SEs on
#'   (default \code{"subspecialty"}).
#' @param verbose          Logical.
#'
#' @return Named list: \code{att} (average treatment effect on the
#'   treated, post-event), \code{event_study} (tibble of event-study
#'   coefficients), \code{figure} (event-study ggplot with 95\% CI),
#'   \code{model} (the feols fit), \code{parallel_trends_test}
#'   (pre-period F-test p-value).
#'
#' @examples
#' set.seed(1L)
#' panel <- data.frame(
#'   subspecialty     = rep(c("FPMRS","REI","MFM"), each=20L),
#'   publication_year = rep(2009:2028, 3L),
#'   publication_count = c(
#'     c(seq(10,20,length.out=10), seq(12,18,length.out=10)),
#'     rep(seq(30,50,length.out=20), 2L)
#'   ) + rnorm(60L, 0, 2),
#'   treated = c(rep(1L,20L), rep(0L,40L))
#' )
#' res <- estimate_policy_effect_did(panel,
#'   treatment_year=2019L, pre_window=5L, post_window=4L,
#'   verbose=FALSE)
#' all(c("att","event_study","figure") %in% names(res))
#'
#' is.numeric(res$att)
#' inherits(res$figure, "gg")
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr filter mutate between tibble bind_rows
#' @importFrom ggplot2 ggplot aes geom_point geom_errorbar geom_hline
#'   geom_vline labs theme_bw
#' @export
estimate_policy_effect_did <- function(
    panel_data,
    treatment_year = 2019L,
    pre_window     = 5L,
    post_window    = 5L,
    cluster_var    = "subspecialty",
    verbose        = TRUE
) {
  assertthat::assert_that(is.data.frame(panel_data))
  assertthat::assert_that(
    all(c("subspecialty","publication_year","publication_count","treated") %in%
        names(panel_data)))
  assertthat::assert_that(assertthat::is.flag(verbose))

  if (!requireNamespace("fixest", quietly = TRUE))
    stop("Package 'fixest' required. Install: install.packages('fixest')")

  df <- panel_data |>
    dplyr::filter(
      dplyr::between(.data$publication_year,
                     treatment_year - pre_window,
                     treatment_year + post_window)) |>
    dplyr::mutate(
      rel_year     = .data$publication_year - treatment_year,
      post         = as.integer(.data$publication_year >= treatment_year),
      did_term     = .data$treated * .data$post,
      # Omit rel_year = -1 as reference period (standard in event studies)
      rel_year_fac = relevel(factor(.data$rel_year), ref = "-1")
    )

  # Event study: interaction of treated × rel_year dummies
  fml_es <- fixest::as.formula(paste0(
    "publication_count ~ treated:rel_year_fac | ",
    "subspecialty + publication_year"
  ))
  model_es <- tryCatch(
    fixest::feols(fml_es, data = df,
                  cluster = cluster_var, nthreads = 1L),
    error = function(e) {
      .log_step(sprintf("[DiD] Event study failed: %s", e$message), TRUE)
      NULL
    }
  )

  # Simple 2×2 DiD for ATT
  fml_att <- fixest::as.formula(paste0(
    "publication_count ~ did_term | ",
    "subspecialty + publication_year"
  ))
  model_att <- tryCatch(
    fixest::feols(fml_att, data = df,
                  cluster = cluster_var, nthreads = 1L),
    error = function(e) NULL
  )

  att_est <- if (!is.null(model_att)) {
    c <- coef(model_att)
    c[["did_term"]]
  } else NA_real_

  # Extract event study coefficients
  es_tbl <- if (!is.null(model_es)) {
    cs <- coef(model_es)
    se <- sqrt(diag(vcov(model_es)))
    idx <- grepl("rel_year_fac", names(cs))
    rel_yrs <- as.integer(
      stringr::str_extract(names(cs)[idx], "-?[0-9]+$"))
    # Add omitted reference year (coef=0)
    dplyr::bind_rows(
      dplyr::tibble(
        rel_year = rel_yrs,
        coef     = cs[idx],
        se       = se[idx],
        ci_lo    = cs[idx] - 1.96 * se[idx],
        ci_hi    = cs[idx] + 1.96 * se[idx]
      ),
      dplyr::tibble(rel_year=-1L, coef=0, se=0, ci_lo=0, ci_hi=0)
    ) |> dplyr::arrange(.data$rel_year)
  } else {
    dplyr::tibble(rel_year=integer(), coef=numeric(),
                   se=numeric(), ci_lo=numeric(), ci_hi=numeric())
  }

  # Pre-period parallel trends test (joint F-test, pre-period only)
  pre_coefs <- es_tbl |> dplyr::filter(.data$rel_year < -1L)
  pt_pval   <- if (nrow(pre_coefs) >= 2L && !is.null(model_es)) {
    tryCatch({
      pre_terms <- paste0("treated:rel_year_fac", pre_coefs$rel_year)
      pre_terms <- intersect(pre_terms, names(coef(model_es)))
      if (length(pre_terms) >= 1L) {
        fixest::wald(model_es, pre_terms)$p
      } else NA_real_
    }, error = function(e) NA_real_)
  } else NA_real_

  .log_step(sprintf(
    "[DiD] ATT = %.3f | Pre-trend p = %s",
    att_est,
    if (is.na(pt_pval)) "N/A" else sprintf("%.4f", pt_pval)), verbose)

  # Event study plot
  fig <- ggplot2::ggplot(es_tbl,
    ggplot2::aes(x = .data$rel_year, y = .data$coef)) +
    ggplot2::geom_hline(yintercept = 0, colour = "grey70") +
    ggplot2::geom_vline(xintercept = -0.5, colour = "#e31a1c",
                        linetype = 2L, linewidth = 0.6) +
    ggplot2::geom_errorbar(
      ggplot2::aes(ymin = .data$ci_lo, ymax = .data$ci_hi),
      width = 0.3, colour = "#2c7bb6") +
    ggplot2::geom_point(size = 2.5, colour = "#2c7bb6") +
    ggplot2::labs(
      title    = "Event Study: Policy Effect on Publication Counts",
      subtitle = sprintf(
        "ATT = %.3f | Treatment year = %d | Pre-trend p = %s",
        att_est, treatment_year,
        if (is.na(pt_pval)) "N/A" else sprintf("%.4f", pt_pval)),
      x = "Years relative to policy event",
      y = "Estimated effect on publication count"
    ) +
    ggplot2::theme_bw(base_size = 11)

  list(
    att                    = att_est,
    event_study            = es_tbl,
    figure                 = fig,
    model                  = model_es,
    parallel_trends_test   = pt_pval
  )
}


# ── #12 Simulation-based calibration ────────────────────────

#' Simulation-Based Calibration for Count Model Validation
#'
#' @description
#' Implements the cas-bioinf/covid19retrospective \code{R/sbc.R}
#' pattern: generates \code{n_sim} datasets from known negative
#' binomial parameters, fits the pipeline's NB GLM to each, and
#' checks whether the 95\% CIs cover the true slope at the nominal
#' rate. Coverage < 90\% signals model mis-specification; < 80\%
#' is a critical failure.
#'
#' @param n_sim         Integer. Simulations (default 500).
#' @param true_slope    Numeric. Ground-truth log-scale annual slope.
#' @param true_intercept Numeric. Log expected count at year 1.
#' @param true_theta    Numeric. NB dispersion parameter.
#' @param n_years       Integer. Length of each synthetic series.
#' @param year_start    Integer.
#' @param conf_level    Numeric. Target CI coverage.
#' @param seed          Integer.
#' @param verbose       Logical.
#'
#' @return Named list: \code{summary} (tibble with coverage,
#'   bias, RMSE, median width), \code{all_results}, \code{figure}
#'   (rank plot), \code{coverage_flag} (\code{"OK"},
#'   \code{"WARNING"}, or \code{"CRITICAL"}).
#'
#' @examples
#' res <- run_sbc_validation(n_sim=50L, true_slope=0.05,
#'   n_years=25L, seed=1L, verbose=FALSE)
#' "summary" %in% names(res)
#'
#' all(c("coverage","bias","rmse") %in% names(res$summary))
#'
#' res$coverage_flag %in% c("OK","WARNING","CRITICAL")
#'
#' @importFrom assertthat assert_that is.count is.flag
#' @importFrom dplyr tibble bind_rows mutate summarise
#' @importFrom purrr map_dfr
#' @importFrom ggplot2 ggplot aes geom_histogram geom_vline
#'   labs theme_bw
#' @export
run_sbc_validation <- function(
    n_sim          = 500L,
    true_slope     = 0.05,
    true_intercept = 3.0,
    true_theta     = 15,
    n_years        = 30L,
    year_start     = 1990L,
    conf_level     = 0.95,
    seed           = 42L,
    verbose        = TRUE
) {
  assertthat::assert_that(assertthat::is.count(n_sim))
  assertthat::assert_that(assertthat::is.flag(verbose))

  .log_step(sprintf(
    "[SBC] Running %d simulations (slope=%.3f, theta=%.0f, n=%d) ...",
    n_sim, true_slope, true_theta, n_years), verbose)

  set.seed(seed)
  seeds_vec <- sample.int(1e6L, n_sim)

  run_one <- function(idx) {
    df <- simulate_annual_counts(
      n_years       = n_years,
      year_start    = year_start,
      true_slope    = true_slope,
      true_theta    = true_theta,
      true_intercept = true_intercept,
      seed          = seeds_vec[[idx]]
    )

    fit <- tryCatch(
      suppressWarnings(MASS::glm.nb(
        publication_count ~ publication_year, data = df)),
      error = function(e) NULL
    )
    if (is.null(fit) || !fit$converged) {
      return(dplyr::tibble(sim=idx, slope_est=NA_real_,
                            ci_lo=NA_real_, ci_hi=NA_real_,
                            covered=NA, width=NA_real_, converged=FALSE))
    }

    ci <- tryCatch(suppressMessages(
      confint(fit, "publication_year", level = conf_level)),
      error = function(e) c(NA_real_, NA_real_))

    dplyr::tibble(
      sim       = idx,
      slope_est = coef(fit)[["publication_year"]],
      ci_lo     = ci[[1L]],
      ci_hi     = ci[[2L]],
      covered   = !is.na(ci[[1L]]) && ci[[1L]] <= true_slope && ci[[2L]] >= true_slope,
      width     = ci[[2L]] - ci[[1L]],
      converged = TRUE
    )
  }

  all_results <- dplyr::bind_rows(purrr::map_dfr(seq_len(n_sim), run_one))
  valid_rows  <- dplyr::filter(all_results, .data$converged,
                                !is.na(.data$covered))

  coverage    <- mean(valid_rows$covered, na.rm = TRUE)
  bias        <- mean(valid_rows$slope_est - true_slope, na.rm = TRUE)
  rmse        <- sqrt(mean((valid_rows$slope_est - true_slope)^2, na.rm=TRUE))
  med_width   <- median(valid_rows$width, na.rm = TRUE)
  n_converged <- sum(all_results$converged)

  coverage_flag <- dplyr::case_when(
    coverage >= 0.90 ~ "OK",
    coverage >= 0.80 ~ "WARNING",
    TRUE             ~ "CRITICAL"
  )

  summary_tbl <- dplyr::tibble(
    n_sim        = n_sim,
    n_converged  = n_converged,
    coverage     = round(coverage, 3L),
    target_coverage = conf_level,
    coverage_flag = coverage_flag,
    bias         = round(bias, 6L),
    rmse         = round(rmse, 6L),
    median_ci_width = round(med_width, 6L)
  )

  if (verbose) {
    .log_step(sprintf(
      "[SBC] Coverage=%.1f%% (target %.0f%%) | Bias=%.5f | RMSE=%.5f | Flag=%s",
      coverage*100, conf_level*100, bias, rmse, coverage_flag), TRUE)
    if (coverage_flag != "OK")
      warning(sprintf(
        "[SBC] Coverage flag: %s (%.1f%% vs %.0f%% target). Check model specification.",
        coverage_flag, coverage*100, conf_level*100), call.=FALSE)
  }

  # Rank plot (SBC diagnostic: ranks should be uniform under correct model)
  all_results2 <- dplyr::filter(all_results, .data$converged) |>
    dplyr::mutate(
      rank = rank(.data$slope_est) / max(rank(.data$slope_est)),
      covered_lab = ifelse(.data$covered, "CI covers true", "CI misses true")
    )

  fig <- ggplot2::ggplot(all_results2,
    ggplot2::aes(x = .data$slope_est)) +
    ggplot2::geom_histogram(bins = 40L, fill = "#2c7bb6",
                             colour = "white", alpha = 0.8) +
    ggplot2::geom_vline(xintercept = true_slope,
                        colour = "#e31a1c", linetype = 2L) +
    ggplot2::labs(
      title    = sprintf("SBC: Slope estimate distribution (n=%d sims)", n_sim),
      subtitle = sprintf(
        "True slope=%.4f | Coverage=%.1f%% | Bias=%.5f | Flag=%s",
        true_slope, coverage*100, bias, coverage_flag),
      x = "Estimated slope",
      y = "Count"
    ) +
    ggplot2::theme_bw(base_size = 11)

  list(
    summary        = summary_tbl,
    all_results    = all_results,
    figure         = fig,
    coverage_flag  = coverage_flag
  )
}


# ── #7 Survival: time-to-adoption ───────────────────────────

#' Model Time-to-Adoption of a Surgical Technique as a Survival Outcome
#'
#' @description
#' Frames the "when did this subspecialty start publishing on technique X?"
#' question as a survival analysis, following the
#' cas-bioinf/covid19retrospective competing-risks framework.
#'
#' For each subspecialty, the "event" is the year in which publications
#' mentioning \code{technique_pattern} first exceeded
#' \code{min_papers_threshold}. Subspecialties that never reached the
#' threshold are right-censored at \code{year_end}.
#'
#' @param bibliography         Data frame with \code{subspecialty},
#'   \code{publication_year}, and a searchable text column
#'   (\code{TI} or \code{AB}).
#' @param technique_pattern    Character. Regex to detect the technique
#'   in \code{TI} + \code{AB} columns.
#' @param min_papers_threshold Integer. Minimum papers per year to
#'   constitute "adoption" (default 3).
#' @param year_end             Integer. Censoring year.
#' @param verbose              Logical.
#'
#' @return Named list: \code{adoption_data} (per-subspecialty tibble
#'   with \code{time}, \code{event}, \code{adoption_year});
#'   \code{km_fit} (survfit object); \code{figure} (K-M curve ggplot).
#'
#' @examples
#' set.seed(1L)
#' bib <- generate_synthetic_bibliography(n_papers=300L, verbose=FALSE)
#' bib$TI <- paste(bib$TI, sample(c("sacrocolpopexy","other"),
#'   nrow(bib), replace=TRUE, prob=c(0.3,0.7)))
#' res <- model_technique_adoption(bib,
#'   technique_pattern = "sacrocolpopexy",
#'   verbose = FALSE)
#' "adoption_data" %in% names(res)
#'
#' is.data.frame(res$adoption_data)
#'
#' inherits(res$figure, "gg")
#'
#' @importFrom assertthat assert_that is.string is.flag
#' @importFrom dplyr filter mutate group_by summarise arrange
#' @importFrom tibble tibble
#' @importFrom ggplot2 ggplot aes geom_step geom_point labs theme_bw
#' @export
model_technique_adoption <- function(
    bibliography,
    technique_pattern    = "sacrocolpopexy",
    min_papers_threshold = 3L,
    year_end             = as.integer(format(Sys.Date(), "%Y")),
    verbose              = TRUE
) {
  assertthat::assert_that(is.data.frame(bibliography))
  assertthat::assert_that(assertthat::is.string(technique_pattern))
  assertthat::assert_that(assertthat::is.flag(verbose))

  if (!requireNamespace("survival", quietly = TRUE))
    stop("Package 'survival' required. Install: install.packages('survival')")

  # Detect technique mentions
  ti_col <- if ("TI" %in% names(bibliography))
    tolower(bibliography$TI) else rep("", nrow(bibliography))
  ab_col <- if ("AB" %in% names(bibliography))
    tolower(bibliography$AB) else rep("", nrow(bibliography))
  hits   <- stringr::str_detect(
    paste(ti_col, ab_col),
    stringr::regex(technique_pattern, ignore_case = TRUE))

  bib_hit <- bibliography[hits, ]

  # Per-subspecialty, find first year with >= threshold papers
  sp_adoption <- bibliography |>
    dplyr::select(dplyr::any_of(c("subspecialty","publication_year"))) |>
    dplyr::distinct(.data$subspecialty) |>
    dplyr::pull(.data$subspecialty)

  adoption_rows <- purrr::map_dfr(sp_adoption, function(sp) {
    sp_hits <- dplyr::filter(bib_hit,
      .data$subspecialty == sp,
      !is.na(.data$publication_year))

    annual <- sp_hits |>
      dplyr::count(.data$publication_year, name = "n_papers") |>
      dplyr::arrange(.data$publication_year)

    adopt_yr <- annual |>
      dplyr::filter(.data$n_papers >= min_papers_threshold) |>
      dplyr::slice(1L) |>
      dplyr::pull(.data$publication_year)

    first_pub_yr <- min(bibliography$publication_year[
      bibliography$subspecialty == sp], na.rm = TRUE)

    if (length(adopt_yr) == 0L || is.na(adopt_yr)) {
      dplyr::tibble(subspecialty=sp, adoption_year=NA_integer_,
                    time=year_end - first_pub_yr, event=0L)
    } else {
      dplyr::tibble(subspecialty=sp, adoption_year=adopt_yr,
                    time=adopt_yr - first_pub_yr, event=1L)
    }
  })

  .log_step(sprintf(
    "[ADOPTION] %d/%d subspecialties adopted '%s' (threshold=%d papers/yr)",
    sum(adoption_rows$event), nrow(adoption_rows),
    technique_pattern, min_papers_threshold), verbose)

  if (sum(!is.na(adoption_rows$time) & adoption_rows$time >= 0) < 2L) {
    .log_step("[ADOPTION] Insufficient events — returning data only.", verbose)
    return(list(adoption_data = adoption_rows,
                km_fit  = NULL,
                figure  = NULL))
  }

  surv_obj <- survival::Surv(
    time  = pmax(adoption_rows$time, 0L),
    event = adoption_rows$event)
  km_fit   <- survival::survfit(surv_obj ~ 1)

  # K-M curve
  km_df <- data.frame(
    time     = km_fit$time,
    survival = km_fit$surv,
    lower    = km_fit$lower,
    upper    = km_fit$upper
  )
  fig <- ggplot2::ggplot(km_df,
    ggplot2::aes(x = .data$time, y = .data$survival)) +
    ggplot2::geom_ribbon(ggplot2::aes(ymin=.data$lower, ymax=.data$upper),
                         alpha=0.2, fill="#2c7bb6") +
    ggplot2::geom_step(colour="#2c7bb6", linewidth=1.1) +
    ggplot2::geom_point(
      data = dplyr::filter(adoption_rows, .data$event==1L),
      ggplot2::aes(x=.data$time, y=0.02),
      shape="|", size=3, colour="#e31a1c") +
    ggplot2::scale_y_continuous(limits=c(0,1),
      labels=scales::label_percent()) +
    ggplot2::labs(
      title    = sprintf("Time-to-Adoption: '%s'", technique_pattern),
      subtitle = sprintf(
        "%d/%d subspecialties adopted (threshold: >= %d papers/yr)",
        sum(adoption_rows$event), nrow(adoption_rows),
        min_papers_threshold),
      x = "Years from first publication in subspecialty",
      y = "Proportion not yet adopted"
    ) +
    ggplot2::theme_bw(base_size=11)

  list(
    adoption_data = adoption_rows,
    km_fit        = km_fit,
    figure        = fig
  )
}

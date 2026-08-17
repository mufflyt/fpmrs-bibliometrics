# Record-accounting tests.
#
# Regression guard for a reporting bug in which the year-range and language
# exclusions were conflated: the caller hardcoded n_excluded_lang <- 0L and
# derived year exclusions from nrow(raw) - nrow(final), so a corpus that
# genuinely dropped thousands of non-English records reported zero.

make_raw_bibliography <- function() {
  # 6 in-range English, 3 in-range non-English, 2 out of range,
  # 1 unparseable year.
  tibble::tibble(
    PY = c(
      rep("2010", 6L),      # in range, English
      rep("2011", 3L),      # in range, non-English
      "1970", "2030",       # outside 1975-2024
      "no year here"        # unparseable
    ),
    LA = c(
      rep("ENG", 6L),
      "FRE", "GER", "CHI",
      "ENG", "ENG",
      "ENG"
    ),
    TI = paste("Title", 1:12)
  )
}

test_that("filter_counts separates year and language exclusions", {
  raw <- make_raw_bibliography()

  filtered <- .standardize_and_filter_bibliography(
    bibliography_raw = raw,
    year_start       = 1975L,
    year_end         = 2024L,
    english_only     = TRUE,
    verbose          = FALSE
  )

  counts <- attr(filtered, "filter_counts")
  expect_type(counts, "list")

  expect_equal(counts$n_retrieved, 12L)
  # 1970 + 2030 out of range, plus 1 unparseable
  expect_equal(counts$n_excluded_year, 3L)
  expect_equal(counts$n_after_year_filter, 9L)
  # FRE + GER + CHI
  expect_equal(counts$n_excluded_language, 3L)
  expect_equal(counts$n_after_lang_filter, 6L)
  expect_equal(nrow(filtered), 6L)
})

test_that("stage counts are internally consistent", {
  raw <- make_raw_bibliography()
  filtered <- .standardize_and_filter_bibliography(
    raw, 1975L, 2024L, english_only = TRUE, verbose = FALSE
  )
  cts <- attr(filtered, "filter_counts")

  expect_equal(
    cts$n_retrieved - cts$n_excluded_year,
    cts$n_after_year_filter
  )
  expect_equal(
    cts$n_after_year_filter - cts$n_excluded_language,
    cts$n_after_lang_filter
  )
  expect_equal(cts$n_after_lang_filter, nrow(filtered))
})

test_that("english_only = FALSE retains non-English records and warns", {
  raw <- make_raw_bibliography()

  # Retaining non-English records degrades keyword/thematic analysis, so
  # the pipeline is expected to warn rather than do it silently.
  expect_warning(
    filtered <- .standardize_and_filter_bibliography(
      raw, 1975L, 2024L, english_only = FALSE, verbose = FALSE
    ),
    "non-English records retained"
  )
  cts <- attr(filtered, "filter_counts")

  expect_equal(nrow(filtered), 9L)
  expect_equal(cts$n_excluded_language, 0L)
})

test_that("a real language mix is not silently reported as zero exclusions", {
  # The specific failure mode: a corpus with non-English records must never
  # report n_excluded_language == 0 while english_only is TRUE.
  raw <- make_raw_bibliography()
  filtered <- .standardize_and_filter_bibliography(
    raw, 1975L, 2024L, english_only = TRUE, verbose = FALSE
  )
  cts <- attr(filtered, "filter_counts")

  n_non_english_in_range <- sum(
    !toupper(trimws(raw$LA)) %in% c("ENGLISH", "ENG", "EN") &
      as.integer(stringr::str_extract(raw$PY, "[0-9]{4}")) %in% 1975:2024,
    na.rm = TRUE
  )
  expect_gt(n_non_english_in_range, 0L)
  expect_equal(cts$n_excluded_language, n_non_english_in_range)
})

test_that("missing LA column retains all records rather than erroring", {
  raw <- make_raw_bibliography()
  raw$LA <- NULL

  filtered <- .standardize_and_filter_bibliography(
    raw, 1975L, 2024L, english_only = TRUE, verbose = FALSE
  )
  cts <- attr(filtered, "filter_counts")

  expect_equal(cts$n_excluded_language, 0L)
  expect_equal(nrow(filtered), 9L)
})

test_that("CONSORT flow file reflects the true stage counts", {
  out <- withr_tempdir()
  .write_consort_flow(
    output_dir          = out,
    data_source         = "pubmed",
    n_retrieved         = 59373L,
    n_excluded_year     = 2898L,
    n_excluded_language = 7120L,
    n_excluded_dedup    = 0L,
    n_final             = 49355L,
    year_start          = 1975L,
    year_end            = 2024L,
    english_only        = TRUE,
    verbose             = FALSE
  )

  txt <- readLines(file.path(out, "consort_flow.txt"))
  expect_true(any(grepl("Excluded \\(year range\\)\\s+: 2898", txt)))
  expect_true(any(grepl("After year filter\\s+: 56475", txt)))
  expect_true(any(grepl("Excluded \\(non-English\\)\\s+: 7120", txt)))
  expect_true(any(grepl("After language filter\\s+: 49355", txt)))
  expect_true(any(grepl("INCLUDED IN ANALYSIS\\s+: 49355", txt)))

  # And the JSON must agree with the text rendering.
  js <- jsonlite::fromJSON(file.path(out, "consort_flow.json"))
  expect_equal(js$excluded_language$n, 7120L)
  expect_equal(js$stage_3_lang_filter$n, 49355L)
  expect_equal(js$stage_4_final$n, 49355L)
})

test_that("zero non-English exclusions still render correctly", {
  out <- withr_tempdir()
  .write_consort_flow(
    output_dir = out, data_source = "pubmed",
    n_retrieved = 100L, n_excluded_year = 10L, n_excluded_language = 0L,
    n_excluded_dedup = 0L, n_final = 90L,
    year_start = 1975L, year_end = 2024L, english_only = TRUE,
    verbose = FALSE
  )
  txt <- readLines(file.path(out, "consort_flow.txt"))
  expect_true(any(grepl("Excluded \\(non-English\\)\\s+: 0", txt)))
  expect_true(any(grepl("INCLUDED IN ANALYSIS\\s+: 90", txt)))
})

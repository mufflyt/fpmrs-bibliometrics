# Scope exclusion.
#
# The search deliberately uses unqualified terms ("urinary incontinence",
# "lower urinary tract symptoms") so that pre-MeSH and loosely indexed
# female pelvic floor literature is still retrieved. The cost is that
# post-prostatectomy incontinence and benign prostatic disease come with it
# -- PROSTATECTOMY and PROSTATE CANCER reached the top-20 keyword figure of
# a female pelvic medicine corpus.

test_that("male lower-urinary-tract records are flagged", {
  bib <- data.frame(
    MESH = c("PROSTATECTOMY; URINARY INCONTINENCE",
             "PROSTATIC NEOPLASMS",
             "PELVIC ORGAN PROLAPSE; PELVIC FLOOR"),
    TI = c("Incontinence after prostatectomy",
           "Prostate cancer outcomes",
           "Sacrocolpopexy outcomes"),
    DE = c("", "", ""),
    stringsAsFactors = FALSE
  )
  expect_equal(.flag_out_of_scope_records(bib), c(TRUE, TRUE, FALSE))
})

test_that("records with female pelvic floor indexing are kept", {
  # A comparative or mixed-sex study belongs in the corpus even though it
  # mentions the prostate.
  bib <- data.frame(
    MESH = "PROSTATECTOMY; PELVIC FLOOR; URINARY INCONTINENCE, STRESS",
    TI   = "Pelvic floor muscle training after prostatectomy and in women",
    DE   = "pelvic floor",
    stringsAsFactors = FALSE
  )
  expect_false(.flag_out_of_scope_records(bib))
})

test_that("flagging reads title and keywords when MeSH is absent", {
  bib <- data.frame(
    TI = c("Radical prostatectomy series", "Vaginal prolapse repair"),
    stringsAsFactors = FALSE
  )
  expect_equal(.flag_out_of_scope_records(bib), c(TRUE, FALSE))
})

test_that("a corpus with no MeSH column does not error", {
  bib <- data.frame(TI = c("A", "B"), stringsAsFactors = FALSE)
  expect_equal(.flag_out_of_scope_records(bib), c(FALSE, FALSE))
})

test_that("the scope filter removes records and reports the count", {
  raw <- tibble::tibble(
    PY = rep("2010", 6L),
    LA = rep("ENG", 6L),
    TI = c("Prostatectomy incontinence", "Prostate cancer survival",
           "Sacrocolpopexy outcomes", "Cystocele repair",
           "Pelvic floor muscle training", "Uterine prolapse cohort"),
    MESH = c("PROSTATECTOMY", "PROSTATIC NEOPLASMS", "PELVIC ORGAN PROLAPSE",
             "CYSTOCELE", "PELVIC FLOOR", "UTERINE PROLAPSE")
  )
  out <- .standardize_and_filter_bibliography(
    raw, 1975L, 2024L, english_only = TRUE, verbose = FALSE
  )
  cts <- attr(out, "filter_counts")

  expect_equal(cts$n_excluded_scope, 2L)
  expect_equal(cts$n_after_scope_filter, 4L)
  expect_equal(nrow(out), 4L)
  expect_false(any(grepl("PROSTAT", toupper(out$TI))))
})

test_that("the scope filter can be switched off", {
  raw <- tibble::tibble(
    PY = rep("2010", 3L), LA = rep("ENG", 3L),
    TI = c("Prostatectomy incontinence", "Cystocele repair", "Prolapse"),
    MESH = c("PROSTATECTOMY", "CYSTOCELE", "PELVIC ORGAN PROLAPSE")
  )
  out <- .standardize_and_filter_bibliography(
    raw, 1975L, 2024L, english_only = TRUE,
    exclude_out_of_scope = FALSE, verbose = FALSE
  )
  cts <- attr(out, "filter_counts")
  expect_equal(cts$n_excluded_scope, 0L)
  expect_equal(nrow(out), 3L)
})

test_that("stage counts remain internally consistent with the scope stage", {
  raw <- tibble::tibble(
    PY = c(rep("2010", 5L), "1970"),
    LA = c(rep("ENG", 4L), "FRE", "ENG"),
    TI = c("Prostatectomy incontinence", "Cystocele repair", "Prolapse",
           "Pelvic floor", "Prostate cancer", "Old paper"),
    MESH = c("PROSTATECTOMY", "CYSTOCELE", "PELVIC ORGAN PROLAPSE",
             "PELVIC FLOOR", "PROSTATIC NEOPLASMS", "PELVIC FLOOR")
  )
  out <- .standardize_and_filter_bibliography(
    raw, 1975L, 2024L, english_only = TRUE, verbose = FALSE
  )
  c2 <- attr(out, "filter_counts")

  expect_equal(c2$n_retrieved - c2$n_excluded_year, c2$n_after_year_filter)
  expect_equal(c2$n_after_year_filter - c2$n_excluded_language,
               c2$n_after_lang_filter)
  expect_equal(c2$n_after_lang_filter - c2$n_excluded_scope,
               c2$n_after_scope_filter)
  expect_equal(c2$n_after_scope_filter, nrow(out))
})

test_that("CONSORT output carries the scope exclusion row", {
  out <- withr_tempdir()
  .write_consort_flow(
    output_dir = out, data_source = "pubmed",
    n_retrieved = 59373L, n_excluded_year = 2898L,
    n_excluded_language = 7120L, n_excluded_dedup = 0L,
    n_final = 46861L, year_start = 1975L, year_end = 2024L,
    english_only = TRUE, n_excluded_scope = 2494L, verbose = FALSE
  )
  txt <- readLines(file.path(out, "consort_flow.txt"))
  expect_true(any(grepl("Excluded \\(out of scope\\)\\s+: 2494", txt)))
  expect_true(any(grepl("After scope filter\\s+: 46861", txt)))

  js <- jsonlite::fromJSON(file.path(out, "consort_flow.json"))
  expect_equal(js$excluded_scope$n, 2494L)
  expect_equal(js$stage_3b_scope_filter$n, 46861L)
})

# Query construction and provenance reproducibility.
#
# The PubMed query defines the corpus, so a silent edit invalidates every
# published number. These tests pin its structure and the determinism of
# the hash used to stamp outputs.

test_that("the URPS query is retrievable under its aliases", {
  q <- get_subspecialty_pubmed_query("fpmrs")
  expect_type(q, "character")
  expect_length(q, 1L)
  expect_gt(nchar(q), 100L)

  # Aliases must resolve to the identical query, not a near-copy.
  expect_identical(get_subspecialty_pubmed_query("FPMRS"), q)
  expect_identical(get_subspecialty_pubmed_query("urps"), q)
  expect_identical(get_subspecialty_pubmed_query("urogynecology"), q)
})

test_that("the URPS query is balanced and well formed", {
  q <- get_subspecialty_pubmed_query("fpmrs")

  n_open  <- lengths(regmatches(q, gregexpr("\\(", q)))
  n_close <- lengths(regmatches(q, gregexpr("\\)", q)))
  expect_equal(n_open, n_close)

  # Quotes must pair up.
  expect_equal(lengths(regmatches(q, gregexpr('"', q))) %% 2L, 0L)

  # No dangling boolean operators.
  expect_false(grepl("OR\\s*\\)", q))
  expect_false(grepl("\\(\\s*OR", q))
  expect_false(grepl("OR\\s+OR", q))
})

test_that("the URPS query covers both nomenclature eras and the journal", {
  q <- get_subspecialty_pubmed_query("fpmrs")
  expect_match(q, "FPMRS", fixed = TRUE)
  expect_match(q, "URPS", fixed = TRUE)
  # The journal was renamed in 2022; both names must be captured.
  expect_match(q, "Female Pelvic Medicine and Reconstructive Surgery", fixed = TRUE)
  expect_match(q, '"Urogynecology"[Journal]', fixed = TRUE)
  # Core MeSH anchors.
  expect_match(q, "pelvic organ prolapse", fixed = TRUE)
  expect_match(q, "pelvic floor", fixed = TRUE)
})

test_that("comparator subspecialty queries are distinct from URPS", {
  q_urps <- get_subspecialty_pubmed_query("fpmrs")
  for (key in c("gyn_onc", "mfm", "rei")) {
    q <- get_subspecialty_pubmed_query(key)
    expect_type(q, "character")
    expect_gt(nchar(q), 50L)
    expect_false(identical(q, q_urps))
  }
})

test_that("an unknown subspecialty key is rejected", {
  expect_error(get_subspecialty_pubmed_query("not_a_subspecialty"))
})

test_that("the query hash is deterministic and query-sensitive", {
  q <- get_subspecialty_pubmed_query("fpmrs")

  h1 <- .compute_query_hash(q, "pubmed", 1975L, 2024L)
  h2 <- .compute_query_hash(q, "pubmed", 1975L, 2024L)
  expect_identical(h1, h2)

  # Any change to query or window must change the stamp.
  expect_false(identical(h1, .compute_query_hash(paste0(q, " "), "pubmed", 1975L, 2024L)))
  expect_false(identical(h1, .compute_query_hash(q, "pubmed", 1975L, 2023L)))
  expect_false(identical(h1, .compute_query_hash(q, "pubmed", 1976L, 2024L)))
})

test_that("search provenance records the parameters actually used", {
  out <- withr_tempdir()
  q <- get_subspecialty_pubmed_query("fpmrs")

  .write_search_provenance(
    output_dir          = out,
    data_source         = "pubmed",
    pubmed_query        = q,
    file_path           = NULL,
    year_start          = 1975L,
    year_end            = 2024L,
    english_only        = TRUE,
    n_retrieved         = 59373L,
    n_after_year_filter = 56475L,
    n_after_lang_filter = 49355L,
    n_final             = 49355L,
    verbose             = FALSE
  )

  js <- jsonlite::fromJSON(file.path(out, "search_provenance.json"))
  expect_identical(js$pubmed_query, q)
  expect_equal(js$year_start, 1975L)
  expect_equal(js$year_end, 2024L)
  expect_true(js$english_only)
  expect_equal(js$records$retrieved, 59373L)
  # The year-filter figure must be the post-year, pre-language count.
  expect_equal(js$records$after_year_filter, 56475L)
  expect_equal(js$records$after_lang_filter, 49355L)
  expect_equal(js$records$final_analysis_set, 49355L)
})

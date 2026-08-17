# End-to-end pipeline stages driven by synthetic data.
#
# The real pipeline needs PubMed and OpenAlex, which must never be a
# prerequisite for CI. generate_synthetic_bibliography() produces a corpus
# with the same column contract, letting the analysis stages run offline.

test_that("synthetic bibliography has the expected column contract", {
  bib <- generate_synthetic_bibliography(
    n_papers = 200L, n_subspecialties = 3L,
    year_start = 1990L, year_end = 2024L, seed = 1L, verbose = FALSE
  )

  expect_s3_class(bib, "data.frame")
  expect_equal(nrow(bib), 200L)
  # Columns consumed downstream by the filter/trend/plot stages.
  expect_true(all(c(
    "UT", "publication_year", "TI", "AB", "AU", "AF",
    "SO", "TC", "PY", "DE", "LA", "DT", "subspecialty"
  ) %in% names(bib)))
})

test_that("synthetic corpora use URPS, not the retired FPMRS name", {
  bib <- generate_synthetic_bibliography(
    n_papers = 200L, n_subspecialties = 8L, seed = 3L, verbose = FALSE
  )
  expect_true("URPS" %in% bib$subspecialty)
  expect_false("FPMRS" %in% bib$subspecialty)
})

test_that("synthetic generation is reproducible for a fixed seed", {
  a <- generate_synthetic_bibliography(n_papers = 100L, seed = 7L, verbose = FALSE)
  b <- generate_synthetic_bibliography(n_papers = 100L, seed = 7L, verbose = FALSE)
  expect_identical(a, b)

  c <- generate_synthetic_bibliography(n_papers = 100L, seed = 8L, verbose = FALSE)
  expect_false(identical(a$TI, c$TI))
})

test_that("synthetic generator validates its arguments", {
  expect_error(generate_synthetic_bibliography(n_papers = -5L, verbose = FALSE))
  expect_error(generate_synthetic_bibliography(pct_female = 1.5, verbose = FALSE))
})

test_that("filter stage runs on synthetic data and reports counts", {
  bib <- generate_synthetic_bibliography(
    n_papers = 300L, year_start = 1990L, year_end = 2024L,
    seed = 2L, verbose = FALSE
  )
  filtered <- .standardize_and_filter_bibliography(
    bib, 1990L, 2024L, english_only = TRUE, verbose = FALSE
  )

  expect_gt(nrow(filtered), 0L)
  expect_true("publication_year" %in% names(filtered))
  cts <- attr(filtered, "filter_counts")
  expect_equal(cts$n_retrieved, 300L)
  expect_equal(cts$n_after_lang_filter, nrow(filtered))
})

test_that("narrowing the year window drops records", {
  bib <- generate_synthetic_bibliography(
    n_papers = 300L, year_start = 1990L, year_end = 2024L,
    seed = 2L, verbose = FALSE
  )
  wide <- .standardize_and_filter_bibliography(
    bib, 1990L, 2024L, english_only = TRUE, verbose = FALSE
  )
  narrow <- .standardize_and_filter_bibliography(
    bib, 2010L, 2024L, english_only = TRUE, verbose = FALSE
  )
  expect_lt(nrow(narrow), nrow(wide))
  expect_true(all(narrow$publication_year >= 2010L))
})

test_that("annual trends are computed and internally consistent", {
  bib <- generate_synthetic_bibliography(
    n_papers = 400L, year_start = 1990L, year_end = 2024L,
    seed = 5L, verbose = FALSE
  )
  filtered <- .standardize_and_filter_bibliography(
    bib, 1990L, 2024L, english_only = TRUE, verbose = FALSE
  )
  trends <- .compute_annual_publication_trends(filtered, verbose = FALSE)

  expect_true(all(c(
    "publication_year", "publication_count", "total_citations",
    "mean_citations", "cumulative_publications"
  ) %in% names(trends)))

  # Every record lands in exactly one year.
  expect_equal(sum(trends$publication_count), nrow(filtered))
  # Cumulative series is monotonic and ends at the total.
  expect_false(is.unsorted(trends$cumulative_publications))
  expect_equal(
    trends$cumulative_publications[nrow(trends)],
    sum(trends$publication_count)
  )
  # Years are unique and ascending.
  expect_false(anyDuplicated(trends$publication_year) > 0L)
  expect_false(is.unsorted(trends$publication_year))
})

test_that("mean citations equal total over count", {
  bib <- generate_synthetic_bibliography(
    n_papers = 400L, year_start = 1990L, year_end = 2024L,
    seed = 6L, verbose = FALSE
  )
  filtered <- .standardize_and_filter_bibliography(
    bib, 1990L, 2024L, english_only = TRUE, verbose = FALSE
  )
  trends <- .compute_annual_publication_trends(filtered, verbose = FALSE)

  expect_equal(
    trends$mean_citations,
    trends$total_citations / trends$publication_count,
    tolerance = 1e-8
  )
})

test_that("trend output feeds the figure functions without adaptation", {
  bib <- generate_synthetic_bibliography(
    n_papers = 400L, year_start = 1990L, year_end = 2024L,
    seed = 9L, verbose = FALSE
  )
  filtered <- .standardize_and_filter_bibliography(
    bib, 1990L, 2024L, english_only = TRUE, verbose = FALSE
  )
  trends <- .compute_annual_publication_trends(filtered, verbose = FALSE)

  expect_s3_class(
    plot_annual_publications(trends, 1990L, 2024L, verbose = FALSE),
    "ggplot"
  )
  skip_if_no_pkg("patchwork")
  expect_s3_class(
    plot_citation_trends(trends, 1990L, 2024L, verbose = FALSE),
    "patchwork"
  )
})

test_that("an empty corpus is rejected with a clear message", {
  bib <- generate_synthetic_bibliography(
    n_papers = 50L, year_start = 1990L, year_end = 2000L,
    seed = 4L, verbose = FALSE
  )
  # No records can survive a disjoint year window.
  expect_error(
    .standardize_and_filter_bibliography(
      bib, 2050L, 2060L, english_only = TRUE, verbose = FALSE
    ),
    "No records remain"
  )
})

# Field-Weighted Citation Impact.
#
# FWCI was already being fetched from OpenAlex and silently discarded. It is
# normalised for field and publication year, so unlike raw citation counts it
# is not depressed for recent papers by their shorter citation window -- it
# is the evidence that the decline in raw counts is censoring rather than a
# real fall in impact.

test_that("annual trends carry FWCI when the column is present", {
  bib <- data.frame(
    publication_year = rep(2000:2002, each = 4),
    TC   = as.character(rep(10L, 12)),
    fwci = c(rep(1.0, 4), rep(2.0, 4), rep(0.5, 4))
  )
  tr <- .compute_annual_publication_trends(bib, verbose = FALSE)

  expect_true(all(c("mean_fwci", "median_fwci", "n_with_fwci") %in% names(tr)))
  expect_equal(tr$mean_fwci, c(1.0, 2.0, 0.5))
  expect_equal(tr$median_fwci, c(1.0, 2.0, 0.5))
  expect_equal(tr$n_with_fwci, c(4L, 4L, 4L))
})

test_that("a corpus without FWCI still produces annual trends", {
  bib <- data.frame(
    publication_year = rep(2000:2001, each = 3),
    TC = as.character(rep(5L, 6))
  )
  tr <- .compute_annual_publication_trends(bib, verbose = FALSE)
  expect_true(all(is.na(tr$mean_fwci)))
  expect_equal(tr$n_with_fwci, c(0L, 0L))
})

test_that("missing FWCI values are excluded rather than treated as zero", {
  bib <- data.frame(
    publication_year = rep(2000L, 4L),
    TC   = as.character(rep(5L, 4)),
    fwci = c(2.0, 2.0, NA, NA)
  )
  tr <- .compute_annual_publication_trends(bib, verbose = FALSE)
  expect_equal(tr$mean_fwci, 2.0)
  expect_equal(tr$n_with_fwci, 2L)
})

test_that("the citation figure gains a third panel when FWCI is available", {
  tr <- fixture_annual_trends()
  tr$mean_fwci   <- seq(0.6, 1.1, length.out = nrow(tr))
  tr$median_fwci <- seq(0.5, 1.0, length.out = nrow(tr))
  tr$n_with_fwci <- 100L

  p <- plot_citation_trends(tr, 1975L, 2024L, verbose = FALSE)
  expect_s3_class(p, "patchwork")
  # patchwork holds n-1 plots in $patches$plots plus the top-level plot
  expect_equal(length(p$patches$plots) + 1L, 3L)
})

test_that("the citation figure stays two panels without FWCI", {
  p <- plot_citation_trends(fixture_annual_trends(), 1975L, 2024L, verbose = FALSE)
  expect_s3_class(p, "patchwork")
  expect_equal(length(p$patches$plots) + 1L, 2L)
})

test_that("the FWCI panel caption is not clipped", {
  skip_if_no_pkg("pdftools")
  tr <- fixture_annual_trends()
  tr$mean_fwci   <- seq(0.6, 1.1, length.out = nrow(tr))
  tr$median_fwci <- seq(0.5, 1.0, length.out = nrow(tr))
  tr$n_with_fwci <- 100L
  p <- plot_citation_trends(tr, 1975L, 2024L, verbose = FALSE)

  rendered <- render_pdf_text(p, width = 7, height = 8.5)
  expect_true(grepl("reflects citation censoring", rendered, fixed = TRUE))
})

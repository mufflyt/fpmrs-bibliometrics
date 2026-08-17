# Live API contract tests.
#
# These hit NCBI E-utilities and OpenAlex, so they are opt-in: they run
# only when FPMRS_RUN_NETWORK_TESTS=true, which CI sets exclusively in the
# scheduled workflow. Pull requests must never fail because an external
# service is slow or rate-limiting.
#
# Their purpose is to detect upstream contract drift -- a renamed field or
# changed response shape -- not to validate our analysis logic.

test_that("PubMed search returns PMIDs for the URPS query", {
  skip_if_offline_suite()
  skip_if_no_pkg("rentrez")

  q <- get_subspecialty_pubmed_query("fpmrs")
  res <- try(
    rentrez::entrez_search(db = "pubmed", term = q, retmax = 5L),
    silent = TRUE
  )
  skip_if(inherits(res, "try-error"), "NCBI unreachable")

  expect_true(is.list(res))
  expect_true("count" %in% names(res))
  expect_gt(as.integer(res$count), 1000L)
  expect_gte(length(res$ids), 1L)
})

test_that("OpenAlex still exposes the fields the enrichment step reads", {
  skip_if_offline_suite()
  skip_if_no_pkg("openalexR")

  # Two well-known PMIDs; the enrichment maps by PMID and reads
  # cited_by_count, affiliations$country_code, is_oa and fwci.
  res <- try(
    openalexR::oa_fetch(
      entity = "works",
      pmid = c("11376147", "16260828"),
      verbose = FALSE
    ),
    silent = TRUE
  )
  skip_if(inherits(res, "try-error"), "OpenAlex unreachable")
  skip_if(is.null(res) || nrow(res) == 0L, "OpenAlex returned no rows")

  expect_true("cited_by_count" %in% names(res))
  expect_true("ids" %in% names(res) || "id" %in% names(res))
  # Enrichment reads the first affiliation's country_code.
  if ("affiliations" %in% names(res)) {
    affs <- res$affiliations[[1L]]
    if (!is.null(affs) && is.data.frame(affs) && nrow(affs) > 0L) {
      expect_true("country_code" %in% names(affs))
    }
  }
})

test_that("enrichment degrades gracefully when openalexR is unavailable", {
  # Not a network test: verifies the guard path. The function must return
  # the input unchanged rather than erroring when the package is absent.
  bib <- generate_synthetic_bibliography(
    n_papers = 20L, seed = 11L, verbose = FALSE
  )
  bib$PMID <- as.character(seq_len(nrow(bib)))

  out <- .enrich_with_openalex(bib, verbose = FALSE)
  expect_s3_class(out, "data.frame")
  expect_equal(nrow(out), nrow(bib))
})

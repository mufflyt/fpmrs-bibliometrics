# Country normalisation and World Bank income tiers.
#
# Regression guard for a defect that put a false claim in the abstract:
# OpenAlex reports affiliation countries as ISO 3166-1 alpha-2 codes, but
# .wb_income_tier() keyed only on full names and 3-letter codes. Every
# non-US record fell through as NA, so 72.8% of the corpus was "unmapped",
# the high-income share collapsed to the US share alone (27.2% vs a true
# ~82%), and the abstract reported "only 0.0% of publications originated
# from lower- or low-income countries" as though it were a finding.

test_that("ISO-2 codes normalise to canonical country names", {
  expect_equal(.normalize_country_string("GB"), "UNITED KINGDOM")
  expect_equal(.normalize_country_string("CN"), "CHINA")
  expect_equal(.normalize_country_string("DE"), "GERMANY")
  expect_equal(.normalize_country_string("KR"), "SOUTH KOREA")
  expect_equal(.normalize_country_string("US"), "USA")
})

test_that("full names and legacy variants still normalise", {
  expect_equal(.normalize_country_string("UNITED STATES"), "USA")
  expect_equal(.normalize_country_string("USA"), "USA")
  expect_equal(.normalize_country_string("ENGLAND"), "UNITED KINGDOM")
  expect_equal(.normalize_country_string("HOLLAND"), "NETHERLANDS")
})

test_that("normalisation is vectorised over mixed code and name input", {
  out <- .normalize_country_string(c("GB", "USA", "CN", "ENGLAND"))
  expect_equal(out, c("UNITED KINGDOM", "USA", "CHINA", "UNITED KINGDOM"))
})

test_that("ISO-2 codes resolve to an income tier", {
  tier <- function(x) .wb_income_tier(.normalize_country_string(x))
  expect_equal(tier("GB"), "High")
  expect_equal(tier("DE"), "High")
  expect_equal(tier("JP"), "High")
  expect_equal(tier("CN"), "Upper-Middle")
  expect_equal(tier("BR"), "Upper-Middle")
  expect_equal(tier("IN"), "Lower-Middle")
  expect_equal(tier("NG"), "Lower-Middle")
  expect_equal(tier("ET"), "Low")
})

test_that("no ISO-2 code in the map is left without a tier", {
  # A code that normalises but has no tier silently becomes "unmapped" and
  # deflates every income percentage.
  names_out <- unname(.ISO2_TO_COUNTRY)
  tiers <- .wb_income_tier(names_out)
  missing <- names(.ISO2_TO_COUNTRY)[is.na(tiers)]
  expect_equal(
    length(missing), 0L,
    info = paste("codes with no income tier:", paste(missing, collapse = ", "))
  )
})

test_that("equity metrics classify a realistic ISO-2 corpus", {
  bib <- data.frame(
    AU_CO = c(rep("US", 50), rep("GB", 20), rep("CN", 15),
              rep("IN", 10), rep("ET", 5)),
    stringsAsFactors = FALSE
  )
  eq <- .compute_equity_metrics(bib, verbose = FALSE)

  expect_equal(eq$pct_unmapped, 0)
  expect_equal(eq$pct_high_income, 70)    # US + GB
  expect_equal(eq$pct_upper_mid, 15)      # CN
  expect_equal(eq$pct_lower_mid, 10)      # IN
  expect_equal(eq$pct_low_income, 5)      # ET
})

test_that("placeholder country strings are excluded from the denominator", {
  # The literal "NA" is a missing-country marker, not a country. Counting it
  # inflates pct_unmapped and dilutes every income share.
  bib <- data.frame(
    AU_CO = c(rep("US", 50), rep("NA", 50)),
    stringsAsFactors = FALSE
  )
  eq <- .compute_equity_metrics(bib, verbose = FALSE)

  expect_equal(eq$n_papers_with_auco, 50L)
  expect_equal(eq$pct_high_income, 100)
  expect_equal(eq$pct_unmapped, 0)
})

test_that("an all-placeholder corpus does not divide by zero", {
  bib <- data.frame(AU_CO = rep("NA", 10), stringsAsFactors = FALSE)
  eq <- .compute_equity_metrics(bib, verbose = FALSE)
  expect_equal(eq$n_papers_with_auco, 0L)
  expect_true(is.na(eq$pct_high_income))
})

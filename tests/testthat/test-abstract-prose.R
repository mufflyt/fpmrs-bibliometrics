# Abstract prose tests.
#
# generate_abstract_results_text() emits manuscript sentences, so its output
# is checked as prose: correct subspecialty name, journal-style statistics,
# no malformed numbers, and no claims that contradict the data.

test_that("abstract returns one string per expected section", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )

  expect_type(ab, "list")
  expect_true(all(c(
    "title", "introduction", "methods", "corpus", "citations", "growth",
    "geography", "output", "urology", "conclusion", "formatted"
  ) %in% names(ab)))

  present <- Filter(Negate(is.null), ab)
  expect_true(all(vapply(present, is.character, logical(1L))))
  expect_true(all(nchar(unlist(present)) > 0L))
})

test_that("prose uses URPS and never the retired FPMRS name", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )

  expect_false(grepl("FPMRS", ab$formatted, fixed = TRUE))
  expect_true(grepl("URPS", ab$formatted, fixed = TRUE))
})

test_that("URPS resolves to a full-name label when labels are supplied", {
  expect_true("URPS" %in% names(default_subspecialty_labels))
  expect_match(
    default_subspecialty_labels[["URPS"]],
    "urogynecology and reconstructive pelvic surgery",
    fixed = TRUE
  )
})

test_that("volume ratio is not rounded to a useless integer", {
  # 45000 / 10000 = 4.5. Rounding to whole numbers printed "1x" for a true
  # ratio of 1.4 and "0x" for anything below 0.5.
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$urology, "4.5x", fixed = TRUE)
})

test_that("a near-parity volume ratio does not collapse to 0x or 1x", {
  tbl <- fixture_comparison_table()
  tbl$total_documents <- c(10000L, 4000L, 20000L)  # urology/focal = 0.4

  ab <- generate_abstract_results_text(
    tbl, focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$urology, "0.4x", fixed = TRUE)
  expect_false(grepl("approximately 0x", ab$urology, fixed = TRUE))
})

test_that("journal names are title-cased without capitalising 'of'", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$output, "The Journal of Urology", fixed = TRUE)
  expect_false(grepl("Journal Of Urology", ab$output, fixed = TRUE))
})

test_that("author initials are preserved rather than title-cased", {
  # "DIETZ HP" must not become "Dietz Hp".
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$output, "Dietz HP", fixed = TRUE)
  expect_false(grepl("Dietz Hp", ab$output, fixed = TRUE))
})

test_that("semicolons do not start a new capitalised word", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  # A semicolon is not a sentence boundary.
  expect_false(
    grepl("; [A-Z][a-z]", ab$formatted),
    info = paste("capitalised token after semicolon in:", ab$formatted)
  )
})

test_that("methods sentence discloses OpenAlex as the citation/country source", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$methods, "OpenAlex", fixed = TRUE)
  expect_match(ab$methods, "PubMed", fixed = TRUE)
})

test_that("focal subspecialty absent from the table is rejected", {
  expect_error(
    generate_abstract_results_text(
      fixture_comparison_table(),
      focal_subspecialty = "NotASubspecialty", verbose = FALSE
    ),
    "not found in comparison_summary_table"
  )
})

test_that("equity sentence does not claim concentration when HHI is low", {
  tbl <- fixture_comparison_table()
  # Low HHI, but high-income dominance -- the sentence fires on HIC alone
  # and must not assert "heavily concentrated".
  equity <- tibble::tibble(
    pct_high_income   = 95.0,
    pct_upper_mid     = 3.0,
    pct_lower_mid     = 1.0,
    pct_low_income    = 1.0,
    concentration_hhi = 0.05
  )

  ab <- generate_abstract_results_text(
    tbl, focal_subspecialty = "URPS",
    focal_equity_metrics = equity,
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )

  expect_false(is.null(ab$equity))
  expect_false(
    grepl("heavily concentrated", ab$equity, fixed = TRUE),
    info = ab$equity
  )
})

test_that("equity sentence does claim concentration when HHI is high", {
  tbl <- fixture_comparison_table()
  equity <- tibble::tibble(
    pct_high_income   = 95.0,
    pct_upper_mid     = 3.0,
    pct_lower_mid     = 1.0,
    pct_low_income    = 1.0,
    concentration_hhi = 0.45
  )

  ab <- generate_abstract_results_text(
    tbl, focal_subspecialty = "URPS",
    focal_equity_metrics = equity,
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$equity, "heavily concentrated", fixed = TRUE)
})

test_that("growth sentence handles a missing CAGR without erroring", {
  tbl <- fixture_comparison_table()
  tbl$cagr_pct[tbl$subspecialty == "URPS"] <- NA_real_

  ab <- generate_abstract_results_text(
    tbl, focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_true(is.character(ab$growth))
  expect_match(ab$growth, "unavailable", fixed = TRUE)
})

test_that("cohort label pluralises correctly and counts urology separately", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(),
    focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  # 3 rows = URPS + MFM + Urology -> "2 OB/GYN subspecialties and urology"
  # is expanded by the formatter, so assert on the un-expanded pieces.
  expect_match(ab$corpus, "URPS", fixed = TRUE)
  expect_match(ab$corpus, "10,000", fixed = TRUE)
})

# ---- Prose defects found by reading a real generated abstract -----------

test_that("affiliation strings are reduced to a readable institution", {
  # Raw institution_metrics rows are whole affiliation strings. Used
  # verbatim they produced "Department Of Sports Medicine, Norwegian School
  # Of Sport Sciences, Oslo, Norway. Was the most productive institution..."
  expect_equal(
    .clean_affiliation(
      "DEPARTMENT OF SPORTS MEDICINE, NORWEGIAN SCHOOL OF SPORT SCIENCES, OSLO, NORWAY."
    ),
    "Norwegian School of Sport Sciences"
  )
  expect_equal(
    .clean_affiliation("DEPARTMENT OF UROGYNAECOLOGY, KING'S COLLEGE HOSPITAL, LONDON, UK."),
    "King's College Hospital"
  )
  expect_equal(.clean_affiliation("MAYO CLINIC"), "Mayo Clinic")
  expect_true(is.na(.clean_affiliation(NA_character_)))
  expect_true(is.na(.clean_affiliation("")))
})

test_that("the institution sentence has no stray mid-sentence capital", {
  inst <- tibble::tibble(
    institution = "DEPARTMENT OF SPORTS MEDICINE, NORWEGIAN SCHOOL OF SPORT SCIENCES, OSLO, NORWAY.",
    publication_count = 39L
  )
  ab <- generate_abstract_results_text(
    fixture_comparison_table(), focal_subspecialty = "URPS",
    focal_institution_metrics = inst,
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$institution, "Norwegian School of Sport Sciences", fixed = TRUE)
  expect_false(grepl(". Was the most", ab$institution, fixed = TRUE))
  expect_false(grepl("Department Of", ab$institution, fixed = TRUE))
})

test_that("comparator counts exclude the focal subspecialty", {
  # URPS + MFM + Urology means ONE OB/GYN comparator, not two. Counting the
  # focal as its own comparator overstated the cohort in title and intro.
  ab <- generate_abstract_results_text(
    fixture_comparison_table(), focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$title, "1 obstetrics and gynecology subspecialty and urology",
               fixed = TRUE)
  expect_false(grepl("2 obstetrics and gynecology subspecialties and urology",
                     ab$title, fixed = TRUE))
})

test_that("the analysed-cohort count still includes the focal subspecialty", {
  # "among the N analysed" is the full set; only the comparator sense drops
  # the focal row.
  ab <- generate_abstract_results_text(
    fixture_comparison_table(), focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$corpus, "2 obstetrics and gynecology subspecialties and urology",
               fixed = TRUE)
})

test_that("rank 1 reads as 'largest', not '1st largest'", {
  tbl <- fixture_comparison_table()
  tbl$rank_by_volume <- c(1L, 2L, 3L)
  ab <- generate_abstract_results_text(
    tbl, focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$corpus, "the largest corpus", fixed = TRUE)
  expect_false(grepl("1st largest", ab$corpus, fixed = TRUE))
})

test_that("a non-first rank keeps its ordinal", {
  ab <- generate_abstract_results_text(
    fixture_comparison_table(), focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$corpus, "3rd largest corpus", fixed = TRUE)
})

test_that("the geography sentence is not a comma splice", {
  # The focal-is-broadest branch ended its clause with "," against a
  # following independent clause; the sibling branch used ";".
  tbl <- fixture_comparison_table()
  tbl$unique_countries <- c(200L, 100L, 90L)   # focal has widest reach
  ab <- generate_abstract_results_text(
    tbl, focal_subspecialty = "URPS",
    year_start = 1975L, year_end = 2024L, verbose = FALSE
  )
  expect_match(ab$geography, "broadest international reach in the cohort;",
               fixed = TRUE)
  expect_false(grepl("in the cohort, [0-9]", ab$geography))
})

# Formatting helpers: P-value style and journal/name title-casing.
#
# Journal house style for this manuscript is capital P, no leading zero.
# The formatter returns operator+value only; callers prepend the "P".

test_that(".fmt_pvalue drops the leading zero", {
  expect_equal(.fmt_pvalue(0.03),  "=.03")
  expect_equal(.fmt_pvalue(0.004), "=.004")
  expect_equal(.fmt_pvalue(0.5),   "=.50")
  expect_false(grepl("0\\.", .fmt_pvalue(0.03)))
})

test_that(".fmt_pvalue collapses very small values to <.001", {
  expect_equal(.fmt_pvalue(0.0001),  "<.001")
  expect_equal(.fmt_pvalue(1e-16),   "<.001")
  # Boundary: p == 0.001 is included in the "<" branch.
  expect_equal(.fmt_pvalue(0.001),   "<.001")
})

test_that(".fmt_pvalue uses 3 decimals below .01 and 2 above", {
  expect_equal(.fmt_pvalue(0.0042), "=.004")
  expect_equal(.fmt_pvalue(0.042),  "=.04")
})

test_that(".fmt_pvalue handles NA without erroring", {
  expect_equal(.fmt_pvalue(NA_real_), "=NA")
})

test_that(".fmt_pvalue rejects non-scalar input", {
  expect_error(.fmt_pvalue(c(0.01, 0.02)), "single numeric")
  expect_error(.fmt_pvalue("0.01"), "single numeric")
})

test_that("prepending P yields journal-style output", {
  expect_equal(paste0("P", .fmt_pvalue(0.0004)), "P<.001")
  expect_equal(paste0("P", .fmt_pvalue(0.03)),   "P=.03")
})

test_that(".title_case_journal lowercases mid-title function words", {
  expect_equal(
    .title_case_journal("THE JOURNAL OF UROLOGY"),
    "The Journal of Urology"
  )
  expect_equal(
    .title_case_journal("AMERICAN JOURNAL OF OBSTETRICS AND GYNECOLOGY"),
    "American Journal of Obstetrics and Gynecology"
  )
})

test_that(".title_case_journal keeps a leading article capitalised", {
  out <- .title_case_journal("THE JOURNAL OF UROLOGY")
  expect_true(startsWith(out, "The "))
})

test_that(".title_case_journal is vectorised", {
  out <- .title_case_journal(c("UROLOGY", "THE JOURNAL OF UROLOGY"))
  expect_length(out, 2L)
  expect_equal(out[[1L]], "Urology")
  expect_equal(out[[2L]], "The Journal of Urology")
})

test_that(".title_case_journal handles single-word and empty input", {
  expect_equal(.title_case_journal("UROLOGY"), "Urology")
  expect_equal(.title_case_journal(""), "")
})

test_that("ordinal_label produces ordinals with correct suffixes", {
  expect_equal(ordinal_label(1L), "1st")
  expect_equal(ordinal_label(2L), "2nd")
  expect_equal(ordinal_label(3L), "3rd")
  expect_equal(ordinal_label(4L), "4th")
  # Teens take "th" regardless of final digit.
  expect_equal(ordinal_label(11L), "11th")
  expect_equal(ordinal_label(12L), "12th")
  expect_equal(ordinal_label(13L), "13th")
  expect_equal(ordinal_label(21L), "21st")
})

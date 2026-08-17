# Test entry point.
#
# This project is a sourced script rather than an installed package, so the
# suite is driven with test_dir() against tests/testthat. The helper file
# sources R/fpmrs_bibliometrics_pipeline.R into the global environment.

library(testthat)

testthat::test_dir(
  "testthat",
  stop_on_failure = TRUE,
  stop_on_warning = FALSE
)

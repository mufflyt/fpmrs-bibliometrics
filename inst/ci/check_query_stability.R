#!/usr/bin/env Rscript
# Fail the build when a subspecialty PubMed query changes without the
# baseline being updated in the same commit.
#
# The query defines the corpus. A silent edit changes every count, every
# citation total and every figure, while all tests still pass. This guard
# makes that change explicit and reviewable.
#
# To accept an intentional query change:
#   Rscript inst/ci/check_query_stability.R --update
# then commit the updated inst/ci/query-baseline.json, and re-run the
# pipeline so published outputs match the new corpus.

suppressWarnings(suppressMessages(
  sys.source("R/fpmrs_bibliometrics_pipeline.R", envir = globalenv())
))

baseline_path <- "inst/ci/query-baseline.json"
keys <- c("fpmrs", "gyn_onc", "mfm", "rei")

current <- vapply(keys, function(k) {
  digest::digest(get_subspecialty_pubmed_query(k), algo = "sha256")
}, character(1L))

args <- commandArgs(trailingOnly = TRUE)

if ("--update" %in% args) {
  dir.create(dirname(baseline_path), showWarnings = FALSE, recursive = TRUE)
  jsonlite::write_json(
    as.list(current), baseline_path, pretty = TRUE, auto_unbox = TRUE
  )
  message("Baseline updated: ", baseline_path)
  quit(status = 0L)
}

if (!file.exists(baseline_path)) {
  stop(
    "Missing ", baseline_path, ".\n",
    "Create it with: Rscript inst/ci/check_query_stability.R --update",
    call. = FALSE
  )
}

baseline <- unlist(jsonlite::fromJSON(baseline_path))

drifted <- character(0L)
for (k in keys) {
  if (!k %in% names(baseline)) {
    drifted <- c(drifted, sprintf("  %-10s NEW (no baseline entry)", k))
  } else if (!identical(unname(baseline[[k]]), unname(current[[k]]))) {
    drifted <- c(drifted, sprintf(
      "  %-10s changed\n      baseline: %s\n      current : %s",
      k, substr(baseline[[k]], 1L, 16L), substr(current[[k]], 1L, 16L)
    ))
  }
}

removed <- setdiff(names(baseline), keys)
if (length(removed) > 0L) {
  drifted <- c(drifted, sprintf("  %-10s REMOVED", removed))
}

if (length(drifted) > 0L) {
  stop(
    "PubMed query definitions changed:\n",
    paste(drifted, collapse = "\n"),
    "\n\nA query change redefines the corpus and invalidates every ",
    "published number.\nIf this is intentional, run:\n",
    "  Rscript inst/ci/check_query_stability.R --update\n",
    "commit the updated baseline, and re-run the pipeline.",
    call. = FALSE
  )
}

message("Query definitions unchanged (", length(keys), " checked).")

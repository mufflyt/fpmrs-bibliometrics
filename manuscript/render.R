#!/usr/bin/env Rscript
# Render the URPS manuscript.
#
#   Rscript manuscript/render.R              # HTML (default)
#   Rscript manuscript/render.R word         # Word .docx
#   Rscript manuscript/render.R both
#   Rscript manuscript/render.R journal      # submission .docx -> output/
#   Rscript manuscript/render.R both
#   Rscript manuscript/render.R journal      # submission .docx -> output/ --refresh   # re-run the pipeline first
#
# Without --refresh the cached analysis at output/pipeline_result.rds is
# reused. That file is gitignored, so a fresh clone will run the full
# pipeline (PubMed fetch, ~30 min) on the first render.

args    <- commandArgs(trailingOnly = TRUE)
refresh <- "--refresh" %in% args
which   <- setdiff(args, "--refresh")
if (length(which) == 0L) which <- "html"

# "journal" renders the submission manuscript: structured abstract, IMRaD
# sections, numbered references via BibTeX + CSL, straight into output/.
if (identical(which[[1L]], "journal")) {
  rmd <- "manuscript/urps_journal_manuscript.Rmd"
  if (!file.exists(rmd)) stop("Cannot find ", rmd, " -- run from the repo root.")
  dir.create("output", showWarnings = FALSE)
  out <- rmarkdown::render(
    input         = rmd,
    output_format = "word_document",
    output_file   = "URPS_bibliometrics_manuscript.docx",
    output_dir    = normalizePath("output"),
    quiet         = TRUE
  )
  message("Wrote ", out)
  quit(status = 0L)
}

formats <- switch(
  which[[1L]],
  html = "html_document",
  word = "word_document",
  both = c("html_document", "word_document"),
  stop("Unknown target '", which[[1L]], "'. Use: html | word | both | journal")
)

rmd <- "manuscript/urps_bibliometrics_manuscript.Rmd"
if (!file.exists(rmd)) stop("Cannot find ", rmd, " -- run from the repo root.")

for (fmt in formats) {
  message("\n=== Rendering ", fmt, " ===")
  out <- rmarkdown::render(
    input         = rmd,
    output_format = fmt,
    params        = list(refresh = refresh),
    quiet         = TRUE
  )
  message("Wrote ", out)
}

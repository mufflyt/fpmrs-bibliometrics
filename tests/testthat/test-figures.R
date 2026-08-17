# Figure tests.
#
# The important one here is caption integrity. ggplot2 does not wrap
# captions, so a caption longer than the device width is silently clipped
# in the rendered PDF -- the plot object looks correct while the published
# figure is missing text. These tests render to a real PDF and assert the
# caption survives the round trip.

test_that("all five manuscript figures build without error", {
  expect_s3_class(
    plot_annual_publications(fixture_annual_trends(), 1975L, 2024L, verbose = FALSE),
    "ggplot"
  )
  expect_s3_class(
    plot_country_contributions(fixture_country_trends(), 4L, verbose = FALSE),
    "ggplot"
  )
  expect_s3_class(
    plot_journal_trends(fixture_journal_trends(), 4L, verbose = FALSE),
    "ggplot"
  )
  expect_s3_class(
    plot_keyword_evolution(fixture_keyword_trends(), 4L, verbose = FALSE),
    "ggplot"
  )
})

test_that("citation trends builds a two-panel patchwork", {
  skip_if_no_pkg("patchwork")
  p <- plot_citation_trends(fixture_annual_trends(), 1975L, 2024L, verbose = FALSE)
  expect_s3_class(p, "patchwork")
})

test_that("figure titles use URPS, not the retired FPMRS name", {
  titles <- c(
    plot_annual_publications(fixture_annual_trends(), 1975L, 2024L, verbose = FALSE)$labels$title,
    plot_country_contributions(fixture_country_trends(), 4L, verbose = FALSE)$labels$title,
    plot_journal_trends(fixture_journal_trends(), 4L, verbose = FALSE)$labels$title
  )
  expect_true(all(nchar(titles) > 0L))
  expect_false(any(grepl("FPMRS", titles, fixed = TRUE)))
  expect_true(any(grepl("URPS", titles, fixed = TRUE)))
})

test_that("country figure does not claim multi-country counting", {
  # The data is first-author country only (one row per article), so the
  # subtitle must not say "one count per article per country".
  p <- plot_country_contributions(fixture_country_trends(), 4L, verbose = FALSE)
  expect_false(
    grepl("per article per country", p$labels$subtitle, fixed = TRUE)
  )
  expect_match(p$labels$caption, "First-author", fixed = TRUE)
})

test_that("citation panel A is labelled by publication year, not receipt year", {
  skip_if_no_pkg("patchwork")
  p <- plot_citation_trends(fixture_annual_trends(), 1975L, 2024L, verbose = FALSE)
  panel_a_title <- p[[1]]$labels$title
  expect_match(panel_a_title, "Published", ignore.case = TRUE)
  expect_false(grepl("Citations Received per Year", panel_a_title, fixed = TRUE))
})

test_that("keyword subtitle has no dangling separator", {
  p <- plot_keyword_evolution(fixture_keyword_trends(), 4L, verbose = FALSE)
  expect_false(grepl("\\|\\s*$", p$labels$subtitle))
})

test_that("keyword caption does not define an unused ID field", {
  p <- plot_keyword_evolution(fixture_keyword_trends(), 4L, verbose = FALSE)
  expect_false(grepl("keyword-plus", p$labels$caption, fixed = TRUE))
})

# ---- Caption integrity --------------------------------------------------

expect_caption_intact <- function(plot, caption, width = 7, height = 5) {
  rendered <- render_pdf_text(plot, width = width, height = height)
  expect_true(
    grepl(normalise_text(caption), rendered, fixed = TRUE),
    info = paste0(
      "caption was clipped in the rendered PDF.\n  expected: ",
      normalise_text(caption), "\n  rendered: ", rendered
    )
  )
}

test_that("annual publications caption is not clipped", {
  p <- plot_annual_publications(fixture_annual_trends(), 1975L, 2024L, verbose = FALSE)
  expect_caption_intact(p, p$labels$caption)
})

test_that("country contributions caption is not clipped", {
  p <- plot_country_contributions(fixture_country_trends(), 4L, verbose = FALSE)
  expect_caption_intact(p, p$labels$caption)
})

test_that("keyword evolution caption is not clipped", {
  p <- plot_keyword_evolution(fixture_keyword_trends(), 4L, verbose = FALSE)
  expect_caption_intact(p, p$labels$caption)
})

test_that("the caption check actually detects clipping", {
  # Guard against the assertion silently passing for the wrong reason: an
  # unwrapped long caption must be caught.
  skip_if_no_pkg("pdftools")
  p <- plot_country_contributions(fixture_country_trends(), 4L, verbose = FALSE)
  overlong <- paste(
    "First-author affiliation country (OpenAlex). Articles without a",
    "resolvable first-author country are excluded, so annual totals fall",
    "below overall publication volume and should not be compared directly."
  )
  p_bad <- p + ggplot2::labs(caption = overlong)
  rendered <- render_pdf_text(p_bad)
  expect_false(grepl(normalise_text(overlong), rendered, fixed = TRUE))
})

test_that("y-axis breaks cover the tallest bar", {
  trends <- fixture_annual_trends()
  p <- plot_annual_publications(trends, 1975L, 2024L, verbose = FALSE)
  built <- ggplot2::ggplot_build(p)
  breaks <- built$layout$panel_params[[1L]]$y$breaks
  breaks <- breaks[!is.na(breaks)]
  expect_gte(max(breaks), max(trends$publication_count) * 0.9)
})

test_that("empty inputs return placeholder figures rather than erroring", {
  empty_kw <- fixture_keyword_trends()[0L, ]
  expect_s3_class(
    plot_keyword_evolution(empty_kw, 4L, verbose = FALSE),
    "ggplot"
  )
})

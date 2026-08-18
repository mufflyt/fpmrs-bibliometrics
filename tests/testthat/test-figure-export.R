# Figure export in multiple formats.
#
# Figures are needed as vector art for typesetting and as raster images for
# slides, email, and submission portals that reject PDFs, so the pipeline
# writes a primary format plus companions.

make_plot <- function() {
  plot_annual_publications(
    fixture_annual_trends(), 1975L, 2024L, verbose = FALSE
  )
}

# The base pdf device warns when it substitutes a non-Latin-1 glyph (the en
# dash in the subtitle). That is a device limitation, not a defect in the
# export logic under test, and it would otherwise fail a strict reporter.
save_figs <- function(...) suppressWarnings(.save_all_figures(...))

test_that("companion formats are written alongside the primary", {
  d <- withr_tempdir()
  paths <- save_figs(
    list(fig_one = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = "jpeg"
  )

  expect_true(file.exists(file.path(d, "fig_one.pdf")))
  expect_true(file.exists(file.path(d, "fig_one.jpeg")))
  expect_gt(file.size(file.path(d, "fig_one.jpeg")), 1000)
})

test_that("the return value stays the primary-format paths", {
  # Callers and the output manifest depend on this contract.
  d <- withr_tempdir()
  paths <- save_figs(
    list(fig_one = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = "jpeg"
  )
  expect_length(paths, 1L)
  expect_match(paths[[1L]], "[.]pdf$")
  expect_named(paths, "fig_one")
})

test_that("companion paths are recorded as an attribute", {
  d <- withr_tempdir()
  paths <- save_figs(
    list(a = make_plot(), b = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = "jpeg"
  )
  comp <- attr(paths, "companion_paths")
  expect_named(comp, "jpeg")
  expect_length(comp$jpeg, 2L)
  expect_true(all(grepl("[.]jpeg$", comp$jpeg)))
})

test_that("multiple companion formats are all written", {
  d <- withr_tempdir()
  save_figs(
    list(fig = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = c("jpeg", "png")
  )
  expect_true(file.exists(file.path(d, "fig.pdf")))
  expect_true(file.exists(file.path(d, "fig.jpeg")))
  expect_true(file.exists(file.path(d, "fig.png")))
})

test_that("a companion equal to the primary is not written twice", {
  d <- withr_tempdir()
  paths <- save_figs(
    list(fig = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = "pdf"
  )
  expect_length(attr(paths, "companion_paths"), 0L)
  expect_equal(length(list.files(d, pattern = "^fig[.]")), 1L)
})

test_that("no companion formats still writes the primary", {
  d <- withr_tempdir()
  save_figs(
    list(fig = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = character(0)
  )
  expect_equal(list.files(d), "fig.pdf")
})

test_that("an unsupported format is rejected with a helpful message", {
  d <- withr_tempdir()
  expect_error(
    .save_all_figures(
      list(fig = make_plot()), d, "pdf", 7, 5,
      verbose = FALSE, additional_formats = "bmp"
    ),
    "Unsupported figure format"
  )
})

test_that("JPEG is written on an opaque white background", {
  # JPEG cannot store transparency; without an explicit background the plot
  # area is composited onto black by some viewers.
  skip_if_no_pkg("jpeg")
  d <- withr_tempdir()
  save_figs(
    list(fig = make_plot()), d, "pdf", 7, 5,
    verbose = FALSE, additional_formats = "jpeg"
  )
  img <- jpeg::readJPEG(file.path(d, "fig.jpeg"))
  corner <- img[1:20, 1:20, ]
  expect_gt(mean(corner), 0.95)
})

test_that("raster formats get a white background, vector formats do not", {
  expect_equal(.figure_device_args("jpeg")$bg, "white")
  expect_equal(.figure_device_args("png")$bg, "white")
  expect_null(.figure_device_args("pdf")$bg)
  expect_null(.figure_device_args("svg")$bg)
})

test_that("JPEG quality is raised above the device default", {
  # The default (75) shows visible artefacts on thin plot lines at print size.
  expect_equal(.figure_device_args("jpeg")$quality, 95)
  expect_equal(.figure_device_args("jpg")$quality, 95)
  expect_null(.figure_device_args("png")$quality)
})

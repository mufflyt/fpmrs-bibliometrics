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

# ---- Journal submission formats ----------------------------------------
#
# Urogynecology requires line art as TIFF or EPS at >= 1200 dpi. JPEG is not
# an accepted submission format, and 300 dpi is the requirement for
# photographs, not line art.

test_that("TIFF is written at line-art resolution", {
  skip_if_no_pkg("tiff")
  d <- withr_tempdir()
  save_figs(list(fig = make_plot()), d, "tiff", 7, 5,
            verbose = FALSE, additional_formats = character(0))

  img <- tiff::readTIFF(file.path(d, "fig.tiff"))
  # 7 x 5 inches at 1200 dpi
  expect_equal(dim(img)[2], 7 * .LINE_ART_DPI)
  expect_equal(dim(img)[1], 5 * .LINE_ART_DPI)
})

test_that("TIFF uses lossless compression rather than none", {
  # Uncompressed 1200 dpi TIFF is ~150 MB per figure.
  expect_equal(.figure_device_args("tiff")$compression, "lzw")
  d <- withr_tempdir()
  save_figs(list(fig = make_plot()), d, "tiff", 7, 5,
            verbose = FALSE, additional_formats = character(0))
  expect_lt(file.size(file.path(d, "fig.tiff")), 20e6)
})

test_that("EPS is written as a vector file", {
  d <- withr_tempdir()
  save_figs(list(fig = make_plot()), d, "eps", 7, 5,
            verbose = FALSE, additional_formats = character(0))
  f <- file.path(d, "fig.eps")
  expect_true(file.exists(f))
  expect_match(readLines(f, n = 1L), "^%!PS")
})

test_that("raster preview formats stay at 300 dpi", {
  # JPEG/PNG are for email and slides, not submission, so they should not
  # carry the 1200 dpi line-art cost.
  skip_if_no_pkg("jpeg")
  d <- withr_tempdir()
  save_figs(list(fig = make_plot()), d, "jpeg", 7, 5,
            verbose = FALSE, additional_formats = character(0))
  img <- jpeg::readJPEG(file.path(d, "fig.jpeg"))
  expect_equal(dim(img)[2], 7 * 300)
})

test_that("submission formats can be produced alongside the working PDF", {
  d <- withr_tempdir()
  paths <- save_figs(list(fig = make_plot()), d, "pdf", 7, 5,
                     verbose = FALSE, additional_formats = c("tiff", "eps"))
  expect_true(all(file.exists(
    file.path(d, c("fig.pdf", "fig.tiff", "fig.eps"))
  )))
  expect_match(paths[[1L]], "[.]pdf$")
})

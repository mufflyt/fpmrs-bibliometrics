# FPMRS Bibliometrics Pipeline

Manuscript-ready longitudinal bibliometric analysis for Female Pelvic Medicine and Reconstructive Surgery (FPMRS) literature. Supports PubMed API, Web of Science, and Scopus data sources.

## Dependencies

Install required R packages:

```r
install.packages(c(
  "assertthat", "bibliometrix", "brms", "config", "cowplot",
  "digest", "dplyr", "fixest", "furrr", "future", "gender",
  "genderizeR", "geofacet", "ggplot2", "here", "httr",
  "jsonlite", "MASS", "Matrix", "mblm", "mgcv", "parallel",
  "patchwork", "pscl", "pubmedR", "purrr", "rentrez", "renv",
  "scales", "segmented", "sessioninfo", "sf", "statebins",
  "stringr", "survival", "tibble", "tidyr", "tigris", "tseries"
))
```

## Usage

```r
source("R/fpmrs_bibliometrics_pipeline.R")
```

See function documentation within the pipeline file for detailed usage of individual analysis steps.

## Author

Tyler Muffly, MD

## License

MIT

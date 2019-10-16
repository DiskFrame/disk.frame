context("test-dtplyr-verbs")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  tf = file.path(tempdir(), "test-dtplyr.df")
  as.disk.frame(b, outdir = tf, nchunks = 5, overwrite = TRUE)
})

test_that("testing dtplyr", {
  # TODO add tests when new version of dtplyr on CRAN
  # iris_df = as.disk.frame(iris)
  # 
  # iris_df %>% 
  #   filter(Sepal.Length > 7) %>% 
  #   collect()
  # 
  # 
  # aa = iris_df %>% 
  #   map(~{
  #     dtplyr::lazy_dt(.x) %>% 
  #       filter(Sepal.Length > 7) %>% 
  #       collect()
  #   }) %>% 
  #   collect
  # 
  # 
  # lazy_dt <- function(...) {
  #   UseMethod("lazy_dt")
  # }
  # 
  # lazy_dt.disk.frame <- function(df, ...) {
  #   map(df, )
  # }
  # 
  # lazy_dt.default <- function(...) {
  #   dtplyr::lazy_dt(...)
  # }
  expect_true(TRUE)
})



teardown({
  fs::dir_delete(file.path(tempdir(), "test-dtplyr.df"))
})


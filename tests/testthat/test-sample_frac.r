context("test-sampe_frac")

setup({
  a = data.frame(a = 1:100, b = 1:100)
  
  as.disk.frame(a, "tmp_sample_frac.df", nchunks = 5, overwrite = T)
})

test_that("testing sample_frac", {
  a = disk.frame("tmp_sample_frac.df")
  a40 <- sample_frac(a, 0.4) %>% collect
  
  expect_equal(nrow(a40), 40)
  
  expect_error(a40 <- sample_frac(a, 0.4, weight = 1) %>% collect)
})

teardown({
  fs::dir_delete("tmp_sample_frac.df")
})
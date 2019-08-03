context("test-sample_n")

setup({
  a = data.frame(a = 1:100, b = 1:100)
  
  as.disk.frame(a, "tmp_sample_n.df", nchunks = 5, overwrite = T)
})

test_that("testing semi_join where right is data.frame", {
  a = disk.frame("tmp_sample_n.df")
  expect_error(a40 <- sample_n(a, 40) %>% collect)
})

teardown({
  fs::dir_delete("tmp_sample_n.df")
})
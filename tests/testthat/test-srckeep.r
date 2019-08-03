context("test-keep")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, "tmp_srckeep.df", nchunks = 5, overwrite = T)
})

test_that("testing srckeep", {
  b = disk.frame("tmp_srckeep.df")
  b1 = b %>% srckeep("a")
  expect_equal(ncol(b1 %>% collect), 1)
  expect_equal(colnames(b1 %>% collect), "a")
})

teardown({
  fs::dir_delete("tmp_srckeep.df")
})
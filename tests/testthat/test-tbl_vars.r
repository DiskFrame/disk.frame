context("test-tbl_vars")

setup({
})

test_that("testing tbl_vars", {
  a = data.table(a = rep(1:10, 10), b = 1:100)
  a = shard(a, "a", nchunks = 2, overwrite = TRUE, outdir=file.path(tempdir(), "tmp_tbl_vars.df"))
  
  expect_setequal(tbl_vars(a), c("a","b"))
})


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_tbl_vars.df"))
})
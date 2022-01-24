context("test-shardkey")

setup({
})

test_that("testing shardkey", {
  set.seed(1)
  a = data.table(a = rep(1:10, 10), b = 1:100)
  a = shard(a, "a", nchunks = 2, overwrite = TRUE, outdir=file.path(tempdir(), "tmp_shardkey.df"))
  
  expect_equal(shardkey(a), list(shardkey="a", shardchunks=2))
})


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_shardkey.df"))
})
context("test-shard")

setup({
})

test_that("testing shard data.frame", {
  set.seed(1)
  a = data.table(a = rep(1:10, 10), b = 1:100)
  a = shard(a, "a", nchunks = 2, overwrite = T, outdir="tmp_shard.df")
  
  expect_equal(nchunks(a), 2)
  expect_equal(nrow(a), 100)
  expect_equal(ncol(a), 2)
  
  a1 = unique(get_chunk(a,1)$a)
  a2 = unique(get_chunk(a,2)$a)
  expect_equal(length(intersect(a1, a2)), 0)
  
  a3 = shard(a, "a", nchunks = 4, overwrite = T)
  
  expect_equal(nchunks(a3), 4)
  expect_equal(nrow(a3), 100)
})


teardown({
  fs::dir_delete("tmp_shard.df")
})
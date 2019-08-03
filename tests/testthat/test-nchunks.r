context("test-nchunks")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, "tmp_chunks.df", nchunks = 5, overwrite = T)
})

test_that("testing nchunks", {
  b = disk.frame("tmp_chunks.df")
  
  expect_equal(nchunks(b), 5)
  expect_equal(nchunk(b), 5)
})


teardown({
  fs::dir_delete("tmp_chunks.df")
})
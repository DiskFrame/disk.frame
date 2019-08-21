context("test-remove")

test_that("testing remove chunk 3 of 5", {
  b = data.frame(a = 51:150, b = 1:100)
  tmp = file.path(tempdir(), "tmp_remove.df")
  b = as.disk.frame(b, tmp, nchunks = 5, overwrite = T)
  
  b = remove_chunk(b, 3)
  expect_equal(nrow(b), 80)
  expect_equal(ncol(b), 2)
  expect_equal(nchunk(b), 4)
  
  res <- collect(b)[order(b)]
  
  expect_equal(nrow(res), 80)
  
  delete(b)
})

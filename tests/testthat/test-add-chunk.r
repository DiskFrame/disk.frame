context("test-add-chunk")

setup({
  setup_disk.frame(workers = 2)
})

test_that("guard against github 292", {
  a = data.frame(a = as.Date("2020-07-01"), b = runif(1e6))
  
  a.df = as.disk.frame(a)
  
  head(a.df)
  
  expect_s3_class(add_chunk(a.df, a), "disk.frame")
  delete(a.df)
})

test_that("testing add chunk without naming chunk_id", {
  a = data.frame(a = 1:100, b = 1:100)
  
  a1 = as.disk.frame(a, overwrite = TRUE)
  
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  add_chunk(a1, b)
  expect_equal(nrow(a1), 200)
  
  add_chunk(a1, d)
  expect_equal(nrow(a1), 250)
  
  delete(a1)
})

test_that("testing add chunk by naming chunk_id", {
  a = data.frame(a = 1:100, b = 1:100)
  
  a1 = as.disk.frame(a, overwrite = TRUE)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  add_chunk(a1, b, chunk_id = nchunks(a1)+2)
  expect_equal(nrow(a1), 200)
  
  add_chunk(a1, d, chunk_id = nchunks(a1)+2)
  expect_equal(nrow(a1), 250)
  
  delete(a1)
})

test_that("testing add chunk by using compression", {
  a = data.frame(a = 1:100, b = 1:100)
  
  a1 = as.disk.frame(a, overwrite = TRUE)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  add_chunk(a1, b, compress=50)
  expect_equal(nrow(a1), 200)
  
  delete(a1)
})

teardown({
})
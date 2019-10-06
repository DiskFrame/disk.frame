context("test-add-chunk")

setup({
  setup_disk.frame(workers = 2)
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

teardown({
})
context("test-add-chunk")

setup({
  setup_disk.frame(workers = 1)
})

test_that("testing add chunk without naming chunk_id", {
  a = data.frame(a = 1:100, b = 1:100)
  
  as.disk.frame(a, "tmp_a.df", overwrite = TRUE)
  as.disk.frame(a, "tmp_a1.df", overwrite = TRUE)
  
  a = disk.frame("tmp_a.df")
  
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  add_chunk(a, b)
  expect_equal(nrow(a), 200)
  
  add_chunk(a, d)
  expect_equal(nrow(a), 250)
  
  fs::dir_delete("tmp_a1.df")
})

test_that("testing add chunk by naming chunk_id", {
  a = data.frame(a = 1:100, b = 1:100)
  
  as.disk.frame(a, "tmp_a1.df", overwrite = T)
  
  a = disk.frame("tmp_a1.df")
  
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  add_chunk(a, b, chunk_id = nchunks(a)+2)
  expect_equal(nrow(a), 200)
  
  add_chunk(a, d, chunk_id = nchunks(a)+2)
  expect_equal(nrow(a), 250)
  
  fs::dir_delete("tmp_a1.df")
})

teardown({
  # fs::dir_delete("tmp_a.df")
  # fs::dir_delete("tmp_a1.df")
})
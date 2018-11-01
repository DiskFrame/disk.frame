context("test-inner_join")

setup({
  a = data.frame(a = 1:100, b = 1:100)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  as.disk.frame(a, "tmp_a_ij.df", nchunks = 4, overwrite = T)
  as.disk.frame(b, "tmp_b_ij.df", nchunks = 5, overwrite = T)
  as.disk.frame(d, "tmp_d_ij.df", overwrite = T)
})

test_that("testing inner_join where right is data.frame", {
  a = disk.frame("tmp_a_ij.df")
  b = disk.frame("tmp_b_ij.df")
  d = disk.frame("tmp_d_ij.df")
  bc = collect(b)
  dc = collect(d)
  
  abc = inner_join(a, bc, by = "a") %>% collect
  expect_equal(nrow(abc), 50)
  
  abc0 = inner_join(a, bc, by = c("a","b")) %>% collect
  expect_equal(nrow(abc0), 0)
  
  abc100 = inner_join(a, bc, by = "b") %>% collect
  expect_equal(nrow(abc100), 100)
  
  abd50 = inner_join(a, dc, by = "b") %>% collect
  expect_equal(nrow(abd50), 50)
})

test_that("testing inner_join where right is disk.frame", {
  a = disk.frame("tmp_a_ij.df")
  b = disk.frame("tmp_b_ij.df")
  d = disk.frame("tmp_d_ij.df")
  
  ab = inner_join(a, b, by = "a", merge_by_chunk_id = F) %>% collect
  expect_equal(nrow(ab), 50)
  
  ab0 = inner_join(a, b, by = c("a","b"), merge_by_chunk_id = F) %>% collect
  expect_equal(nrow(ab0), 0)
  
  ab100 = inner_join(a, b, by = "b", merge_by_chunk_id = F) %>% collect
  expect_equal(nrow(ab100), 100)
  
  ad50 = inner_join(a, d, by = "b", merge_by_chunk_id = F) %>% collect
  expect_equal(nrow(ad50), 50)
})


teardown({
  fs::dir_delete("tmp_a_ij.df")
  fs::dir_delete("tmp_b_ij.df")
  fs::dir_delete("tmp_d_ij.df")
})
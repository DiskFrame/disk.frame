context("test-left_join")

setup({
  a = data.frame(a = 1:100, b = 1:100)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  as.disk.frame(a, "tmp_a.df", nchunks = 4, overwrite = T)
  as.disk.frame(b, "tmp_b.df", nchunks = 5, overwrite = T)
  as.disk.frame(d, "tmp_d.df", overwrite = T)
})

test_that("testing left_join where right is data.frame", {
  a = disk.frame("tmp_a.df")
  b = disk.frame("tmp_b.df")
  d = disk.frame("tmp_d.df")
  bc = collect(b)
  dc = collect(d)
  
  abc = left_join(a, bc, by = "a") %>% collect
  expect_equal(nrow(abc), 100)
  
  abc0 = left_join(a, bc, by = c("a","b")) %>% collect
  expect_equal(nrow(abc0), 100)
  
  abc100 = left_join(a, bc, by = "b") %>% collect
  expect_equal(nrow(abc100), 100)
  
  abd50 = left_join(a, dc, by = "b") %>% collect
  expect_equal(nrow(abd50), 100)
})

test_that("testing left_join where right is disk.frame", {
  a = disk.frame("tmp_a.df")
  b = disk.frame("tmp_b.df")
  d = disk.frame("tmp_d.df")
  
  expect_warning({ab = left_join(a, b, by = "a", merge_by_chunk_id = F) %>% collect})
  expect_equal(nrow(ab), 100)
  
  expect_warning({ab0 = left_join(a, b, by = c("a","b"), merge_by_chunk_id = F) %>% collect})
  expect_equal(nrow(ab0), 100)
  
  expect_warning({ab100 = left_join(a, b, by = "b", merge_by_chunk_id = F) %>% collect})
  expect_equal(nrow(ab100), 100)
  
  expect_warning({ad50 = left_join(a, d, by = "b", merge_by_chunk_id = F) %>% collect})
  expect_equal(nrow(ad50), 100)
})


teardown({
  fs::dir_delete("tmp_a.df")
  fs::dir_delete("tmp_b.df")
  fs::dir_delete("tmp_d.df")
})
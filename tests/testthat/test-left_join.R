context("test-left_join")

setup({
  setup_disk.frame(2)
  a = data.frame(a = 1:100, b = 1:100)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  as.disk.frame(a, file.path(tempdir(), "tmp_a_lj.df"), nchunks = 4, overwrite = T)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_lj.df"), nchunks = 5, overwrite = T)
  as.disk.frame(d, file.path(tempdir(), "tmp_d_lj.df"), overwrite = T)

  as.disk.frame(a, file.path(tempdir(), "tmp_a_lj2.df"), nchunks = 4, overwrite = T)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_lj2.df"), nchunks = 5, overwrite = T)
  as.disk.frame(d, file.path(tempdir(), "tmp_d_lj2.df"), overwrite = T)
})

test_that("testing left_join where right is data.frame", {
  a = disk.frame(file.path(tempdir(), "tmp_a_lj.df"))
  b = disk.frame(file.path(tempdir(), "tmp_b_lj.df"))
  d = disk.frame(file.path(tempdir(), "tmp_d_lj.df"))
  bc = collect(b)
  dc = collect(d)
  
  abc = left_join(a, bc, by = "a") %>% collect
  expect_equal(nrow(abc), 100)
  
  abc0 = left_join(a, bc, by = c("a","b")) %>% collect
  expect_equal(nrow(abc0), 100)
  
  by_cols = c("a","b")
  abc0 = left_join(a, bc, by = by_cols) %>% collect
  expect_equal(nrow(abc0), 100)
  
  abc100 = left_join(a, bc, by = "b") %>% collect
  expect_equal(nrow(abc100), 100)
  
  abd50 = left_join(a, dc, by = "b") %>% collect
  expect_equal(nrow(abd50), 100)
})

test_that("testing left_join where right is disk.frame", {
  a = disk.frame(file.path(tempdir(), "tmp_a_lj2.df"))
  b = disk.frame(file.path(tempdir(), "tmp_b_lj2.df"))
  d = disk.frame(file.path(tempdir(), "tmp_d_lj2.df"))
  
  expect_warning({
    ab = left_join(a, b, by = "a", merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ab), 100)
  
  expect_warning({
    ab0 = left_join(a, b, by = c("a","b"), merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ab0), 100)
  
  expect_warning({
    ab100 = left_join(a, b, by = "b", merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ab100), 100)
  
  expect_warning({
    ad50 = left_join(a, d, by = "b", merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ad50), 100)
})

teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_a_lj.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_b_lj.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_d_lj.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_a_lj2.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_b_lj2.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_d_lj2.df"))
})
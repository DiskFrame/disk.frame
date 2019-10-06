context("test-semi_join")

setup({
  
  a = data.frame(a = 1:100, b = 1:100)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)
  
  as.disk.frame(a, file.path(tempdir(), "tmp_a_sj.df"), nchunks = 4, overwrite = TRUE)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_sj.df"), nchunks = 5, overwrite = TRUE)
  as.disk.frame(d, file.path(tempdir(), "tmp_d_sj.df"), overwrite = TRUE)

  as.disk.frame(a, file.path(tempdir(), "tmp_a_sj2.df"), nchunks = 4, overwrite = TRUE)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_sj2.df"), nchunks = 5, overwrite = TRUE)
  as.disk.frame(d, file.path(tempdir(), "tmp_d_sj2.df"), overwrite = TRUE)
})

test_that("testing semi_join where right is data.frame", {
  a = disk.frame(file.path(tempdir(), "tmp_a_sj.df"))
  b = disk.frame(file.path(tempdir(), "tmp_b_sj.df"))
  d = disk.frame(file.path(tempdir(), "tmp_d_sj.df"))
  bc = collect(b)
  dc = collect(d)
  
  abc = semi_join(a, bc, by = "a") %>% collect
  expect_equal(nrow(abc), 50)
  
  abc0 = semi_join(a, bc, by = c("a","b")) %>% collect
  expect_equal(nrow(abc0), 0)
  
  abc100 = semi_join(a, bc, by = "b") %>% collect
  expect_equal(nrow(abc100), 100)
  
  abd50 = semi_join(a, dc, by = "b") %>% collect
  expect_equal(nrow(abd50), 50)
})

test_that("testing semi_join where right is disk.frame", {
  a = disk.frame(file.path(tempdir(), "tmp_a_sj2.df"))
  b = disk.frame(file.path(tempdir(), "tmp_b_sj2.df"))
  d = disk.frame(file.path(tempdir(), "tmp_d_sj2.df"))
  
  expect_warning({
    ab = semi_join(a, b, by = "a", merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ab), 50)
  
  expect_warning({
    ab0 = semi_join(a, b, by = c("a","b"), merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ab0), 0)
  
  expect_warning({
    ab100 = semi_join(a, b, by = "b", merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ab100), 100)
  
  expect_warning({
    ad50 = semi_join(a, d, by = "b", merge_by_chunk_id = F) %>% collect
    })
  expect_equal(nrow(ad50), 50)
})

teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_a_sj.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_b_sj.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_d_sj.df"))
  
  fs::dir_delete(file.path(tempdir(), "tmp_a_sj2.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_b_sj2.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_d_sj2.df"))
})
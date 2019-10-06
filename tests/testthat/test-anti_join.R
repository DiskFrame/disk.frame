context("test-anti_join")

setup({
  setup_disk.frame(workers = 2)
  
  a = data.frame(a = 1:100, b = 1:100)
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 1:50, b = 1:50)

  as.disk.frame(a, file.path(tempdir(), "tmp_a_aj.df"), nchunks = 4, overwrite = TRUE)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_aj.df"), nchunks = 5, overwrite = TRUE)
  as.disk.frame(d, file.path(tempdir(), "tmp_d_aj.df"), overwrite = TRUE)

  as.disk.frame(a, file.path(tempdir(), "tmp_a_aj2.df"), nchunks = 4, overwrite = TRUE)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_aj2.df"), nchunks = 5, overwrite = TRUE)
  as.disk.frame(d, file.path(tempdir(), "tmp_d_aj2.df"), overwrite = TRUE)
})

test_that("testing anti_join where right is data.frame", {
  #skip_on_cran()
  a = disk.frame(file.path(tempdir(), "tmp_a_aj.df"))
  b = disk.frame(file.path(tempdir(), "tmp_b_aj.df"))
  d = disk.frame(file.path(tempdir(), "tmp_d_aj.df"))
  bc = collect(b)
  dc = collect(d)
  
  abc = anti_join(a, bc, by = "a") %>% collect
  expect_equal(nrow(abc), 50)
  
  abc0 = anti_join(a, bc, by = c("a","b")) %>% collect
  expect_equal(nrow(abc0), 100)
  
  abc100 = anti_join(a, bc, by = "b") %>% collect
  expect_equal(nrow(abc100), 0)
  
  abd50 = anti_join(a, dc, by = "b") %>% collect
  expect_equal(nrow(abd50), 50)
})

test_that("testing anti_join where right is disk.frame", {
  #skip_on_cran()
  a = disk.frame(file.path(tempdir(),"tmp_a_aj2.df"))
  b = disk.frame(file.path(tempdir(),"tmp_b_aj2.df"))
  d = disk.frame(file.path(tempdir(),"tmp_d_aj2.df"))
  
  expect_warning({
    ab <- anti_join(a, b, by = "a", merge_by_chunk_id = FALSE) %>% collect
    })
  expect_equal(nrow(ab), 50)
  
  expect_warning({
    ab0 = anti_join(a, b, by = c("a","b"), merge_by_chunk_id = FALSE) %>% collect
    })
  expect_equal(nrow(ab0), 100)
  
  expect_warning({
    ab100 = anti_join(a, b, by = "b", merge_by_chunk_id = FALSE) %>% collect
    })
  expect_equal(nrow(ab100), 0)
  
  expect_warning({
    ad50 = anti_join(a, d, by = "b", merge_by_chunk_id = FALSE) %>% collect
    })
  expect_equal(nrow(ad50), 50)
})

teardown({
  fs::dir_delete(file.path(tempdir(),"tmp_a_aj.df"))
  fs::dir_delete(file.path(tempdir(),"tmp_b_aj.df"))
  fs::dir_delete(file.path(tempdir(),"tmp_d_aj.df"))

  fs::dir_delete(file.path(tempdir(),"tmp_a_aj2.df"))
  fs::dir_delete(file.path(tempdir(),"tmp_b_aj2.df"))
  fs::dir_delete(file.path(tempdir(),"tmp_d_aj2.df"))
})
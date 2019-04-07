context("test-map")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, "tmp_map.df", nchunks = 5, overwrite = T)
})

test_that("testing map lazy", {
  b = disk.frame("tmp_map.df")
  
  # return 1 row from each chunk
  df = b %>% map(~.x[1])
  
  expect_s3_class(df, "disk.frame")
  
  df2 = df %>% collect
  
  expect_s3_class(df2, "data.frame")
  
  expect_equal(nrow(df2), 5L)
})

test_that("testing map eager", {
  b = disk.frame("tmp_map.df")
  
  # return 1 row from each chunk
  df = b %>% map(~.x[1], lazy = F)
  expect_false("disk.frame" %in% class(df))

  # return 1 row from each chunk
  df = b %>% map_dfr(~.x[1], lazy = F)
  expect_false("disk.frame" %in% class(df))
  expect_true("data.frame" %in% class(df))
})

test_that("testing delayed", {
  b = disk.frame("tmp_map.df")
  
  # return 1 row from each chunk
  df = b %>% delayed(~.x[1])
  
  expect_s3_class(df, "disk.frame")
  
  df2 = df %>% collect
  
  expect_s3_class(df2, "data.frame")
  
  expect_equal(nrow(df2), 5L)
})

# TODO rest of the functions 

teardown({
  fs::dir_delete("tmp_map.df")
})
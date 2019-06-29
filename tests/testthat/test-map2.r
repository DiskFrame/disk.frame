context("test-map2")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  d = data.frame(a = 151:250, b = 1:100)
  as.disk.frame(b, "tmp_map2.df", nchunks = 5, overwrite = T)
  as.disk.frame(d, "tmp_map2d.df", nchunks = 5, overwrite = T)
})

test_that("testing map2 .y is disk.frame", {
  b = disk.frame("tmp_map2.df")
  d = disk.frame("tmp_map2d.df")
  
  # return 1 row from each chunk
  df = map2(b,d,~rbindlist(list(.x[1,],.y[1,])), outdir = "tmp_map2_out.df")
  
  expect_s3_class(df, "disk.frame")
  
  df2 = df %>% collect
  
  expect_s3_class(df2, "data.frame")
  
  expect_equal(nrow(df2), 10L)
})

test_that("testing map2 .y is not disk.frame", {
  b = disk.frame("tmp_map2.df")
  d = 1:nchunks(b)
  
  # return 1 row from each chunk
  df = map2(b,d,~.x[1,.(y = .y)], outdir = "tmp_map2_out2.df")
  
  expect_type(df, "list")
  
  df2 = df %>% rbindlist
  
  expect_s3_class(df2, "data.frame")
  
  expect_equal(nrow(df2), 5L)
})

teardown({
  fs::dir_delete("tmp_map2.df")
  fs::dir_delete("tmp_map2d.df")
  fs::dir_delete("tmp_map2_out.df")
})
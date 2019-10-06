context("test-map")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_map.df"), nchunks = 5, overwrite = T)
})

test_that("testing map lazy", {
  b = disk.frame(file.path(tempdir(), "tmp_map.df"))
  
  # return 1 row from each chunk
  df = b %>% map(~.x[1])
  
  expect_s3_class(df, "disk.frame")
  
  df2 = df %>% collect
  
  expect_s3_class(df2, "data.frame")
  
  expect_equal(nrow(df2), 5L)
})

test_that("testing map eager", {
  b = disk.frame(file.path(tempdir(), "tmp_map.df"))
  
  # return 1 row from each chunk
  df = b %>% map(~.x[1], lazy = F)
  expect_false("disk.frame" %in% class(df))

  # return 1 row from each chunk
  df = b %>% map_dfr(~.x[1])
  expect_false("disk.frame" %in% class(df))
  expect_true("data.frame" %in% class(df))
})

test_that("testing delayed", {
  b = disk.frame(file.path(tempdir(), "tmp_map.df"))
  
  # return 1 row from each chunk
  df = b %>% delayed(~.x[1])
  
  expect_s3_class(df, "disk.frame")
  
  df1 = collect(df)
  
  expect_equal(nrow(df1), 5)
})


test_that("testing map_dfr", {
  b = disk.frame(file.path(tempdir(), "tmp_map.df"))
  
  # return 1 row from each chunk
  df = b %>% map_dfr(~.x[1,])
  
  expect_s3_class(df, "data.frame")
})


test_that("testing imap", {
  b = disk.frame(file.path(tempdir(), "tmp_map.df"))
  
  # return 1 row from each chunk
  df = b %>% imap_dfr(~{
    y = .x[1,]
    y[,ok := .y]
    y
    })
  
  expect_s3_class(df, "data.frame")
})


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_map.df"))
})
context("test-compute")

setup({
  #setup_disk.frame(workers = 1)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_col_delete", overwrite=T)
})

test_that("compute works on simple data", {
  df = disk.frame("tmp_col_delete")
  dff = compute(df)
  
  expect_equal(nrow(dff), 1e5+11)
  expect_s3_class(dff, "disk.frame")
})

test_that("compute works on lazy stream", {
  df = disk.frame("tmp_col_delete")
  df = map(df, lazy = T, ~{
    .x[1:10, ]
  })
  dff = compute(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_s3_class(dff, "disk.frame")
})

test_that("compute works on lazy stream followed by dplyr", {
  df = disk.frame("tmp_col_delete")
  df = map(df, lazy = T, ~{
    .x[1:10, ]
  }) %>% select(id1, id4)
  
  dff = compute(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_equal(ncol(dff), 2)
  expect_s3_class(dff, "disk.frame")
})


test_that("compute works on dplyr::select followed by lazy", {
  df = disk.frame("tmp_col_delete")
  df = df %>% select(id1, id4) %>%
    map(lazy = T, ~{
      .x[1:10, ]
    })
  
  dff = dplyr::collect(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_equal(ncol(dff), 2)
  expect_s3_class(dff, "data.frame")
})


teardown({
  fs::dir_delete("tmp_col_delete")
})
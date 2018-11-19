context("test-collect")

setup({
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_pls_delete", overwrite=T)
})

test_that("collect works on simple data", {
  df = disk.frame("tmp_pls_delete")
  dff = collect(df)
  expect_equal(nrow(dff), 1e5+11)
  expect_s3_class(dff, "data.frame")
  expect_s3_class(dff, "data.table")
})

test_that("collect works on lazy stream", {
  df = disk.frame("tmp_pls_delete")
  df = map.disk.frame(df, lazy = T, ~{
    .x[1:10, ]
  })
  dff = collect(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_s3_class(dff, "data.frame")
  expect_s3_class(dff, "data.table")
})

test_that("collect works on lazy stream followed by dplyr", {
  df = disk.frame("tmp_pls_delete")
  df = map.disk.frame(df, lazy = T, ~{
    .x[1:10, ]
  }) %>% select(id1, id4)
  
  dff = collect(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_equal(ncol(dff), 2)
  expect_s3_class(dff, "data.frame")
  expect_s3_class(dff, "data.table")
})


test_that("collect works on dplyr::select followed by lazy", {
  df = disk.frame("tmp_pls_delete")
  df = df %>% select(id1, id4) %>%
    map.disk.frame(lazy = T, ~{
      .x[1:10, ]
    })
  
  dff = collect(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_equal(ncol(dff), 2)
  expect_s3_class(dff, "data.frame")
  expect_s3_class(dff, "data.table")
})


teardown({
  #fs::dir_delete("tmp_pls_delete")
})
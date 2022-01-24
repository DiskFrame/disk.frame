context("test-compute")

setup({
  setup_disk.frame(workers = 2)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(),"tmp_compute_delete"), overwrite=T)
})

test_that("compute works on simple data", {
  df = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  dff = compute(df)
  
  expect_equal(nrow(dff), 1e5+11)
  expect_s3_class(dff, "disk.frame")
})

test_that("compute works on lazy stream", {
  df = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  df = cmap(df, lazy = T, ~{
    .x[1:10, ]
  })
  dff = compute(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_s3_class(dff, "disk.frame")
})

test_that("compute works on lazy stream followed by dplyr", {
  df = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  df = cmap(df, lazy = T, ~{
    .x[1:10, ]
  }) %>% select(id1, id4)
  
  dff = compute(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_equal(ncol(dff), 2)
  expect_s3_class(dff, "disk.frame")
})


test_that("compute works on dplyr::select followed by lazy", {
  df = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  df = df %>% select(id1, id4) %>%
    cmap(lazy = T, ~{
      .x[1:10, ]
    })
  
  dff = dplyr::collect(df)
  expect_equal(nrow(dff), nchunks(df)*10)
  expect_equal(ncol(dff), 2)
  expect_s3_class(dff, "data.frame")
})


teardown({
  fs::dir_delete(file.path(tempdir(),"tmp_compute_delete"))
})
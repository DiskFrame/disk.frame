context("test-compute")

setup({
  setup_disk.frame(workers = 2)
  diskf = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(),"tmp_compute_delete"), overwrite=T)
})

test_that("compute works on simple data", {
  diskf = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  diskff = compute(diskf)
  
  expect_equal(nrow(diskff), 1e5+11)
  expect_s3_class(diskff, "disk.frame")
})

test_that("compute works on lazy stream", {
  diskf = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  diskf = cmap(diskf, lazy = T, ~{
    .x[1:10, ]
  })
  diskff = compute(diskf)
  expect_equal(nrow(diskff), nchunks(diskf)*10)
  expect_s3_class(diskff, "disk.frame")
})

test_that("compute works on lazy stream followed by dplyr", {
  diskf = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  diskf = cmap(diskf, lazy = T, ~{
    .x[1:10, ]
  }) %>% select(id1, id4)
  
  diskff = compute(diskf)
  expect_equal(nrow(diskff), nchunks(diskf)*10)
  expect_equal(ncol(diskff), 2)
  expect_s3_class(diskff, "disk.frame")
})


test_that("compute works on dplyr::select followed by lazy", {
  diskf = disk.frame(file.path(tempdir(),"tmp_compute_delete"))
  diskf = diskf %>% select(id1, id4) %>%
    cmap(lazy = T, ~{
      .x[1:10, ]
    })
  
  diskff = dplyr::collect(diskf)
  expect_equal(nrow(diskff), nchunks(diskf)*10)
  expect_equal(ncol(diskff), 2)
  expect_s3_class(diskff, "data.frame")
})


teardown({
  fs::dir_delete(file.path(tempdir(),"tmp_compute_delete"))
})
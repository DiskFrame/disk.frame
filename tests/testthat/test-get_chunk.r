context("test-get_chunk")

setup({
  #setup_disk.frame(workers = 1)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_del_delete"), overwrite=T)
})

test_that("data.table .N", {
  df = disk.frame(file.path(tempdir(), "tmp_del_delete"))
  expect_s3_class(get_chunk(df, 1), "data.frame")

  expect_s3_class(get_chunk(df, "1.fst"), "data.frame")
})

teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_del_delete"))
})
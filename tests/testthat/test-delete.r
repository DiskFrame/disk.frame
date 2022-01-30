context("test-delete")

setup({
  setup_disk.frame(workers = 2)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_del_delete"), overwrite = TRUE)
})

test_that("data.table .N", {
  df = disk.frame(file.path(tempdir(), "tmp_del_delete"))
  p = attr(df, "path", exact=TRUE)
  expect_true(fs::dir_exists(p))
  
  delete(df)
  
  expect_false(fs::dir_exists(p))
})

teardown({
  #fs::dir_delete("tmp_del_delete")
})
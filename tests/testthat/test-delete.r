context("test-delete")

setup({
  #setup_disk.frame(workers = 1)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_del_delete", overwrite=T)
})

test_that("data.table .N", {
  df = disk.frame("tmp_del_delete")
  p = attr(df, "path")
  expect_true(fs::dir_exists(p))
  
  delete(df)
  
  expect_false(fs::dir_exists(p))
})

teardown({
  #fs::dir_delete("tmp_del_delete")
})
context("test-data.table [")

setup({
  setup_disk.frame(workers = 1)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_col_delete", overwrite=T)
})

test_that("data.table .N", {
  df = disk.frame("tmp_col_delete")
  expect_equal(sum(df[,.N]), 1e5+11)
})

teardown({
  fs::dir_delete("tmp_col_delete")
})
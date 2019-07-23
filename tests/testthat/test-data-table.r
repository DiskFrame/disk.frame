context("test-data.table [")

setup({
  setup_disk.frame(workers = 2)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_col_delete", overwrite=T, nchunks = 8)
})

test_that("data.table .N", {
  df = disk.frame("tmp_col_delete")
  expect_equal(sum(unlist(df[,.N])), 1e5+11)
})

# test_that("data.table .N+y V1", {
#   df = disk.frame("tmp_col_delete")
#   y = 2 
#   expect_equal(sum(unlist(df[,.N + y, v1])), 1e5+11)
# })

teardown({
  fs::dir_delete("tmp_col_delete")
})
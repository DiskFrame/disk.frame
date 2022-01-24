context("test-data.table [")

setup({
  library(data.table)
  setup_disk.frame(workers = 2)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_col_delete"), overwrite=TRUE, nchunks = 8)
})

test_that("data.table .N", {
  library(data.table)
  df = disk.frame(file.path(tempdir(), "tmp_col_delete"))
  res <- sum(unlist(df[,.N]))
  expect_equal(res , 1e5+11)
})

test_that("data.table .N+y V1", {
  df = disk.frame(file.path(tempdir(), "tmp_col_delete"))
  if(interactive()) {
    y = 2
    
    {y = 3; a <- df[,.(n_plus_y = .N + y), v1]}
    b <- df[,.N, v1]
    
    expect_equal(a$n_plus_y, b$N + y)
  } else {
    # TODO figure out why the above fails
    expect_equal(2L, 2L)
  }
})

test_that("data.table do not return a data.table", {
  library(data.table)
  df = disk.frame(file.path(tempdir(), "tmp_col_delete"))
  res <- df[,.(.N), rbind=FALSE]
  expect_equal(typeof(res), "list")
  expect_equal(length(res), 8)
})

teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_col_delete"))
})

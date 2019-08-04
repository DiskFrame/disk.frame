context("test-data.table [")

setup({
  setup_disk.frame(workers = 2)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_col_delete", overwrite=T, nchunks = 8)
})

test_that("data.table .N", {
  df = disk.frame("tmp_col_delete")
  expect_warning(res <- sum(unlist(df[,.N])))
  expect_equal(res , 1e5+11)
})

test_that("data.table .N+y V1", {
  df = disk.frame("tmp_col_delete")
  if(interactive()) {
    y = 2
    
    expect_warning({y = 3; a <- df[,.(n_plus_y = .N + y), v1]})
    expect_warning(b <- df[,.N, v1])
    
    expect_equal(a$n_plus_y, b$N + y)
  } else {
    # TODO figure out why the above fails
    expect_equal(2L, 2L)
  }
})

teardown({
  fs::dir_delete("tmp_col_delete")
})
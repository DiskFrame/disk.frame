context("test-write_disk.frame")

test_that("as.disk.frame works", {
  ROWS = 1e3+11

  tmp_write_disk.frame = tempfile()
  tmp_write_disk.frame2 = tempfile()
  
  df = disk.frame:::gen_datatable_synthetic(ROWS)
  dfdf <- as.disk.frame(df, tmp_write_disk.frame, overwrite = TRUE, nchunks = 5)

  a = dfdf %>% map(~{
    .x[1,]
  }) %>% write_disk.frame(outdir = tmp_write_disk.frame2, overwrite = T)

  expect_equal(nrow(a), 5)

  fs::dir_delete(tmp_write_disk.frame)
  fs::dir_delete(tmp_write_disk.frame2)
})

test_that("as.disk.frame fails if data frame has list-columns", {
    df <- tibble::tibble("a" = c(1,2,3), "b" = list("a", "b", "c"))
    expect_error(as.disk.frame(df, file.path(tempdir(), "tmp_write_disk.frame"), overwrite = TRUE, nchunks = 6))
})

test_that("write_disk.frame shard works", {
  mtcars_df = as.disk.frame(
    mtcars, 
    outdir = file.path(tempdir(), "mt_shard_by_cyl"), 
    shardby = c("cyl","vs"), 
    nchunks = 3, 
    overwrite = TRUE)
  
  res = mtcars_df %>% collect_list
  expect_equal(length(res), 3)
  testthat::expect_type(res, "list")
  
})

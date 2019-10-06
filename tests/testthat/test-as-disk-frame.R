context("test-as-disk-frame")

test_that("as.disk.frame works", {
  ROWS = 1e5+11
  
  df = disk.frame:::gen_datatable_synthetic(ROWS)
  tf = file.path(tempdir(), "tmp_as_disk_frame_delete")
  
  dfdf <- as.disk.frame(df, outdir = tf, overwrite=TRUE)
  
  expect_equal(nrow(dfdf), ROWS)
  expect_error(dfdf <- as.disk.frame(df, tf, overwrite=FALSE))
  
  delete(dfdf)
})

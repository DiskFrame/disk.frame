context("test-as-disk-frame")

test_that("as.disk.frame works", {
  ROWS = 1e5+11
  
  df = disk.frame:::gen_datatable_synthetic(ROWS)
  dfdf <- as.disk.frame(df, "tmp_as_disk_frame_delete", overwrite=T)
  
  expect_equal(nrow(dfdf), ROWS)
  expect_error(dfdf <- as.disk.frame(df, "tmp_as_disk_frame_delete", overwrite=F))
  
  fs::dir_delete("tmp_as_disk_frame_delete")
})

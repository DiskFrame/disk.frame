context("test-write_disk.frame")

test_that("as.disk.frame works", {
  ROWS = 1e3+11
  
  df = disk.frame:::gen_datatable_synthetic(ROWS)
  dfdf <- as.disk.frame(df, "tmp_write_disk.frame", overwrite=T, nchunks = 5)
  
  a = dfdf %>% map(~{
    .x[1,]
  }) %>% write_disk.frame(outdir = "tmp_write_disk.frame2", overwrite = T)
  
  expect_equal(nrow(a), 5)
  
  fs::dir_delete("tmp_write_disk.frame")
  fs::dir_delete("tmp_write_disk.frame2")
})

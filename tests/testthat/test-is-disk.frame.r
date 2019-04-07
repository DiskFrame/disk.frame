context("test-is-disk-frame")

test_that("testing is_disk.frame", {
  fs::dir_create("tmp_is_disk_frame")
  fst::write_fst(data.frame(a= 1, b = 1), "tmp_is_disk_frame/1.fst")
  fst::write_fst(data.frame(a= 1, b = 1), "tmp_is_disk_frame/2.fst")
  
  df = disk.frame("tmp_is_disk_frame")
  expect_true(is_disk.frame(df))
  
  disk.frame::delete(df)
})

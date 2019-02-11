context("test-as-data-frame")

test_that("as.data.frame works", {
  tmpdir = tempfile("disk.frame.tmp")
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11, 100), tmpdir, overwrite = T)
  dff = as.data.frame(df)
  dft = data.table::as.data.table(df)
  expect_s3_class(dff, "data.frame")
  expect_s3_class(dft, "data.table")
  expect_equal(nrow(dff), 1e5+11)
})


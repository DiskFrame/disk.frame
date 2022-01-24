context("test-overwrite_check")

setup({
})

test_that("testing overwrite_check", {
  b = data.frame(a = 51:150, b = 1:100)
  
  fs::dir_create(file.path(tempdir(), "tmp_overwrite-check"))
  fs::file_create(file.path(tempdir(), "tmp_overwrite-check/tmp"))
  
  
  expect_error(disk.frame::overwrite_check(file.path(tempdir(), "tmp_overwrite-check"), overwrite = TRUE))
  
  expect_error(disk.frame::overwrite_check(file.path(tempdir(), "tmp_overwrite-check"), overwrite = FALSE))
})


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_overwrite-check"))
})
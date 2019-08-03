context("test-overwrite_check")

setup({
})

test_that("testing overwrite_check", {
  b = data.frame(a = 51:150, b = 1:100)
  
  fs::dir_create("tmp_overwrite-check")
  fs::file_create("tmp_overwrite-check/tmp")
  
  
  expect_error(disk.frame::overwrite_check("tmp_overwrite-check", overwrite = T))
  
  expect_error(disk.frame::overwrite_check("tmp_overwrite-check", overwrite = F))
})


teardown({
  fs::dir_delete("tmp_overwrite-check")
})
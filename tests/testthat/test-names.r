context("test-names")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_names.df"), nchunks = 5, overwrite = T)
})

test_that("testing names", {
  b = disk.frame(file.path(tempdir(), "tmp_names.df"))
  
  expect_setequal(colnames(b), c("a","b"))
  expect_setequal(names(b), c("a","b"))
})

test_that("testing names with lazyfn", {
  b = disk.frame(file.path(tempdir(), "tmp_names.df")) %>% 
    mutate(d = a + b)
  
  expect_setequal(colnames(b), c("a","b", "d"))
  expect_setequal(names(b), c("a","b", "d"))
})

teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_names.df"))
})
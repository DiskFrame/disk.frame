context("test-rechunk")

setup({
})

test_that("testing rechunk 5 to 4", {
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_rechunks.df"), nchunks = 5, overwrite = T)

  b = disk.frame(file.path(tempdir(), "tmp_rechunks.df"))
  
  b = rechunk(b, 4)
  expect_equal(nrow(b), 100)
  expect_equal(ncol(b), 2)
  expect_equal(nchunk(b), 4)
  
  res = collect(b)[order(b)]
  
  expect_equal(res$b, 1:100)
  expect_equal(res$a, 51:150)
})

test_that("testing rechunk 5 to 3", {
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_rechunks2.df"), nchunks = 5, overwrite = T)
  
  b = disk.frame(file.path(tempdir(), "tmp_rechunks2.df"))
  
  b = rechunk(b, 3)
  expect_equal(nrow(b), 100)
  expect_equal(ncol(b), 2)
  expect_equal(nchunk(b), 3)
  
  res = collect(b)[order(b)]
  
  expect_equal(res$b, 1:100)
  expect_equal(res$a, 51:150)
})

test_that("testing rechunk 5 to 6", {
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_rechunks3.df"), nchunks = 5, overwrite = T)
  
  b = disk.frame(file.path(tempdir(), "tmp_rechunks3.df"))
  
  b = rechunk(b, 6)
  expect_equal(nrow(b), 100)
  expect_equal(ncol(b), 2)
  expect_equal(nchunk(b), 6)
  
  res = collect(b)[order(b)]
  
  expect_equal(res$b, 1:100)
  expect_equal(res$a, 51:150)
})

test_that("testing rechunk 5 to 7", {
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_rechunks4.df"), nchunks = 5, overwrite = T)
  
  b = disk.frame(file.path(tempdir(), "tmp_rechunks4.df"))
  
  b = rechunk(b, 7)
  expect_equal(nrow(b), 100)
  expect_equal(ncol(b), 2)
  expect_equal(nchunk(b), 7)
  
  res = collect(b)[order(b)]
  
  expect_equal(res$b, 1:100)
  expect_equal(res$a, 51:150)
})

# TODO do shardby; it's kinda of mitigated by thorough testing on Fannie Mae


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_rechunks.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rechunks2.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rechunks3.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rechunks4.df"))
})

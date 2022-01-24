context("test-rbindlist")

setup({
  as.disk.frame(disk.frame:::gen_datatable_synthetic(1e3+11), file.path(tempdir(), "tmp_rbindlist1.df"), overwrite=TRUE)
  as.disk.frame(disk.frame:::gen_datatable_synthetic(1e3+11), file.path(tempdir(), "tmp_rbindlist2.df"), overwrite=TRUE)
  as.disk.frame(disk.frame:::gen_datatable_synthetic(1e3+11), file.path(tempdir(), "tmp_rbindlist4.df"), overwrite=TRUE)
})

test_that("test rbindlist", {
  df1 = disk.frame(file.path(tempdir(), "tmp_rbindlist1.df"))
  df2 = disk.frame(file.path(tempdir(), "tmp_rbindlist2.df"))
  
  df3 = rbindlist.disk.frame(list(df1, df2), outdir = file.path(tempdir(), "tmp_rbindlist3.df"), overwrite=TRUE)
  
  expect_equal(nrow(df3), 2*(1e3+11))
})

test_that("test rbindlist accepts only list", {
  df1 = disk.frame(file.path(tempdir(), "tmp_rbindlist4.df"))

  expect_error(rbindlist.disk.frame(df1, outdir = file.path(tempdir(), "tmp_rbindlist5.df")))
})


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_rbindlist1.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rbindlist2.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rbindlist3.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rbindlist4.df"))
  fs::dir_delete(file.path(tempdir(), "tmp_rbindlist5.df"))
})

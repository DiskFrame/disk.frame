context("test-dplyr-verbs")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, "tmp_b_dv.df", nchunks = 5, overwrite = T)
})

test_that("testing dplyr verbs", {
  b = disk.frame("tmp_b_dv.df")
})


teardown({
  fs::dir_delete("tmp_a_ij.df")
  fs::dir_delete("tmp_b_ij.df")
  fs::dir_delete("tmp_d_ij.df")
  fs::dir_delete("tmp_a_ij2.df")
  fs::dir_delete("tmp_b_ij2.df")
  fs::dir_delete("tmp_d_ij2.df")
})
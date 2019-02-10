context("test-rename-filter")

setup({
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, "tmp_pls_delete.csv")
})

test_that("dplyr::rename", {
  dff = csv_to_disk.frame("tmp_pls_delete.csv", "tmp_pls_delete.df")
  nd = names(dff)
  dff1 = rename(dff, id12 = id1, id21 = id2)
  nd1 = names(head(dff1))
  expect_false(setequal(nd, nd1))
})

test_that("dplyr::filter", {
  dff = csv_to_disk.frame("tmp_pls_delete.csv", "tmp_pls_delete.df")
  dff1 = dff %>% 
    filter(v1 == 1) %>% 
    collect

  expect_false(nrow(dff) == nrow(dff1))
})


teardown({
#  fs::file_delete("tmp_pls_delete.csv")
#  fs::dir_delete("tmp_pls_delete.df")
})
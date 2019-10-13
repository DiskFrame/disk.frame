context("test-arrange")

setup({
  
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, "tmp_pls_delete_gb.csv")
})


test_that("test hard_arrange on disk.frame", {
  dff = csv_to_disk.frame("tmp_pls_delete_gb.csv", "tmp_pls_delete_gb.df")
  
  # Sort ascending
  sorted_dff <- dff %>% hard_arrange(id1, id4)
  sorted_df <- sorted_dff %>% collect
  expect_true(!is.unsorted(sorted_df$id1))
  
  # Sort decending
  desc_dff <- dff %>% hard_arrange(desc(id4), id2)
  desc_dff <- desc_dff %>% collect
  expect_true(!is.unsorted(-desc_dff$id4))
})

test_that("test hard_arrange on data.frame", {
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  
  # Sort ascending
  sorted_dff <- df %>% hard_arrange(id1, id4)
  sorted_df <- sorted_dff %>% collect
  expect_true(!is.unsorted(sorted_df$id1))
  
  # Sort decending
  desc_dff <- df %>% hard_arrange(desc(id4), id2)
  desc_df <- desc_dff %>% collect
  expect_true(!is.unsorted(-desc_df$id4))
})

teardown({
  fs::file_delete("tmp_pls_delete_gb.csv")
  fs::dir_delete("tmp_pls_delete_gb.df")
})
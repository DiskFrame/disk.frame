context("test-arrange")

setup({
  
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_gb.csv"))
})


test_that("test hard_arrange on disk.frame", {
  dff = csv_to_disk.frame(
    file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
    file.path(tempdir(), "tmp_pls_delete_gb.df"))
  
  # Sort ascending, one level
  sorted_dff <- dff %>% hard_arrange(id1)
  sorted_df <- sorted_dff %>% collect
  
  expect_true(!is.unsorted(sorted_df$id1))
  
  # Sort ascending, two levels
  sorted_dff <- dff %>% hard_arrange(id1, id4)
  sorted_df <- sorted_dff %>% collect
  sorted_df$id1_id4 <- paste0(
    sorted_df$id1, 
    formatC(sorted_df$id4, width=3, format="d", flag= "0")
  )
  expect_true(!is.unsorted(sorted_df$id1_id4))
  
  # Sort decending, two levels
  desc_dff <- dff %>% hard_arrange(desc(id4), id2)
  desc_dff <- desc_dff %>% collect
  
  #  Level 1
  expect_true(!is.unsorted(-desc_dff$id4))
  
  #  Level 2
  desc_dff$id4_id2 <- paste0(
    formatC(max(desc_dff$id4) - desc_dff$id4, width=3, format="d", flag= "0"), 
    desc_dff$id2)
  expect_true(!is.unsorted(-desc_dff$id4))
  
  # Sort ascending, three levels, from already partially sorted disk frame
  sorted_dff2 <- sorted_dff %>% hard_arrange(id1, id4, id6)
  sorted_df2 <- sorted_dff2 %>% collect

  sorted_df2$id1_id4_id6 <- paste0(
    sorted_df2$id1,
    formatC(sorted_df2$id4, width=3, format="d", flag= "0"),
    formatC(sorted_df2$id6, width=3, format="d", flag= "0"))
  expect_true(!is.unsorted(sorted_df2$id1_id4_id6))
  
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
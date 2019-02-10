context("test-group_by")

setup({
  library(magrittr)
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, "tmp_pls_delete_gb.csv")
})

test_that("group_by", {
  dff = csv_to_disk.frame("tmp_pls_delete_gb.csv", "tmp_pls_delete_gb.df")
  dff_res = dff %>% 
    collect %>% 
    group_by(id1) %>% 
    summarise(mv1 = mean(v1))
  
  dff1 = NULL
  expect_warning({
    dff1 <- dff %>% 
      group_by(id1, id2, .hard = F) %>%
      summarise(mv1 = mean(v1)) %>% collect
  })
  
  expect_false(nrow(dff1) == nrow(dff_res))
})


teardown({
  fs::file_delete("tmp_pls_delete_gb.csv")
  fs::dir_delete("tmp_pls_delete_gb.df")
})

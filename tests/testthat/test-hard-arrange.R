# context("test-arrange")
# 
# setup({
#   
#   df = disk.frame:::gen_datatable_synthetic(1e3+11)
#   data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_gb.csv"))
# })
# 
# test_that("test hard_arrange on disk.frame, single chunk", {
#   # Randomise rows since rows are already sorted
#   iris.df = as.disk.frame(sample_n(iris, nrow(iris)), nchunks = 1)
#   iris_hard.df = hard_arrange(iris.df, Species)
#   
#   # Check sort
#   expect_true(!is.unsorted(iris_hard.df$Species))
# })
# 
# test_that("test hard_arrange on disk.frame, single variable", {
#   dff = csv_to_disk.frame(
#     file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
#     file.path(tempdir(), "tmp_pls_delete_gb.df"))
#   
#   # Sort ascending, one level
#   sorted_dff <- dff %>% hard_arrange(id1)
#   sorted_df <- sorted_dff %>% collect
#   
#   expect_true(!is.unsorted(sorted_df$id1))
# })
# 
# test_that("test hard_arrange on disk.frame, factor data type", {
#   iris.df = as.disk.frame(sample_n(iris, nrow(iris)), nchunks = 2)
#   iris_hard.df = hard_arrange(iris.df, Species)
# 
#   expect_true(!is.unsorted(iris_hard.df$Species))  
# })
# 
# test_that("test hard_arrange on disk.frame, date data type", {
#   dff = csv_to_disk.frame(
#     file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
#     file.path(tempdir(), "tmp_pls_delete_gb.df"))
#   sorted_dff <- dff %>% hard_arrange(date1)
#   
#   expect_true(!is.unsorted(sorted_dff$date1))    
# })
# 
# test_that("test hard_arrange on disk.frame, two and three variables", {   
#   dff = csv_to_disk.frame(
#     file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
#     file.path(tempdir(), "tmp_pls_delete_gb.df"))
#   
#   dfp <- read.csv(file.path(tempdir(), "tmp_pls_delete_gb.csv"))
#   
#   # Sort ascending, two levels
#   sorted_dff <- dff %>% hard_arrange(id1, id4) %>% collect
#   sorted_dfp <- dff %>% collect %>% dplyr::arrange(id1, id4) 
#   
#   # Compare vs dplyr
#   expect_true(all(sorted_dff$id1 == sorted_dfp$id1))
#   expect_true(all(sorted_dff$id4 == sorted_dfp$id4))
#   
#   # Sort ascending, three levels, from already partially sorted disk frame
#   sorted_dff2 <- sorted_dff %>% hard_arrange(id1, id4, id6) %>% collect
#   sorted_dfp2 <- dff %>% collect %>% dplyr::arrange(id1, id4, id6) 
#   
#   # Compare vs dplyr
#   expect_true(all(sorted_dff2$id1 == sorted_dfp2$id1))
#   expect_true(all(sorted_dff2$id4 == sorted_dfp2$id4))
#   expect_true(all(sorted_dff2$id6 == sorted_dfp2$id6))  
# })
# 
# test_that("test hard_arrange on disk.frame, two factors", { 
#   dff = csv_to_disk.frame(
#     file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
#     file.path(tempdir(), "tmp_pls_delete_gb.df"))
#     
#   # Sort decending, two levels
#   desc_dff <- dff %>% hard_arrange(desc(id4), id2)
#   desc_dff <- desc_dff %>% collect
#   
#   #  Level 1
#   expect_true(!is.unsorted(-desc_dff$id4))
#   
#   #  Level 2
#   desc_dff$id4_id2 <- paste0(
#     formatC(max(desc_dff$id4) - desc_dff$id4, width=3, format="d", flag= "0"), 
#     desc_dff$id2)
#   expect_true(!is.unsorted(-desc_dff$id4))
# })
# 
# test_that("test hard_arrange on data.frame vs dplyr", {
#   df = disk.frame:::gen_datatable_synthetic(1e3+11)
#   
#   # Sort ascending
#   sorted_dff <- df %>% hard_arrange(id1, id4) %>% collect
#   sorted_dfp <- df %>% dplyr::arrange(id1, id4)
#   
#   expect_true(all(sorted_dff$id1 == sorted_dfp$id1))
#   expect_true(all(sorted_dff$id4 == sorted_dfp$id4))
#   
#   # Sort decending
#   desc_dff <- df %>% hard_arrange(desc(id4), id2) %>% collect
#   desc_dfp <- df %>% dplyr::arrange(desc(id4), id2) 
#   
#   expect_true(all(sorted_dff$id4 == sorted_dfp$id4))
#   expect_true(all(sorted_dff$id2 == sorted_dfp$dfp))  
# })
# 
# teardown({
#   fs::file_delete(file.path(tempdir(), "tmp_pls_delete_gb.csv"))
#   fs::dir_delete(file.path(tempdir(), "tmp_pls_delete_gb.df"))
# })
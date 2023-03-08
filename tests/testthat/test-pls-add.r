# context("test-pls-add")
# 
# setup({
#   #setup_disk.frame(workers = 1)
# })
# 
# test_that("pls-add", {
#   
#   # if (interactive()) {
#   #   library(disk.frame)
#   #   library(tidyverse)
#   #   
#   #   setup_disk.frame(2)
#   #   example <- as.disk.frame(
#   #     data.frame(
#   #       purchase_date=c("2020-03-20","2020-04-20"),
#   #       a = 1:2,
#   #       b = 3:4
#   #     )
#   #   )
#   #   example %>% 
#   #     mutate(Panel_Month = str_sub(purchase_date, 6, 7)) %>% 
#   #     collect
#   #   
#   #   str_sub2 = function(xx, yy) xx + yy
#   #   
#   #   example %>% 
#   #     mutate(Panel_Month = str_sub2(a, b)) %>% 
#   #     collect
#   #   
#   #   example %>% 
#   #     mutate(Panel_Month = str_sub2(a, 7)) %>% 
#   #     collect
#   #   
#   #   example %>% 
#   #     mutate(Panel_Month = str_sub2(6, 7)) %>% 
#   #     collect
#   #   
#   #   
#   #   example %>% 
#   #     mutate(Panel_Month = str_subs(purchase_date, 7)) %>% 
#   #     collect
#   # }
# })
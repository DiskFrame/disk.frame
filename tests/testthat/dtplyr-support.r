library(disk.frame)
library(tidyverse)
library(dtplyr)
iris_df = as.disk.frame(iris)

iris_df %>% 
  filter(Sepal.Length > 7) %>% 
  collect()


aa = iris_df %>% 
  map(~{
    dtplyr::lazy_dt(.x) %>% 
      filter(Sepal.Length > 7) %>% 
      collect()
  }) %>% 
  collect


lazy_dt <- function(...) {
  UseMethod("lazy_dt")
}

lazy_dt.disk.frame <- function(df, ...) {
  map(df, )
}

lazy_dt.default <- function(...) {
  dtplyr::lazy_dt(...)
}
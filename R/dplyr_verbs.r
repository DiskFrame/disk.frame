library(dplyr)
library(dtplyr)

select_.disk.frame <- function(.data, ..., .dots) {
  .dots <- lazyeval::all_dots(.dots, ...)
  cmd <- lazyeval::lazy(select_(.data, .dots=.dots))
  record(.data, cmd)
}

record <- function(.data, cmd){
  .data$cmds <- c(.data$cmds, list(cmd))
  .data$.vars <- NULL
  .data$.groups <- NULL
  .data
}

play <- function(.data, cmds=NULL){
  for (cmd in cmds){
    .data <- lazyeval::lazy_eval(cmd, list(.data=.data))
  }
  .data
}


if (F) {
  superpt = proc.time() #3:34 minutes
  library(magrittr)
  library(tidyr)
  library(future)
  library(fst)
  plan(multiprocess)
  library(data.table)
  library(lubridate)
  pt = proc.time()
  source("R/disk.frame.r")
  wbc_hl = disk.frame("wbc_hl.df/")
  
  tbl_vars()
  
  select_.disk.frame (wbc_hl)
}




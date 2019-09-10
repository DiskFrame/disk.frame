library(disk.frame)

setup_disk.frame()

system.time(b <- csv_to_disk.frame(
  "c:/data/combined.csv",
  colClasses = list(character = c("WHEELS_ON", "WHEELS_OFF")),
  in_chunk_size = 4e6,
  chunk_reader = "bigreadr"
))

system.time(nl <- bigreadr::nlines("c:/data/combined.csv"))
system.time(a <- bigreadr::split_file(
  "c:/data/combined.csv", 
  every_nlines = 4e6, 
  repeat_header = TRUE))

system.time(b <- csv_to_disk.frame(
  bigreadr::get_split_files(a),
  header = TRUE,
  colClasses = list(character = c("WHEELS_ON", "WHEELS_OFF"))
))



#' system('
#' @echo off
#' Setlocal EnableDelayedExpansion
#' for /f "usebackq" %%a in (`dir /b %1`)  do (
#'   for /f "usebackq" %%b in (`type %%a ^| find "" /v /c`) do (
#'     set /a lines += %%b
#'     )
#'   )
#' echo %lines%
#' endlocal
#' 
#' CountLines "c:\data\combined.csv"
#'        ')

a = disk.frame(file.path(path_to_data, "airontimecsv.df"))


system.time(r_mean_del_delay <- a %>% 
              group_by(YEAR, MONTH, DAY_OF_MONTH) %>% 
              summarise(sum_delay = sum(DEP_DELAY, na.rm = TRUE), n = n()) %>% 
              collect %>% 
              group_by(YEAR, MONTH, DAY_OF_MONTH) %>% 
              summarise(mean_delay = sum(sum_delay)/sum(n)))


library(lubridate)

dep_delay =  r_mean_del_delay %>%
  arrange(YEAR, MONTH, DAY_OF_MONTH) %>%
  mutate(date = ymd(paste(YEAR, MONTH, DAY_OF_MONTH, sep = "-")))

library(ggplot2)
ggplot(dep_delay, aes(date, mean_delay)) + geom_smooth()


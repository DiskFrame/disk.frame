library(data.table)
library(disk.frame)
setup_disk.frame()

bench_disk.frame_data.table_group_by <- function(data1,n) {
  a.sharded.df = as.disk.frame(data1, shardby = c("year", "month", "day"))
  
  a.not_sharded.df = as.disk.frame(data1)
  
  a = copy(data1)
  setDT(a)
  
  data.table_timing = system.time(a[,.(mean_dep_time = mean(dep_time, na.rm=T)), .(year, month, day)])[3]
  
  disk.frame_sharded_timing = system.time(
    a.sharded.df[
      ,
      .(mean_dep_time = mean(dep_time, na.rm=TRUE)), 
      .(year, month, day),
      keep = c("year", "month","day", "dep_time")])[3]
  
  
  disk.frame_not_sharded_timing = system.time(
    a.not_sharded.df[
      ,
      .(
        sum_dep_time = sum(dep_time, na.rm=TRUE),
        n = sum(!is.na(dep_time))
      ), 
      .(year, month, day),
      keep = c("year", "month","day", "dep_time")][
        ,
        .(mean_dep_time = sum(sum_dep_time)/sum(n)),
        .(year, month, day)
        ])[3]
  
  barplot(
    c(data.table_timing, disk.frame_sharded_timing, disk.frame_not_sharded_timing),
    names.arg = c("data.table", "sharded disk.frame", "not sharded disk.frame"), 
    main = glue:glue("flights duplicated {n}  times group-by year, month, day"),
    ylab = "Seconds")
}

system.time(flights_100 <- rbindlist(lapply(1:100, function(x) nycflights13::flights)))

system.time(flights_1000 <- rbindlist(lapply(1:10, function(x) flights_100)))

bench_disk.frame_data.table_group_by(flights_100, 100)
bench_disk.frame_data.table_group_by(flights_1000, 1000)

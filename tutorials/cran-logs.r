a = data.table::fread("c:/Users/ZJ.DAI/Downloads/a (1).csv")

cran.stats::read_logs(start=as.Date("2019-08-27"), as.Date("2019-08-27"), dir = "d:/data/cran-logs")

library(dplyr)
library(data.table)
a[,date:=as.Date(date, "%Y-%m-%d")]

a1 = a %>% 
  # filter(N < 100) %>% 
  right_join(data.frame(date = seq(min(a$date), max(a$date), by="day")))

setDT(a1)
a1[,N := ifelse(is.na(N), (lag(N) + lead(N))/2, N)]
a2 = a1[!is.na(N)]
fwrite(a2, "c:/data/a1.csv", row.names  = FALSE)

a3 = cbind(a2, a2[,shift(N,-3:3, type = "lead")] %>% 
             apply(1,mean)) %>% 
  filter(!is.na(V2)) %>% 
  select(-N)
fwrite(a3, "c:/data/a3.csv", row.names  = FALSE)

plot(a3)

library(pixapi)

result = pixapi::get_pix_forecasts("https://ep-d5adc7ba-0218-44f2-9ed9-eb6d202784e8.serving.aiaengine.com/invocations", 14)
setDT(result)
a4 = rbind(a3, result[,.(date = as.Date(date), V2)])
plot(a4)

a4 %>% 
  full_join(a1) -> a5

write.csv(a5, "a5.csv")


rs = cran.stats::read_logs(start = as.Date("2019-09-26"), as.Date("2019-09-30"), dir = "cran-logs")

library(drake)
library(disk.frame)
setup_disk.frame()



plan <- drake_plan(
  lf1 = list.files(
      file_in("d:/data/cran-logs"), 
      pattern="*.csv", 
      full.names = T
    ),
  x = target(
    disk.frame::csv_to_disk.frame(
      lf1,
      outdir = drake_tempfile()),
    format = "diskframe"
  )
)

make(plan)

config <- drake_config(plan)
vis_drake_graph(config)

x = readd(lf1)

head(x)

readd(x)
system.time(
  df <- csv_to_disk.frame(list.files("d:/data/cran-logs", pattern="*.csv", full.names = T))
)

head(df)

df = shard(df, shardby = "date")
df[package=="disk.frame",.N, date, keep=c("package","date")][order(date)]

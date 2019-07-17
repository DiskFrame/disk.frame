library(disk.frame)
plan(multiprocess)
library(magrittr)

a = disk.frame("flights_all_sharded.df")
b = disk.frame("flights_all_notsharded.df")

a2 = a %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=n()) %>% 
  collect %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=sum(n)) %>% 
  arrange(UniqueCarrier)

b2 = b %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=n()) %>% 
  collect %>% 
  group_by(UniqueCarrier) %>% 
  summarise(n=sum(n)) %>% 
  arrange(UniqueCarrier)

identical(a2, b2)

fa = list.files("inst/flights_case_study/data", 
           pattern="*.csv", 
           full.names = T) %>% 
  head(10)

ab = csv_to_disk.frame(fa, outdir ="pls_del_tmp", overwrite = T)

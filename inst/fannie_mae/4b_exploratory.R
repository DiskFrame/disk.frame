source("inst/fannie_mae/0_setup.r")
library(disk.frame)
#future::plan(multiprocess(workers = availableCores()))

fmdf_all = disk.frame(file.path(outpath, "fmdf_appl"))

head(fmdf_all)

# how many delq.status are there ------------------------------------------
# system.time(dscnt <- fmdf_all[,.N,delq.status, keep="delq.status"])
#dscnt[order(delq.status),sum(N),delq.status %>% as.integer]

system.time(dscnt2 <- fmdf_all %>% 
              srckeep(c("delq.status", "default_12m", "harp_12m")) %>% 
              group_by(delq.status, hard = F) %>% 
              summarise(n = n(), ndef = sum(default_12m, na.rm=T), nharp = sum(harp_12m, na.rm=T)) %>% 
              collect(parallel = T) %>% 
              group_by(delq.status) %>% 
              summarise(n = sum(n), ndef = sum(ndef), nharp = sum(nharp)) %>% 
              arrange(delq.status))


dscnt2[delq.status %in% c("X","","0","1","2"),.(delq.status, ndef/n)]

# how many default and go to HARP -----------------------------------------
# system.time(hd <- fmdf_all[,.N, .(harp_12m,default_12m), keep=c("harp_12m","default_12m")])
# hd[,.(N = sum(N)), .(harp_12m, default_12m)]

# 45 seconds
system.time(hd <- fmdf_all %>% 
  srckeep(c("harp_12m", "default_12m", "delq.status")) %>% 
  filter(delq.status %in% c("X","","0","1","2")) %>% 
  group_by(harp_12m, default_12m, hard = F) %>% 
  summarise(n = n()) %>% 
  collect(parallel = T) %>% 
  group_by(harp_12m, default_12m) %>% 
  summarise(n = sum(n)))

# Source: local data table [4 x 3]
# Groups: harp_12m
# 
# # A tibble: 4 x 3
# harp_12m default_12m          n
# <lgl>    <lgl>            <int>
#   1 NA       NA          1765366501
# 2 TRUE     NA            12343954
# 3 NA       TRUE          22366382
# 4 TRUE     TRUE               638

# how many accounts eventually default ------------------------------------
#future::plan(multiprocess(workers = availableCores()))
pt = proc.time() # 4:26
simple2 = fmdf_all %>%
  srckeep(c("oltv", "default_12m", "harp_12m", "loan_id", "orig_dte", "delq.status")) %>%
  #filter(delq.status %in% c("0","X","1","2")) %>% 
  mutate(orig_yr = substr(orig_dte, 4,7)) %>% 
  group_by(oltv, loan_id, orig_yr, hard = F) %>%
  summarise(ndef = sum(default_12m, na.rm=T), n = n()) %>%
  collect(parallel = T) %>%
  group_by(oltv, loan_id, orig_yr) %>%
  summarise(ndef = sum(ndef), n = sum(n))
timetaken(pt)

stopifnot(simple2[,n_distinct(loan_id) == .N])

#simple3 = simple2[,.(ndef = sum(ndef > 0), .N), .(orig_yr_band = cut(as.numeric(orig_yr),c(-Inf, 2005, 2007, 2008,2009,Inf)), oltv_band = ceiling(oltv/10)*10)]

simple3 = simple2[,.(ndef = sum(ndef > 0), .N), .(orig_yr, oltv_band = cut(oltv, c(-Inf, seq(0,80,by=20),Inf)))]

simple3 %>% 
  mutate(odr = ndef/N) %>% 
  select(oltv_band, odr, orig_yr) %>% 
  spread(key = oltv_band, value = odr)

simple3 %>% 
  filter(!is.na(oltv_band)) %>% 
  mutate(odr = ndef/N) %>%
  arrange(oltv_band) %>% 
  ggplot +
  geom_bar(
    aes(
      x = as.factor(oltv_band), 
      weight = odr, 
      colour = orig_yr),
      position = "dodge")








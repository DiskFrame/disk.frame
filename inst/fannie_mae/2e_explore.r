df = csv_to_disk.frame("d:/data/fannie_mae/Performance_2000Q1.txt", outdir = "tmp_pls_del", col.names = paste0("col",1:31), nchunks = 500)
df
ncol(df)
nchunks(df)
glimpse(df)
library(disk.frame)


glimpse(a)

library(disk.frame)
library(magrittr)
a = disk.frame("tmp4")
system.time(a1 <- a %>% 
  keep(names(a)[1:16]) %>% 
  map.disk.frame(~.x, outdir="only16.df", lazy = F))

system.time(a2 <- a1[,.(.N, sum(default_12m, na.rm=T)), servicer.name, keep=c("servicer.name","default_12m")])
a2[,sum(N)/sum(default_12m), servicer.name]
  
system.time(a2<-a1[,.N,mod_flag])
a2[,sum(N), mod_flag]

tmp4 = disk.frame("tmp4")
head(tmp4)

tmp5 = tmp4 %>% 
  keep(c("zero.bal.code","monthly.rpt.prd", "default_12m"))


system.time(tmp6 <- tmp5[,.(ndef = sum(default_12m,na.rm=T), .N),.(zero.bal.code, monthly.rpt.prd)])


tmp7 = tmp6[,.(ndef = sum(ndef), N = sum(N)),.(zero.bal.code, monthly.rpt.prd)]

tmp7[,rate:=ndef/N]

library(ggplot2)
tmp7 %>% 
  ggplot +
  geom_line(aes(y=rate, x=monthly.rpt.prd, colour = zero.bal.code))



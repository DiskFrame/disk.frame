# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")
pt <- proc.time()

fmdf = disk.frame("only16.df")
tmp2 = disk.frame("defaults.df") # tmp2 contains all the defaults
harp2 = disk.frame("first_harp_date.df")

fmdf_lazy = delayed(fmdf, function(df) {
  df[,monthly.rpt.prd:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
  df[,monthly.rpt.prd2:=monthly.rpt.prd]
})

tmp2_lazy = delayed(tmp2, function(df) {
  setkey(df, "loan_id","start_date","end_date")
})

harp2_lazy = lazy(harp2, function(df) {
  setkey(df, "loan_id","before_12m_first_harp_date","first_harp_date")
})

#dh <- rbindlist.disk.frame(tmp2_lazy, harp2_lazy, outdir ="defaults_harp.df", by_chunk_id = T)
#plan(transparent)
system.time(tmp3 <- foverlaps.disk.frame(
  fmdf_lazy, 
  tmp2_lazy, 
  outdir="tmp3_16", 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "start_date", "end_date"),
  merge_by_chunk_id = T,
  compress = 50
))


system.time(tmp4 <- foverlaps.disk.frame(
  tmp3, 
  harp2_lazy, 
  outdir="tmp4_16", 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "before_12m_first_harp_date", "first_harp_date"),
  merge_by_chunk_id = T,
  compress = 50
))

data.table::timetaken(pt)


tmp4 <- disk.frame("tmp4_16")
system.time(tmp5 <- tmp4[,.(ndef=sum(default_12m, na.rm=T),.N), .(monthly.rpt.prd, delq.status),
     keep=c("default_12m","monthly.rpt.prd","delq.status")])


tmp5[, delq.status.cap := pmin(5, as.integer(delq.status))]
tmp5[delq.status == "X", delq.status.cap := 0]
tmp5[delq.status == "", delq.status.cap := 0]


tmp6 <- tmp5[,.(ndef=sum(ndef), N=sum(N)),.(monthly.rpt.prd, delq.status.cap)]


tmp6[,defr:=ndef/N]

library(ggplot2)
tmp6 %>% 
  ggplot +
  geom_line(aes(x=monthly.rpt.prd, y = defr, colour=as.factor(delq.status.cap)))



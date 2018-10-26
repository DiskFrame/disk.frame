# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fmdf = disk.frame("fmdf")
tmp2 = disk.frame("defaults.df") # tmp2 contains all the defaults
harp2 = disk.frame("first_harp_date.df")

#
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
  outdir="tmp3", 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "start_date", "end_date"),
  merge_by_chunk_id = T,
  compress = 50
))


system.time(tmp4 <- foverlaps.disk.frame(
  tmp3, 
  harp2_lazy, 
  outdir="tmp4", 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "before_12m_first_harp_date", "first_harp_date"),
  merge_by_chunk_id = T,
  compress = 50
))
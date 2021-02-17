# 2_exploratory.r
source("tutorials/fannie_mae/00_setup.r")

fmdf = disk.frame(file.path(outpath, "fm.df"))
tmp2 = disk.frame(file.path(outpath, "defaults.df"))
harp2 = disk.frame(file.path("first_harp_date.df"))

fmdf_lazy = delayed(fmdf, function(df) {
  df[,monthly.rpt.prd:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
  df[,monthly.rpt.prd2:=monthly.rpt.prd]
})

tmp2_lazy = delayed(tmp2, function(df) {
  setkey(df, "loan_id","start_date","end_date")
})

harp2_lazy = delayed(harp2, function(df) {
  setkey(df, "loan_id","before_12m_first_harp_date","first_harp_date")
})

# took 513 seconds
pt <- proc.time()
tmp3 <- foverlaps.disk.frame(
  fmdf_lazy, 
  tmp2_lazy, 
  outdir=file.path(outpath, "fm_with_default"), 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "start_date", "end_date"),
  merge_by_chunk_id = T,
  overwrite = T
)
cat(glue::glue("time taken to merge on default flag {timetaken(pt)}"))

# took 6:55
pt <- proc.time()
tmp4 <- foverlaps.disk.frame(
  tmp3, 
  harp2_lazy, 
  outdir=file.path(outpath, "fm_with_harp"), 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "before_12m_first_harp_date", "first_harp_date"),
  merge_by_chunk_id = T,
  overwrite = T
)
cat(glue::glue("time taken to merge on HARP flag {timetaken(pt)}"))
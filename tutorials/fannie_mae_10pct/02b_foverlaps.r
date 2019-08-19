# 2_exploratory.r
source("inst/fannie_mae_10pct/00_setup.r")

fmdf = disk.frame(file.path(outpath, "fm.df"))
tmp2 = disk.frame(file.path(outpath, "defaults.df"))

fmdf_lazy = delayed(fmdf, function(df) {
  df[,monthly.rpt.prd:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
  df[,monthly.rpt.prd2:=monthly.rpt.prd]
})

tmp2_lazy = delayed(tmp2, function(df) {
  setkey(df, "loan_id","start_date","end_date")
})

# took 513 seconds
pt <- proc.time()
tmp3 <- foverlaps.disk.frame(
  fmdf_lazy, 
  tmp2_lazy, 
  outdir=file.path(outpath, "fm_with_harp"), 
  by.x = c("loan_id", "monthly.rpt.prd", "monthly.rpt.prd2"),
  by.y = c("loan_id", "start_date", "end_date"),
  merge_by_chunk_id = T,
  overwrite = T
)
cat(glue::glue("time taken to merge on default flag {timetaken(pt)}"))

head(tmp3)

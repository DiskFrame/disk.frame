library(disk.frame)
setup_disk.frame()


fm.df = disk.frame("c:/data/fannie_mae_disk_frame/fm.df/")

fm.df %>% 
  mutate(
    yr = substr(monthly.rpt.prd, 7, 11) %>% as.numeric, 
    mth = substr(monthly.rpt.prd, 1, 2) %>% as.numeric) %>% 
  rechunk(nchunks = nchunks(fm.df), outdir = "c:/data/fannie_mae_disk_frame/fm_by_yr_mth.df/", shardby = c("yr", "mth"))





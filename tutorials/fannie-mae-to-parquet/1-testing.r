library(disk.frame)
setup_disk.frame()

df1 = disk.frame("c:/data/fannie_mae_disk_frame/fm_by_yr_mth.df/")
system.time(a <- df1[,.N, .(yr, mth), keep=c("yr", "mth")])

a

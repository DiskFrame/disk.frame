source("inst/fannie_mae/00_setup.r")

# create a most compressed set
a = disk.frame("fmdf")
chunk_lapply(a,function(x) x, outdir="fmdf0", chunks=500, compress=100)

a1 = disk.frame("fmdf0")

system.time(print(a[,.N,keep="delq.status"] %>% sum))
system.time(print(a1[,.N,keep="delq.status"] %>% sum))

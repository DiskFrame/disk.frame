library(disk.frame)

fmdf <- disk.frame(file.path(outpath, "fm.df"))

# rechunk respects the shardkey
rechunk(fmdf, 24)

nrow(fmdf)

nchunks(fmdf)

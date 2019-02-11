library(disk.frame)

fmdf <- disk.frame(file.path(outpath, "fm.df"))

# rechunk respects the shardkey
system.time(rechunk(fmdf, nchunks(fmdf)*2))

nrow(fmdf)

nchunks(fmdf)


if(F) {
  system.time(rechunk(fmdf, 6))
}


source("inst/fannie_mae/00_setup.r")

disk.frame_folders = dir(file.path(outpath, "raw_fannie_mae"), full.names = T)
list_of_disk.frames <- purrr::map(disk.frame_folders, disk.frame)

pt <- proc.time()
fmdf = rbindlist.disk.frame(list_of_disk.frames, outdir = file.path(outpath, "fm.df"), compress = compress, overwrite = T)
print(timetaken(pt))


# takes about 6 minutes to read and write
if(F) {
  fmdf = disk.frame(file.path(outpath, "fm.df"))
  system.time(write_disk.frame(
    fmdf, 
    outdir = "fm2.df", 
    nchunks = nchunks(fmdf), 
    overwrite = T, 
    compress = 100))  
}

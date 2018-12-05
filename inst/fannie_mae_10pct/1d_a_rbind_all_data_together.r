source("inst/fannie_mae/0_setup.r")

disk.frame_folders = dir(file.path(outpath, "raw_fannie_mae"),full.names = T)
list_of_disk.frames <- purrr::map(disk.frame_folders, disk.frame)

pt <- proc.time()
fmdf = rbindlist.disk.frame(
  list_of_disk.frames, 
  outdir = file.path(outpath, "fm.df"), 
  by_chunk_id = T,
  overwrite = T)
print(timetaken(pt))

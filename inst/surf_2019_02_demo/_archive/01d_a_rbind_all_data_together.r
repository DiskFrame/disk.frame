source("inst/surf_2019_02_demo/00_setup.r")

disk.frame_folders = dir(file.path(outpath, "raw_fannie_mae"),full.names = T)
list_of_disk.frames <- purrr::map(disk.frame_folders, disk.frame)

pt <- proc.time()
fmdf = rbindlist.disk.frame(
  list_of_disk.frames, 
  outdir = file.path(outpath, "fm.df"), 
  by_chunk_id = T,
  overwrite = T)
print(timetaken(pt))

nrow(fmdf)

nchunks(fmdf)

fmdf %>% delayed(~.x[,.(sum(is.na(prin_forg_upb_oth )))]
) %>% dput

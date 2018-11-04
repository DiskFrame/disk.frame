source("inst/fannie_mae/0_setup.r")

disk.frame_folders = dir("test_fm",full.names = T)
list_of_disk.frames <- purrr::map(disk.frame_folders, disk.frame)


pt <- proc.time()
fmdf = rbindlist.disk.frame(list_of_disk.frames, outdir = "fmdf", overwrite = T)
print(timetaken(pt))

#system.time(map.disk.frame(fmdf, base::I, outdir="fmdf_copy", lazy = F))

source("inst/fannie_mae/0_setup.r")


# try to find files that didn't extract properly --------------------------
full_outdirs = dir(file.path("test_fm"), full.names = T)
folders_to_remove = full_outdirs[map(full_outdirs, ~(length(dir(.x)) != 500)) %>% unlist]
if(length(folders_to_remove) > 0) {
  fs::dir_delete(folders_to_remove)
}

files_to_do = setdiff(dir(raw_perf_data_path), dir(file.path("test_fm")))

if(length(files_to_do) > 0) {
  system.time(future_lapply(1:length(files_to_do), function(i) {
    csv_to_disk.frame(file.path(raw_perf_data_path, files_to_do[i]), glue("test_fm/{files_to_do[i]}"), in_chunk_size = 10e6, shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables)
  }))
}


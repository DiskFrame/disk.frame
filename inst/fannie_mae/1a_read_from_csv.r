source("inst/fannie_mae/0_setup.r")

# number of rows to read in
nreadin = 1e7
compress = 50

# how many chunks do we need?
relative_file_path = dir(raw_perf_data_path)
full_file_path = dir(raw_perf_data_path, full.names = T)

file_sizes = purrr::map_dbl(full_file_path, ~file.size(.x))

# use the recommend_nchunks function to get a chunksize based on your RAM and
# number of CPU cores
nchunks = sum(file_sizes) %>% recommend_nchunks(type="csv")

relative_file_path = relative_file_path[order(file_sizes, decreasing = T)]
full_file_path = full_file_path[order(file_sizes, decreasing = T)]

l = length(full_file_path)

pt <- proc.time()
system.time(future_lapply(1:l, function(i) {
#system.time(future_lapply(1:6, function(i) {
  relative_file_pathi = relative_file_path[i]
  full_file_path
  csv_to_disk.frame(
    full_file_path[i], 
    glue("test_fm/{relative_file_path[i]}"), 
    shardby="loan_id", 
    nchunks=nchunks, 
    colClasses = Performance_ColClasses, 
    col.names = Performance_Variables, 
    sep="|", 
    compress=compress, 
    in_chunk_size = nreadin, 
    overwrite = T)
}))
print(timetaken(pt))


if(F){
  for(k in 4:3) {
    files_to_do = ok[newid == k, path]
    short_files = ok[newid == k, short]
    system.time(future_lapply(1:length(files_to_do), function(i) {
      short_files = short_files
      csv_to_disk.frame(files_to_do[i], glue("test_fm/{short_files[i]}"), shardby="loan_id", nchunks=nchunks, colClasses = Performance_ColClasses, col.names = Performance_Variables, sep="|", compress=compress, in_chunk_size = nreadin)
    }))
    system.time(future_lapply(1:length(files_to_do), function(i) {
      short_files = short_files
      csv_to_disk.frame(files_to_do[i], glue("test_fm/{short_files[i]}"), shardby="loan_id", nchunks=nchunks, colClasses = Performance_ColClasses, col.names = Performance_Variables, sep="|", compress=compress, in_chunk_size = nreadin)
    }))
  }
  
  for(k in 2:1) {
    files_to_do = ok[newid == k, path]
    if(length(files_to_do) > 0) {
      short_files = ok[newid == k, short]
      system.time(future_lapply(1:length(files_to_do), function(i) {
        short_files = short_files
        csv_to_disk.frame(files_to_do[i], glue("test_fm/{short_files[i]}"), shardby="loan_id", nchunks=nchunks, colClasses = Performance_ColClasses, col.names = Performance_Variables, sep="|", compress=compress, in_chunk_size = nreadin)
      }))
    }
  }
}


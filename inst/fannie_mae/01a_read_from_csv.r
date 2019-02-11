source("inst/fannie_mae/00_setup.r")

# number of rows to read in from each file in one go
nreadin = NULL
if(round(memory.limit()/1024,0) < 64) { # if there are less than 64G of RAM read 1m rows at a time
  nreadin = round(memory.limit()/1024,0) * 1e6 # 10 million
}

# compression ratio, max = 100 for best compression but slower running speed
compress = 100

# set up some variable for future use
relative_file_path = dir(raw_perf_data_path)
full_file_path = dir(raw_perf_data_path, full.names = T)

file_sizes = purrr::map_dbl(full_file_path, ~file.size(.x))

set.seed(1)
# randomise the order of files to ensure even spread
new_order = order(runif(length(file_sizes)))

relative_file_path = relative_file_path[new_order]
full_file_path = full_file_path[new_order]

# use the recommend_nchunks function to get a chunksize based on your RAM and
# number of CPU cores
nchunks = sum(file_sizes) %>% recommend_nchunks(type="csv")

# convert CSV in parallel
pt <- proc.time()
res = future.apply::future_Map(function(relative_file_pathi, full_file_pathi) {
  #res = mapply(function(relative_file_pathi, full_file_pathi) {
  relative_file_pathi # for glue
  df = csv_to_disk.frame(
    full_file_pathi, 
    file.path(outpath, glue("raw_fannie_mae/{relative_file_pathi}")), 
    shardby = "loan_id", 
    nchunks = nchunks, 
    colClasses = Performance_ColClasses, 
    col.names = Performance_Variables, 
    sep = "|", 
    compress = compress, 
    in_chunk_size = nreadin, 
    overwrite = T)
  df
}, relative_file_path, full_file_path)
print(timetaken(pt))


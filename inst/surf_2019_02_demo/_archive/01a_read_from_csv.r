source("inst/surf_2019_02_demo/00_setup.r")

# number of rows to read in from each file in one go
nreadin = 1e7

# compression ratio, max = 100 for best compression but slower running speed
#compress = 50

# set up some variable for future use
relative_file_path = dir(raw_perf_data_path)
full_file_path = dir(raw_perf_data_path, full.names = T)

file_sizes = purrr::map_dbl(full_file_path, ~file.size(.x))

# use the recommend_nchunks function to get a chunksize based on your RAM and
# number of CPU cores
nchunks = sum(file_sizes) %>% recommend_nchunks(type="csv")

# order the order of conversion by prioritising the largest files first. Because
# handling the largest files are the most difficult, and if an error occurs it
# is more likely to occur when converting large files, hence this will allow us
# to identify errors early
relative_file_path = relative_file_path[order(file_sizes, decreasing = T)]
full_file_path = full_file_path[order(file_sizes, decreasing = T)]

l = length(full_file_path)

# convert CSV in parallel
pt <- proc.time()
res = future.apply::future_mapply(function(relative_file_pathi, full_file_pathi) {
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
    #compress = compress, 
    in_chunk_size = nreadin, 
    overwrite = T)
  #browser()
  df
}, relative_file_path, full_file_path, SIMPLIFY = F)
print(timetaken(pt))

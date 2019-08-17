source("inst/fannie_mae/00_setup.r")

# number of rows to read in from each file in one go
nreadin = NULL

# your ram size in G
# this is needed as memomry.limit seems broken 
ram.size = 64
nc = parallel::detectCores(logical = FALSE)
conservatism = 2
minchunks = nc
# compression ratio, max = 100 for best compression but slower running speed
# compress = 100

# set up some variable for future use

full_file_path = dir(raw_perf_data_path, full.names = T)


# randomise it to maximize the chance of a good load balancing
#set.seed(1)
#full_file_path = sample(full_file_path, length(full_file_path))

# convert CSV in parallel
pt <- proc.time()
tot_file_size = sum(file.size(full_file_path))/1024^3
res = csv_to_disk.frame(
    full_file_path, 
    outdir = file.path(outpath, "raw_fannie_mae"), 
    shardby = "loan_id", 
    colClasses = Performance_ColClasses, 
    col.names = Performance_Variables, 
    sep = "|", 
    in_chunk_size = nreadin, 
    nchunks = max(round(tot_file_size/ram.size*nc)*nc*conservatism, nc),
    overwrite = TRUE,
    .progress = TRUE # show progress
)
print(data.table::timetaken(pt))


source("inst/fannie_mae/0_setup.r")

kmeans(file.size(dir(raw_perf_data_path,full.names = T)),2)
kmeans(file.size(dir(raw_perf_data_path,full.names = T)),3)
res = kmeans(file.size(dir(raw_perf_data_path,full.names = T)),4)
ok = data.table(short = short_dfiles, path = dfiles, cluster = res$cluster, size = file.size(dir(raw_perf_data_path,full.names = T)))

ok[,msize:=mean(size), cluster]

setkey(ok, msize, size)
ok[,newid:=rleid(cluster)]

if(T) {
  ok = ok[!short %in% dir("test_fm"),]
}

plan(multiprocess(workers = 3))
for(k in 1:2) {
  files_to_do = ok[newid == k, path]
  if(length(files_to_do) > 0) {
    short_files = ok[newid == k, short]
    system.time(future_lapply(1:length(files_to_do), function(i) {
      short_files = short_files
      csv_to_disk.frame(files_to_do[i], glue("test_fm/{short_files[i]}"), shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables, sep="|", compress=100, in_chunk_size = 1e7)
    }))
  }
}

#plan(sequential)
for(k in 3:4) {
  files_to_do = ok[newid == k, path]
  short_files = ok[newid == k, short]
  system.time(future_lapply(1:length(files_to_do), function(i) {
    short_files = short_files
    csv_to_disk.frame(files_to_do[i], glue("test_fm/{short_files[i]}"), shardby="loan_id", nchunks=500, colClasses = Performance_ColClasses, col.names = Performance_Variables, sep="|", compress=100, in_chunk_size = 1e7)
  }))
}

library(disk.frame)
# this willl set disk.frame with multiple workers
setup_disk.frame()
# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)


system.time(
  a <- 
    csv_to_disk.frame(
      "c:/data/feature_matrix_cleaned.csv", 
      in_chunk_size = 1e5/4
    )
)

system.time(rechunk(a, 16, outdir = "c:/data/ft.df", shardby="sk_id_curr"))

path = "d:/data/Performance_All/Performance_2016Q2.txt"

library(disk.frame)

delete(df)
df = disk.frame("pls_del")
df

system.time(
  a <- readr::read_delim_chunked(
    path, 
    function(x, y) {
      disk.frame::add_chunk(df, x, y)
      y
      },
    delim = "|", chunk_size = 1e6) 
)


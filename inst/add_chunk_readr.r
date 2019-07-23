library(disk.frame)
path = "d:/data/Performance_All/Performance_2016Q2.txt"


df = disk.frame("pls_del")
system.time(
  a <- readr::read_delim_chunked(
    path, 
    function(x, y) {
      disk.frame::add_chunk(df, x, y)
      y
    },
    delim = "|", chunk_size = 1e6, col_names = F) 
)
df
#delete(df)

y = 2

df[1:2,.N + y, X1]



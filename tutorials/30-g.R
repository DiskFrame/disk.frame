library(disk.frame)

setup_disk.frame(12)

a = disk.frame("c:/data/airontimecsv.df/")

system.time(b <- a %>% 
   group_by(YEAR, MONTH, DAY_OF_MONTH) %>% 
   summarise(sum(DEP_DELAY)) %>% 
   collect)


path_to_data <- "c:/data/"
rows = 148619656
recommended_nchunks = recommend_nchunks(file.size(file.path(path_to_data, "combined.csv")))
in_chunk_size = ceiling(rows/ recommended_nchunks)
# 
# pt= proc.time()
# a = bigreadr::split_file(file.path(path_to_data, "combined.csv"), every_nlines = in_chunk_size)
# f = bigreadr::get_split_files(a)
# csv_to_disk.frame(
#    f, 
#    outdir = "c:/data/split30g.df",
#    
#    colClasses = list(character = c("WHEELS_OFF","WHEELS_ON"))
# )
# data.table::timetaken(pt)

path_to_data = "c:/data/AirOnTimeCSV/"
#path_to_data = "d:/data/"
system.time(a <- csv_to_disk.frame(
   list.files(path_to_data, pattern = ".csv$", full.names = TRUE),
   outdir = file.path("c:/data/", "airontimecsv.df"),
   colClasses = list(character = c("WHEELS_OFF", "WHEELS_ON"))
))


# pt= proc.time()
# csv_to_disk.frame(
#    file.path(path_to_data, "combined.csv"), 
#    outdir = "c:/data/split30g.df",
#    in_chunk_size = in_chunk_size,
#    chunk_reader = "bigreadr",
#    #colClasses = list(character = c(22,23))
#    colClasses = list(character = c("WHEELS_OFF","WHEELS_ON"))
# )
# data.table::timetaken(pt)


system.time(flights.df <- csv_to_disk.frame(
   paste0(path_to_data, "combined.csv"), 
   outdir = paste0(path_to_data, "combined.laf.df"),
   in_chunk_size = in_chunk_size,
   backend = "LaF"
))

system.time(a <- csv_to_disk.frame(
   file.path(path_to_data, "combined.csv"),
   outdir = file.path(path_to_data, "combined.readr.df"),
   in_chunk_size = in_chunk_size,
   colClasses = list(character = c("WHEELS_OFF","WHEELS_ON")),
   chunk_reader = "readr"
))

system.time(a <- csv_to_disk.frame(
   file.path(path_to_data, "combined.csv"),
   outdir = file.path(path_to_data, "combined.readLines.df"),
   in_chunk_size = in_chunk_size,
   colClasses = list(character = c("WHEELS_OFF","WHEELS_ON")),
   chunk_reader = "readLines"
))







# 
# 
# system.time(flights.df <- csv_to_disk.frame(
#  paste0("c:/data/", "combined.csv"), 
#  outdir = paste0("c:/data/", "combined.all.df"),
#    in_chunk_size = 1e7,
#    backend = "readr",
#    col_types = readr::cols(
#      FL_DATE = readr::col_date()
#    )
#  ))

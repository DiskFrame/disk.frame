source("sas2disk_frame.r")

# please input path to your SAS file
file_to_convert = "c:/path/to/file.sas7bdat"

# set the path to sas2csv.exe
path2sas2csv = "C:/full/path/to/sas2csv.exe"

# where your disk.frame should be
output_path = "tmp_csv2df.df"

# the output directory needs to be empty!!!
fs::dir_delete("tmp_csv2df.df")

library(disk.frame)

sas_to_disk.frame(
  file_to_convert, # input file
  "tmp_csv2df.df", # output dir
  nchunks = 16,  # number of chunks
  sas2csvpath = path2sas2csv # path to sas2csv.exe
  )

df = disk.frame("tmp_csv2df.df")

nrow(df)
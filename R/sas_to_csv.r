#' Convert a SAS file (.sas7bdat) format to CSV by chunks
#' @param infile the SAS7BDAT file
#' @param chunk which convert of nchunks to convert
#' @param nchunks number of chunks
sas_to_csv <- function(infile, chunk, nchunks = disk.frame::recommend_nchunks(fs::file_size(infile))) {  
  sas2csvpath = "sas2csv/sas2csv.exe"
  if(!file.exists(sas2csvpath)) {
    "You must have the sas2csv.exe installed. Only Windows is supported at the moment. Please contact the author"
  }
  sasfile = glue::glue('"{infile}"')
  fs::dir_create(file.path("outcsv"))
  fs::dir_create(file.path("outcsv", chunk))
  options = glue::glue("-o outcsv/{chunk}/ -c -n {nchunks} -k {paste(chunk-1,collapse = ' ')} -m")
  
  cmd = paste(sas2csvpath, sasfile, options)
  system(cmd)
}

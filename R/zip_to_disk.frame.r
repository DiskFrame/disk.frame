#' Automatically read and convert every single CSV file within the zip file to disk.frame format
#' @param zipfile The zipfile
#' @param outdir The output directory for the disk.frames
#' @param ... passed to csv_to_disk.frame and data.table::fread
#' @import glue dplyr fst future future.apply fs
#' @export
#' @return a list of disk.frames
# TODO add all the options of fread into the ... as future may not be able to deal with it
zip_to_disk.frame = function(zipfile, outdir, ..., parallel = T) {
  # obtain the list of files in the zipfile
  files = unzip(zipfile, list=T)
  
  # TODO sort the files by file size
  
  # create the output directory
  fs::dir_create(outdir)
  
  # create a temporary directory; this is where all the CSV files are extracted to
  tmpdir = tempfile(pattern = "tmp_zip2csv")
  
  if(parallel) {
    res = future.apply::future_lapply(files$Name, function(fn) {      
      out_dir_for_file = file.path(outdir, fn)
        
      # unzip a file
      unzip(zipfile, files = fn, exdir = tmpdir)
  
      # create disk.frame from file
      res = csv_to_disk.frame(file.path(tmpdir, fn), out_dir_for_file, ...)
      add_meta(res)
    })
  } else {
    res = lapply(files$Name, function(fn) {      
      out_dir_for_file = file.path(outdir, fn)
        
      # unzip a file
      unzip(zipfile, files = fn, exdir = tmpdir)
  
      
      # create disk.frame
      res = csv_to_disk.frame(file.path(tmpdir, fn), out_dir_for_file, ...)
      add_meta(res)
    })
  }
  
  # validate 
  #if(validation.check) validate_zip_to_disk.frame(zipfile, outdir)
  
  res
}

# validate_zip_to_disk.frame(zipfile, outdir)
#' Validate and auto-correct read and convert every single file within the zip file to df format
#' @param zipfile The zipfile
#' @param outdir The output directory for disk.frame
#' @import glue
#' @import dplyr
#' @import fst
#' @import future
validate_zip_to_disk.frame = function(zipfile, outdir) {
  files = unzip(zipfile, list=T)
  
  if(!dir.exists(outdir)) {
    stop(glue("The output directory {outdir} does not exist.\n Nothing to validate."))
  }
  
  tmpdir = tempfile(pattern = "tmp_zip2csv")
  
  # check if files are ok
  system.time(lapply(files$Name, function(fn) {
    print(fn)
    out_fst_file = file.path(outdir, paste0(fn,".fst"))
    
    if(file.exists(out_fst_file)) {
      tryCatch({
        # the output file already exists
        # read it and if it errors then the file might be corrupted, so 
        # read it again and write again
        pt = proc.time()
        read_fst(out_fst_file, as.data.table = T)
        print(paste0("checking(read): ", timetaken(pt))); pt = proc.time()
      }, error = function(e) {
        print(e)
        pt = proc.time()
        unzip(zipfile, files = fn, exdir = tmpdir)
        print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
        write_fst(fread(file.path(tmpdir, fn)), out_fst_file,100)
        print(paste0("read: ", timetaken(pt)))
        unlink(file.path(tmpdir, fn))
        gc()
      })
      print("output already exists")
      return(NULL)
    } else {
      # if the output file doesn't exists then the process might have failed
      # re do again
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = tmpdir)
      print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      write_fst(fread(file.path(tmpdir, fn)), out_fst_file,100)
      print(paste0("read: ", timetaken(pt)))
      unlink(file.path(tmpdir, fn))
      gc()
    }
  })) # 507 econds
}

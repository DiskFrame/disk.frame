#' Automatically read and convert every single CSV file within the zip file to disk.frame format
#' @param zipfile The zipfile
#' @param outdir The output directory for disk.frame
#' @param ... passed to fread
#' @param validation.check should the function perform a check at the end to check for validatity of output. It can detect issues with conversion
#' @param overwrite overwrite output directory
#' @import dplyr fst fs
#' @importFrom glue glue
#' @importFrom future.apply future_lapply
#' @importFrom utils unzip
#' @export
#' @return a list of disk.frame
# TODO add all the options of fread into the ... as future may not be able to deal with it
zip_to_disk.frame = function(zipfile, outdir, ..., validation.check = F, overwrite = T) {
  files = unzip(zipfile, list=T)
  
  fs::dir_create(outdir)
  
  tmpdir = tempfile(pattern = "tmp_zip2csv")
  
  dotdotdots = list(...)
  
  dfs = future.apply::future_lapply(files$Name, function(fn) {
  #dfs = lapply(files$Name, function(fn) {
    outdfpath = file.path(outdir, fn)
    overwrite_check(outdfpath, T)
    unzip(zipfile, files = fn, exdir = tmpdir)
    
    
    # lift the domain of csv_to_disk.frame so it accepts a list
    cl = purrr::lift(csv_to_disk.frame)
    
    ok = c(
      list(infile = file.path(tmpdir, fn), outdir = outdfpath, overwrite = overwrite),
      dotdotdots)
    
    #csv_to_disk.frame(, outdfpath, overwrite = overwrite, ...)
    cl(ok)
  })

  dfs  
}

#' Validate and auto-correct read and convert every single file within the zip file to df format
#' @importFrom glue glue
#' @importFrom utils unzip
#' @importFrom data.table timetaken fread
#' @import dplyr
#' @import fst
#' @rdname zip_to_disk.frame
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

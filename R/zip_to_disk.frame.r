#' `zip_to_disk.frame` is used to read and convert every CSV file within the zip
#' file to disk.frame format
#' @param zipfile The zipfile
#' @param outdir The output directory for disk.frame
#' @param ... passed to fread
#' @param validation.check should the function perform a check at the end to check for validity of output. It can detect issues with conversion
#' @param overwrite overwrite output directory
#' @import fst
#' @importFrom glue glue
#' @importFrom future.apply future_lapply
#' @importFrom utils unzip
#' @export
#' @family ingesting data
#' @return a list of disk.frame
#' @examples 
#' # create a zip file containing a csv
#' csvfile = tempfile(fileext = ".csv")
#' write.csv(cars, csvfile)
#' zipfile = tempfile(fileext = ".zip")
#' zip(zipfile, csvfile)
#' 
#' # read every file and convert it to a disk.frame
#' zip.df = zip_to_disk.frame(zipfile, tempfile(fileext = ".df"))
#' 
#' # there is only one csv file so it return a list of one disk.frame
#' zip.df[[1]]
#' 
#' # clean up
#' unlink(csvfile)
#' unlink(zipfile)
#' delete(zip.df[[1]])
# TODO do NSE better here. add all the options of fread into the ... as future may not be able to deal with it
zip_to_disk.frame = function(zipfile, outdir, ..., validation.check = FALSE, overwrite = TRUE) {
  files = unzip(zipfile, list=TRUE)
  
  if(!dir.exists(outdir)) {
    dir.create(outdir)
  }
  
  tmpdir = tempfile(pattern = "tmp_zip2csv")

  dfs = future.apply::future_lapply(files$Name, function(fn, ...) {
    outdfpath = file.path(outdir, fn)
    overwrite_check(outdfpath, TRUE)
    unzip(zipfile, files = fn, exdir = tmpdir)

    # lift the domain of csv_to_disk.frame so it accepts a list
    cl = purrr::lift(csv_to_disk.frame)
    
    ok = c(
      list(infile = file.path(tmpdir, fn), outdir = outdfpath, overwrite = overwrite),
      dotdotdots)
    
    #csv_to_disk.frame(, outdfpath, overwrite = overwrite, ...)
    cl(ok)
  }, future.seed=TRUE)
  dfs  
}

#' `validate_zip_to_disk.frame` is used to validate and auto-correct read and convert every single file within the zip file to disk.frame format
#' @importFrom glue glue
#' @importFrom utils unzip
#' @importFrom data.table timetaken fread
#' @importFrom fst read_fst
#' @rdname zip_to_disk.frame
#' @noRd
validate_zip_to_disk.frame = function(zipfile, outdir) {
  files = unzip(zipfile, list=TRUE)
  
  if(!dir.exists(outdir)) {
    stop(glue("The output directory {outdir} does not exist.\n Nothing to validate."))
  }
  
  tmpdir = tempfile(pattern = "tmp_zip2csv")
  
  # check if files are ok
  system.time(lapply(files$Name, function(fn) {
    message(fn)
    out_fst_file = file.path(outdir, paste0(fn,".fst"))
    
    if(file.exists(out_fst_file)) {
      tryCatch({
        # the output file already exists
        # read it and if it errors then the file might be corrupted, so 
        # read it again and write again
        pt = proc.time()
        fst::read_fst(out_fst_file, as.data.table = TRUE)
        message(paste0("checking(read): ", timetaken(pt))); pt = proc.time()
      }, error = function(e) {
        message(e)
        pt = proc.time()
        unzip(zipfile, files = fn, exdir = tmpdir)
        message(paste0("unzip: ", timetaken(pt))); pt = proc.time()
        fst::write_fst(data.table::fread(file.path(tmpdir, fn)), out_fst_file,100)
        message(paste0("read: ", timetaken(pt)))
        unlink(file.path(tmpdir, fn))
        gc()
      })
      message("output already exists")
      return(NULL)
    } else {
      # if the output file doesn't exists then the process might have failed
      # re do again
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = tmpdir)
      message(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      fst::write_fst(data.table::fread(file.path(tmpdir, fn)), out_fst_file,100)
      message(paste0("read: ", timetaken(pt)))
      unlink(file.path(tmpdir, fn))
      gc()
    }
  })) # 507 econds
}

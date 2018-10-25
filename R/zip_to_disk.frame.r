#' Automatically read and convert every single file within the zip file to disk.frame format
#' @param zipfile The zipfile
#' @param outdir The output directory for disk.frame
#' @import glue
#' @import dplyr
#' @import fst
#' @import future
#' @import future.apply
# TODO add all the options of fread into the ... as future may not be able to deal with it
zip_to_disk.frame = function(zipfile, outdir, ..., col.names = NULL, colClasses = NULL, replace = F, validation.check = F, parallel = T, compress = 50) {
  files = unzip(zipfile, list=T)
  
  if(!dir.exists(outdir)) dir.create(outdir)
  
  tmpdir = tempfile(pattern = "tmp_zip2csv")
  
  if(parallel) {
    #browser()
    system.time(future.apply::future_lapply(files$Name, function(fn) {
      print(fn)
      out_fst_file = file.path(outdir, paste0(fn,".fst"))
      
      if(!replace & file.exists(out_fst_file)) {
        print("output already exists")
        return(NULL)
      }
      
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = tmpdir)
      print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      if(is.null(col.names)) {
        write_fst(fread(file.path(tmpdir, fn),colClasses = colClasses), paste0(out_fst_file,".tmp"),compress)
      } else {
        write_fst(fread(file.path(tmpdir, fn),colClasses = colClasses, col.names =col.names), paste0(out_fst_file,".tmp"),compress)
      }
      
      print(paste0("read: ", timetaken(pt)))
      file.rename(paste0(out_fst_file,".tmp"), out_fst_file)
      unlink(file.path(tmpdir, fn))
      gc()
    }))
  } else {
    system.time(lapply(files$Name, function(fn) {
      print(fn)
      out_fst_file = file.path(outdir, paste0(fn,".fst"))
      
      if(file.exists(out_fst_file)) {
        print("output already exists")
        return(NULL)
      }
      
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = tmpdir)
      print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      write_fst(fread(file.path(tmpdir, fn)), paste0(out_fst_file,".tmp"),100)
      print(paste0("read: ", timetaken(pt)))
      file.rename(paste0(out_fst_file,".tmp"), out_fst_file)
      unlink(file.path(tmpdir, fn))
      gc()
    }))
  }
  
  
  # validate 
  if(validation.check) validate_zip_to_disk.frame(zipfile, outdir)
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
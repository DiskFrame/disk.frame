
#' Automatically read and convert every single file within the zip file to df format
#' @import glue
#' @import dplyr
#' @import fst
#' @import future
zip_to_disk.frame = function(zipfile, outpath, ... , replace = F, validation.check = F, parallel = T) {
  files = unzip(zipfile, list=T)
  
  if(!dir.exists(outpath)) dir.create(outpath)
  
  if(parallel) {
    system.time(future_lapply(files$Name, function(fn) {
      print(fn)
      out_fst_file = file.path(outpath, paste0(fn,".fst"))
      
      if(!replace & file.exists(out_fst_file)) {
        print("output already exists")
        return(NULL)
      }
      
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = "fm_perf")
      print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      write_fst(fread(file.path("fm_perf", fn)), paste0(out_fst_file,".tmp"),100)
      print(paste0("read: ", timetaken(pt)))
      file.rename(paste0(out_fst_file,".tmp"), out_fst_file)
      unlink(file.path("fm_perf", fn))
      gc()
    }))
  } else {
    system.time(lapply(files$Name, function(fn) {
      print(fn)
      out_fst_file = file.path(outpath, paste0(fn,".fst"))
      
      if(file.exists(out_fst_file)) {
        print("output already exists")
        return(NULL)
      }
      
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = "fm_perf")
      print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      write_fst(fread(file.path("fm_perf", fn)), paste0(out_fst_file,".tmp"),100)
      print(paste0("read: ", timetaken(pt)))
      file.rename(paste0(out_fst_file,".tmp"), out_fst_file)
      unlink(file.path("fm_perf", fn))
      gc()
    }))
  }
  
  
  # validate 
  if(validation.check) validate_zip_to_disk.frame(zipfile, outpath)
}

# validate_zip_to_disk.frame(zipfile, outpath)
#' Validate and auto-correct read and convert every single file within the zip file to df format
#' @import glue
#' @import dplyr
#' @import fst
#' @import future
validate_zip_to_disk.frame = function(zipfile, outpath) {
  files = unzip(zipfile, list=T)
  
  if(!dir.exists(outpath)) {
    stop(glue("The output directory {outpath} does not exist.\n Nothing to validate."))
  }
  # check if files are ok
  system.time(lapply(files$Name, function(fn) {
    print(fn)
    out_fst_file = file.path(outpath, paste0(fn,".fst"))
    
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
        unzip(zipfile, files = fn, exdir = "fm_perf")
        print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
        write_fst(fread(file.path("fm_perf", fn)), out_fst_file,100)
        print(paste0("read: ", timetaken(pt)))
        unlink(file.path("fm_perf", fn))
        gc()
      })
      print("output already exists")
      return(NULL)
    } else {
      # if the output file doesn't exists then the process might have failed
      # re do again
      pt = proc.time()
      unzip(zipfile, files = fn, exdir = "fm_perf")
      print(paste0("unzip: ", timetaken(pt))); pt = proc.time()
      write_fst(fread(file.path("fm_perf", fn)), out_fst_file,100)
      print(paste0("read: ", timetaken(pt)))
      unlink(file.path("fm_perf", fn))
      gc()
    }
  })) # 507 econds
}
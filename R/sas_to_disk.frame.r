#' Convert a SAS file (.sas7bdat format) to disk.frame via CSVs
#' @param inpath input SAS7BDAT file
#' @param outpath output disk.frame
#' @importFrom future %<-%
#' @rdname sas_to_csv
sas_to_disk.frame = function(inpath, outpath, nchunks = disk.frame::recommend_nchunks(inpath)) {
  files = file.path(outpath, paste0(1:nchunks,".fst"))
  ready = rep(FALSE, nchunks) | file.exists(files)
  # ready = c(rep(TRUE, 96), rep(FALSE, 4))
  extracting = rep(FALSE, nchunks)
  
  fs::dir_create(outpath)
  
  while(!all(ready)) {
    done1 = FALSE
    extracting_jobs = FALSE
    for(w in which(!ready)) {
      incsv = file.path("outcsv", w, paste0("_", w-1,".csv"))
      if(file.exists(incsv)) {
        done1 = TRUE
        ready[w] = TRUE
        ok %<-% {
          fst::write_fst(data.table::fread(incsv), file.path(outpath, paste0(w,".fst.tmp")))
          file.rename(file.path(outpath, paste0(w,".fst.tmp")), file.path(outpath, paste0(w,".fst")))
          file.remove(incsv)
          gc()
        }
        message(glue::glue("converting: {w} of {nchunks}; time: {Sys.time()}"))
      } else if (!extracting_jobs & !extracting[w]) {
        done1 = TRUE
        extracting_jobs = TRUE
        extracting[w] <- TRUE
        ok %<-% {
          sas_to_csv(inpath, w, nchunks)
        }
        message(glue::glue("extracting: {w} of {nchunks}; time: {Sys.time()}"))
      }
    }
    if(!done1) {
      message(glue::glue("didn't get any work: {Sys.time()}"))
      Sys.sleep(18)
    }
  }
}

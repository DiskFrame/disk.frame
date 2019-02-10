#' Convert a SAS file (.sas7bdat format) to disk.frame via CSVs
#' @param inpath input SAS7BDAT file
#' @param outpath output disk.frame
#' @importFrom future %<-%
#' @rdname sas_to_csv
sas_to_disk.frame = function(inpath, outpath, nchunks = disk.frame::recommend_nchunks(inpath)) {
  files = file.path(outpath, paste0(1:nchunks,".fst"))
  ready = rep(F, nchunks) | file.exists(files)
  # ready = c(rep(T, 96), rep(F, 4))
  extracting = rep(F, nchunks)
  
  fs::dir_create(outpath)
  
  while(!all(ready)) {
    done1 = F
    extracting_jobs = F
    for(w in which(!ready)) {
      incsv = file.path("outcsv", w, paste0("_", w-1,".csv"))
      if(file.exists(incsv)) {
        done1 = T
        ready[w] = T
        ok %<-% {
          fst::write_fst(data.table::fread(incsv), file.path(outpath, paste0(w,".fst.tmp")))
          file.rename(file.path(outpath, paste0(w,".fst.tmp")), file.path(outpath, paste0(w,".fst")))
          file.remove(incsv)
          gc()
        }
        print(glue::glue("converting: {w} of {nchunks}; time: {Sys.time()}"))
      } else if (!extracting_jobs & !extracting[w]) {
        done1 = T
        extracting_jobs = T
        extracting[w] <- T
        ok %<-% {
          sas_to_csv(inpath, w, nchunks)
        }
        print(glue::glue("extracting: {w} of {nchunks}; time: {Sys.time()}"))
      }
    }
    if(!done1) {
      print(glue::glue("didn't get any work: {Sys.time()}"))
      Sys.sleep(18)
    }
  }
}

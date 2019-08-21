#' Convert a SAS file (.sas7bdat) format to CSV or disk.frame by chunk
#' @param infile the SAS7BDAT file
#' @param chunk which convert of nchunks to convert
#' @param nchunks number of chunks
#' @param sas2csvpath path to sas2csv.exe
#' @param sep separater of the CSV file, defaults to |
#' @noRd
sas_to_csv <- function(infile, chunk, nchunks = disk.frame::recommend_nchunks(fs::file_size(infile)), sas2csvpath = "sas2csv/sas2csv.exe", sep="|") {
  if(!file.exists(sas2csvpath)) {
    stop("You must have the sas2csv.exe installed. Only Windows is supported at the moment. Please contact the author")
  }
  sasfile = glue::glue('"{infile}"')
  fs::dir_create(file.path("outcsv"))
  fs::dir_create(file.path("outcsv", chunk))
  options = glue::glue("-o outcsv/{chunk}/ -d {sep} -c -n {nchunks} -k {paste(chunk-1,collapse = ' ')} -m")
  
  cmd = paste(sas2csvpath, sasfile, options)
  system(cmd)
}

#' Convert a SAS file (.sas7bdat format) to disk.frame via CSVs
#' @param inpath input SAS7BDAT file
#' @param outpath output disk.frame
#' @param nchunks number of chunks
#' @param sep separater of the intermediate CSV file, defaults to |
#' @param remove_csv TRUE/FALSE. Remove the intermediate CSV after usage?
#' @importFrom future %<-%
#' @noRd
sas_to_disk.frame = function(inpath, outpath, nchunks = disk.frame::recommend_nchunks(inpath), sas2csvpath = "sas2csv/sas2csv.exe", sep = "|", remove_csv = T) {
  files = file.path(outpath, paste0(1:nchunks,".fst"))
  ready = rep(F, nchunks) | file.exists(files)
  # ready = c(rep(T, 96), rep(F, 4))
  extracting = rep(F, nchunks)
  
  fs::dir_create(outpath)
  fs::dir_delete(file.path("outcsv"))

  print("this program converts SAS datasets to CSV first before conversion to disk.frame.")
  print(glue::glue("the intermediate CSVs are here: {file.path(getwd(), 'outcsv')}"))
  
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
          if(remove_csv) {
            file.remove(incsv)
          }
          gc()
        }
        print(glue::glue("converting: {w} of {nchunks}; time: {Sys.time()}"))
      } else if (!extracting_jobs & !extracting[w]) {
      
        done1 = T
        extracting_jobs = T
        extracting[w] <- T
        ok %<-% {
          sas_to_csv(inpath, w, nchunks, sas2csvpath, sep = sep)
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

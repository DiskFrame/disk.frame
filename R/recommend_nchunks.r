#' Recommend number of chunks based on input size
#' @description Computes the recommended number of chunks to break a data.frame
#' into. It can accept filesizes in bytes (as integer) or a data.frame
#' @param df a disk.frame or the file size in bytes of a CSV file holding the
#'   data
#' @param type only = "csv" is supported. It indicates the file type
#'   corresponding to file size `df`
#' @param minchunks the minimum number of chunks. Defaults to the number of CPU
#'   cores (without hyper-threading)
#' @param conservatism a multiplier to the recommended number of chunks. The
#'   more chunks the smaller the chunk size and more likely that each chunk can
#'   fit into RAM
#' @param ram_size The amount of RAM available which is usually computed. Except on RStudio with R3.6+
#' @importFrom pryr object_size
#' @importFrom utils memory.limit
#' @export
#' @examples
#' # recommend nchunks based on data.frame
#' recommend_nchunks(cars)
#'
#' # recommend nchunks based on file size ONLY CSV is implemented at the moment
#' recommend_nchunks(1024^3)
recommend_nchunks <- function(df, type = "csv", minchunks = data.table::getDTthreads(), conservatism = 8, ram_size = df_ram_size()) {
  
  dfsize = 0
  if ("data.frame" %in% class(df)) {
    # the df's size in gigabytes
    dfsize = as.numeric(pryr::object_size(df))/1024/1024/1024
  } else if ("disk.frame" %in% class(df)) {
    return(nchunks(df))
  } else if (is.numeric(df) & type == "csv") {
    # assume that df is the estimated number of bytes of the data
    dfsize = df/1024/1024/1024
  } else {
    dfsize = df/1024/1024/1024
  }

  # ram_size = df_ram_size()
    
  # the number physical cores not counting hyper threaded ones as 2; they are counted as 1
  nc = data.table::getDTthreads() #parallel::detectCores(logical = FALSE)
  
  
  max(round(dfsize/ram_size*conservatism)*nc, minchunks)
}


#' Get the size of RAM in gigabytes
#'
#' @return integer of RAM in gigabyte (GB)
#' @export
#' @importFrom bit64 as.integer64.character
#' @examples
#' # returns the RAM size in gigabyte (GB)
#' df_ram_size() 
df_ram_size <- function() {
  #browser()
  tryCatch({
    ram_size = NULL
    # the amount of memory available in gigabytes
    if (Sys.info()[["sysname"]] == "Windows") {
      majorv = as.integer(version$major)
      minorv = as.integer(strsplit(version$minor, ".", fixed=TRUE)[[1]][1])
      if((majorv>=3 & minorv >= 6) | majorv >= 4) {
        ram_size <- system("wmic MemoryChip get Capacity", intern=TRUE) %>% 
          map(~strsplit(.x, " ")) %>% 
          unlist %>% 
          map(~bit64::as.integer64.character(.x)/1024^3) %>% 
          unlist %>% 
          sum(na.rm=TRUE)
      } else {
        ram_size = memory.limit()/1024
      }
        
      if(.Platform$GUI == "RStudio") {
        if(is.null(ram_size) | is.na(ram_size)) {
          message("You are running RStudio with R 3.6+ on Windows. There is a bug with RAM size detection.")
          message("And disk.frame can't determine your RAM size using manual methods.")
          message("Going to assume your RAM size is 16GB (gigabyte). The program will continue to run.")
          message("")
          message("")
          message("Please report a bug at https://github.com/xiaodaigh/disk.frame/issues")
          message("Include this in your bug report:")
          message(system("wmic MemoryChip get Capacity", intern=TRUE))
          message("")
          message("")
          #message("The option disk.frame.ram_size is not set. 
          #message("To set the ram_size, do options(disk.frame_ram_size = your_ram_size_in_gigabytes)")
          ram_size = 16
        }
      } 
    } else {
      #ram_size = as.numeric(system('grep MemTotal /proc/meminfo', ignore.stdout = TRUE) / 1024)
      os = R.version$os
      if (length(grep("^darwin", os))) {
        a = substring(system("sysctl hw.memsize", intern = TRUE), 13)
      } else {
        a = system('grep MemTotal /proc/meminfo', intern = TRUE)
      }
      l = strsplit(a, " ")[[1]]
      l = as.numeric(l[length(l)-1])
      ram_size = l/1024^2
      #ram_size = benchmarkme::get_ram()/1024/1024/1024
    } 
    
    if(is.null(ram_size)) {
      warning("RAM size not detected. Assumme you have at least 16GB of RAM")
      ram_size = 16
    } else if(is.na(ram_size)) {
      warning("RAM size not detected. Assumme you have at least 16GB of RAM")
      ram_size = 16
    }
    # assume at least 1G of RAM
    ram_size = max(ram_size, 1, na.rm = TRUE)
    
    return(ram_size)
  }, error = function(e) {
    warning("RAM size can't be determined. Assume you have 16GB of RAM.")
    warning("Please report this error github.com/xiaodaigh/disk.frame/issues")
    warning(glue::glue("Please include your operating system, R version, and if using RStudio the Rstudio version number"))
    return(16)
  })
}

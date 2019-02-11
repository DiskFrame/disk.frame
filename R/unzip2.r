# from https://stackoverflow.com/questions/42457395/how-to-read-sizes-of-packed-files-inside-zip-archive-using-r

unzip2 <- function (zipfile, files = NULL, list = FALSE, list.verbose = FALSE, overwrite = TRUE, 
                    junkpaths = FALSE, exdir = ".", unzip = "internal", setTimes = FALSE) {
  if (identical(unzip, "internal")) {
    if (!list && !missing(exdir)) 
      dir.create(exdir, showWarnings = FALSE, recursive = TRUE)
    res <- .External(utils:::C_unzip, zipfile, files, exdir, list, 
                     overwrite, junkpaths, setTimes)
    if (list) {
      dates <- as.POSIXct(res[[3]], "%Y-%m-%d %H:%M", tz = "UTC")
      data.frame(Name = res[[1]], Length = res[[2]], Date = dates, 
                 stringsAsFactors = FALSE)
    }
    else invisible(attr(res, "extracted"))
  }
  else {
    WINDOWS <- .Platform$OS.type == "windows"
    if (!is.character(unzip) || length(unzip) != 1L || !nzchar(unzip)) 
      stop("'unzip' must be a single character string")
    zipfile <- path.expand(zipfile)
    if (list) {
      dashl <- if (list.verbose) "-lv" else "-l"
      res <- if (WINDOWS) 
        system2(unzip, c(dashl, shQuote(zipfile)), stdout = TRUE)
      else system2(unzip, c(dashl, shQuote(zipfile)), stdout = TRUE, 
                   env = c("TZ=UTC"))
      l <- length(res)
      res2 <- res[-c(1, 3, l - 1, l)]
      con <- textConnection(res2)
      on.exit(close(con))
      z <- read.table(con, header = TRUE, as.is = TRUE)
      dt <- paste(z$Date, z$Time)
      formats <- if (max(nchar(z$Date) > 8)) 
        c("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y")
      else c("%m-%d-%y", "%d-%m-%y", "%y-%m-%d")
      slash <- any(grepl("/", z$Date))
      if (slash) 
        formats <- gsub("-", "/", formats)
      formats <- paste(formats, "%H:%M")
      for (f in formats) {
        zz <- as.POSIXct(dt, tz = "UTC", format = f)
        if (all(!is.na(zz))) 
          break
      }
      z[, "Date"] <- zz
      z <- z[, colnames(z) != "Time"]
      nms <- c("Name", "Length", "Date")
      z[, c(nms, setdiff(colnames(z), nms))]
    }
    else {
      args <- c("-oq", shQuote(zipfile))
      if (length(files)) 
        args <- c(args, shQuote(files))
      if (exdir != ".") 
        args <- c(args, "-d", shQuote(exdir))
      system2(unzip, args, stdout = NULL, stderr = NULL, 
              invisible = TRUE)
      invisible(NULL)
    }
  }
}
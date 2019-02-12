source("inst/fannie_mae/00_setup.r")
library(disk.frame)
library(speedglm)
library(biglm)

acqall_dev = disk.frame(file.path(outpath, "appl_mdl_data"))
library(speedglm)

head(acqall_dev)

#' A streaming function for speedglm
#' @param df a disk.frame
stream_shglm <- function(df) {
  i = 0
  is = sample(nchunks(df), replace = F)
  function(reset = F) {
    #browser()
    if(reset) {
      print("reset")
      i <<- 0
    } else {
      i <<- i + 1
      #if (i > 3) {
      if (i > nchunks(df)) {
        return(NULL)
      }
      print(glue::glue("streaming: {i}/{nchunks(df)}"))
      return(get_chunk(df, is[i]))
      #return(get_chunk(df, i))
    }
  }
}

acqall_dev1 = acqall_dev %>% delayed(~{
  .x[,oltv_band := cut(oltv, c(-Inf, seq(60,100,by=20)))]
  .x[,dti_band := addNA(cut(dti, c(-Inf, c(20,100), Inf)))]
  .x
})

streamacq = stream_shglm(acqall_dev1)

m = bigglm(
  default_next_12m ~ oltv_band-1, 
  data = streamacq, 
  family=binomial())
summary(m)

# not gonna work
shglm(default_next_12m ~ oltv_band -1 #+ dti_band + - 1
      , 
      datafun = streamacq, family=binomial())
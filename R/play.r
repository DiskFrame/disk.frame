#' Play the recorded lazy operations
#' @param dataframe A data.frame
#' @param recordings A recording the expression, globals and packages using create_chunk_mapper
play <- function(dataframe, recordings) {
  for(recording in recordings) {
    tmp_env = list2env(recording$globals)
    
    # replace .disk.frame.chunk with dataframe in the function
    code = eval(bquote(substitute(.(recording$expr), list(.disk.frame.chunk=quote(dataframe)))))
    
    # execute the delayed function
    dataframe = eval(code, envir = tmp_env)
  }
  dataframe
}
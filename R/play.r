#' Play the recorded lazy operations
#' @param dataframe A data.frame
#' @param recordings A recording the expression, globals and packages using create_chunk_mapper
play <- function(dataframe, recordings) {
  for(recording in recordings) {
    tmp_env = list2env(recording$globals)
    one_recording_as_string = paste0(deparse(recording$expr), collapse = "")
    code = str2lang(sprintf("dataframe %%>%% %s", one_recording_as_string))
    dataframe = eval(code, envir = tmp_env)
  }
  dataframe
}
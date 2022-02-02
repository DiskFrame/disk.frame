#' Turn a string of the form /partion1=val/partion2=val2 into data.frame
#' @param path_strs The paths in string form to break into partition format
split_string_into_df <- function(path_strs) {
  paths = dirname(path_strs) %>% unique
  list_of_partitions = stringr::str_split(paths, "/")
  
  tmp = mapply(function(partition, path) {
    part_val = stringr::str_split(partition, "=")
    tmp = lapply(part_val, function(part_val, lvl) {
      tmp = data.frame(partition = part_val[2])
      names(tmp) = part_val[1]
      tmp
      }) %>% 
      do.call(cbind, .)
    
    tmp$.disk.frame.sub.path = path
    
    tmp
  }, list_of_partitions,  paths, SIMPLIFY = FALSE) %>% data.table::rbindlist()
  
  tmp
}

if(F) {
  df = disk.frame("C:/temp/ok.df") %>% 
    filter(partition == 1)
}

#' Get the partitioning structure of a folder
#' @param df a disk.frame whose paths will be used to determine if it's
#'   folder-partitioned disk.frame
#' @importFrom utils type.convert
get_partition_paths <- function(df) {
  stopifnot("disk.frame" %in% class(df))
  path = tools::file_path_as_absolute(attr(df, "path"))
  
  allowed_paths = path
  
  meta_path = file.path(path, ".metadata")
  
  is_partitioned = FALSE
  df_of_partitions = NULL
  
  # if it's a partitioned structure allow more search paths than root
  if (length(setdiff(list.dirs(path, recursive = FALSE), meta_path)) >= 1) {
    lf = list.files(path, full.names = FALSE, pattern="fst", recursive=TRUE)
    
    # create a data.frame of the paths so it can be filtered
    df_of_partitions = split_string_into_df(lf)
    # infer the types
    df_of_partitions = utils::type.convert(df_of_partitions, as.is=TRUE)
    
    # if there is a filter operation, filter the above to figure out
    allowed_paths = df_of_partitions$.disk.frame.sub.path
    
    # filter for some paths if necessary
    partition_filter_info = attr(df, "partition_filter")
    if (!is.null(partition_filter_info)) {
      # apply filter
      df_of_partitions = eval(partition_filter_info$expr, list(dataframe=df_of_partitions))
      
      allowed_paths = df_of_partitions$.disk.frame.sub.path
    }
    is_partitioned = TRUE
  }
  
  # now go through the allowed paths
  list(paths=file.path(attr(df, "path"), allowed_paths), is_partitioned=is_partitioned, df = df_of_partitions)
  
  # TODO check all files sit within the same structure
}

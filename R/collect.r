#' Bring the disk.frame into R
#'
#' Bring the disk.frame into RAM by loading the data and running all lazy
#' operations as data.table/data.frame or as a list
#' @param x a disk.frame
#' @param parallel if TRUE the collection is performed in parallel. By default
#'   if there are delayed/lazy steps then it will be parallel, otherwise it will
#'   not be in parallel. This is because parallel requires transferring data
#'   from background R session to the current R session and if there is no
#'   computation then it's better to avoid transferring data between session,
#'   hence parallel = FALSE is a better choice
#' @param ... not used
#' @importFrom data.table data.table as.data.table
#' @importFrom purrr map_dfr
#' @importFrom dplyr collect select mutate
#' @return collect return a data.frame/data.table
#' @examples
#' cars.df = as.disk.frame(cars)
#' # use collect to bring the data into RAM as a data.table/data.frame
#' collect(cars.df)
#'
#' # clean up
#' delete(cars.df)
#' @export
#' @rdname collect
collect.disk.frame <- function(x, ..., parallel = !is.null(attr(x,"recordings"))) {
  cids = get_chunk_ids(x, full.names = TRUE, strip_extension = FALSE)
  # obtain filters from structure
  partitioned_paths_or_not = get_partition_paths(x)
  
  if (partitioned_paths_or_not$is_partitioned) {
    partitioned_paths = partitioned_paths_or_not$paths
    if(length(partitioned_paths) >= 1) {
      # filter the cids based on the paths
      tmp = data.frame(paths = cids) %>% 
        mutate(dirname = dirname(paths)) %>% 
        inner_join(data.frame(dirname = sapply(partitioned_paths, tools::file_path_as_absolute)), by = "dirname")
      cids = tmp$paths
    }
  }
  
  if(nchunks(x) > 0) {
    if(parallel) {
      tmp<-future.apply::future_lapply(cids, function(.x, meh) {
          if(partitioned_paths_or_not$is_partitioned) {
            dirpath = dirname(.x)
            tmp2 = partitioned_paths_or_not$df %>%
              mutate(fullpath = file.path(attr(x, "path") %>% tools::file_path_as_absolute(), .disk.frame.sub.path)) 
            
            tmp2a = tmp2 %>% 
              filter(fullpath == dirpath) %>% 
              mutate(.check=1)
            
            stopifnot(nrow(tmp2a) == 1)
            
            tmp3 = get_chunk.disk.frame(x, .x, full.names = TRUE, partitioned_info = tmp2a)
            return(tmp3)
          } else {
            return(get_chunk.disk.frame(x, .x, full.names = TRUE))
          }
      }, future.seed = NULL)
      return(rbindlist(tmp))
    } else {
      tmp<-lapply(cids, function(.x, meh) {
        if(partitioned_paths_or_not$is_partitioned) {
          dirpath = dirname(.x)
          tmp2 = partitioned_paths_or_not$df %>%
            mutate(fullpath = file.path(attr(x, "path") %>% tools::file_path_as_absolute(), .disk.frame.sub.path)) 
          
          tmp2a = tmp2 %>% 
            filter(fullpath == dirpath) %>% 
            mutate(.check=1)
          
          stopifnot(nrow(tmp2a) == 1)
          
          tmp3 = get_chunk.disk.frame(x, .x, full.names = TRUE, partitioned_info = tmp2a)
          return(tmp3)
        } else {
          return(get_chunk.disk.frame(x, .x, full.names = TRUE))
        }
      })
      return(rbindlist(tmp))
    }
  } else {
    data.table()
  }
}

#' @param simplify Should the result be simplified to array
#' @export
#' @rdname collect
#' @return collect_list returns a list
#' @examples
#' cars.df = as.disk.frame(cars)
#' 
#' # returns the result as a list
#' collect_list(cmap(cars.df, ~1))
#' 
#' # clean up
#' delete(cars.df)
collect_list <- function(x, simplify = FALSE, parallel = !is.null(attr(x,"recordings")), ...) {
  # get the chunk ids
  cids = get_chunk_ids(x, full.names = TRUE, strip_extension = FALSE)
  
  if(length(cids) > 0) {
    list_of_results = NULL
    if (parallel) {
      list_of_results = future.apply::future_lapply(cids, function(.x) {
        get_chunk(x, .x, full.names = TRUE)
      }, future.seed=TRUE)
    } else {
      list_of_results = lapply(cids, function(cid) {
        get_chunk(x, cid, full.names = TRUE)
      })
    }
    
    if (simplify) {
      return(simplify2array(list_of_results))
    } else {
      return(list_of_results)
    }
  } else {
    list()
  }
}

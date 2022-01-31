#' One Stage function
#' @param x the input
#' @param listx a list
#' @param na.rm Remove NAs. TRUE of FALSE 
#' @param ... additional options
#' @rdname one-stage-group-by-verbs
#' @export
var_df.chunk_agg.disk.frame <- function(x, na.rm = FALSE) {
  # Guard against Github #241
  data.frame(
    sumx = sum(x, na.rm = na.rm), 
    sumsqrx = sum(x^2, na.rm = na.rm), 
    nx = length(x) - ifelse(na.rm, sum(is.na(x)), 0)
  )
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom dplyr bind_rows
var_df.collected_agg.disk.frame <- function(listx)  {
  df = Reduce(dplyr::bind_rows, listx)

  sumlengthx = sum(df$nx)

  first_part = sum(df$sumsqrx) / sumlengthx
  second_part = sum(df$sumx) / sumlengthx

  # unbiased adjustment
  (first_part - second_part^2) * sumlengthx / (sumlengthx-1)
}

#' @export
#' @rdname one-stage-group-by-verbs
sd_df.chunk_agg.disk.frame <- var_df.chunk_agg.disk.frame

#' @export
#' @rdname one-stage-group-by-verbs
sd_df.collected_agg.disk.frame <- function(listx)  {
  sqrt(var_df.collected_agg.disk.frame(listx))
}
 

#' mean chunk_agg
#' @export
#' @rdname one-stage-group-by-verbs
mean_df.chunk_agg.disk.frame <- function(x, na.rm = FALSE, ...) {
  sumx = sum(x, na.rm = na.rm)
  lengthx = length(x) - ifelse(na.rm, sum(is.na(x)), 0)
  data.frame(sumx = sumx, lengthx = lengthx)
}

#' mean collected_agg
#' @export
#' @rdname one-stage-group-by-verbs
mean_df.collected_agg.disk.frame <- function(listx) {
  sum(sapply(listx, function(x) x$sumx))/sum(sapply(listx, function(x) x$lengthx))
}

#' @export
#' @rdname one-stage-group-by-verbs
sum_df.chunk_agg.disk.frame <- function(x, ...) {
  sum(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
sum_df.collected_agg.disk.frame <- function(listx, ...) {
  sum(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
min_df.chunk_agg.disk.frame <- function(x, ...) {
  min(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
min_df.collected_agg.disk.frame <- function(listx, ...) {
  min(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
max_df.chunk_agg.disk.frame <- function(x, ...) {
  max(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
max_df.collected_agg.disk.frame <- function(listx, ...) {
  max(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom stats median
median_df.chunk_agg.disk.frame <- function(x, ...) {
  stats::median(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
median_df.collected_agg.disk.frame <- function(listx, ...) {
  stats::median(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom dplyr n
n_df.chunk_agg.disk.frame <- function(...) {
  dplyr::n()
}

#' @export
#' @rdname one-stage-group-by-verbs
n_df.collected_agg.disk.frame <- function(listx, ...) {
  sum(unlist(listx))
}

#' @export
#' @rdname one-stage-group-by-verbs
length_df.chunk_agg.disk.frame <- function(x, ...) {
  length(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
length_df.collected_agg.disk.frame <- function(listx, ...) {
  length(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
any_df.chunk_agg.disk.frame <- function(x, ...) {
  any(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
any_df.collected_agg.disk.frame <- function(listx, ...) {
  any(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
all_df.chunk_agg.disk.frame <- function(x, ...) {
  all(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
all_df.collected_agg.disk.frame <- function(listx, ...) {
  all(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
n_distinct_df.chunk_agg.disk.frame <- function(x, na.rm = FALSE, ...) {
  if(na.rm) {
    setdiff(unique(x), NA)
  } else {
    unique(x)
  }
}

#' @export
#' @importFrom dplyr n_distinct
#' @rdname one-stage-group-by-verbs
n_distinct_df.collected_agg.disk.frame <- function(listx, ...) {
  n_distinct(unlist(listx))
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom stats quantile
quantile_df.chunk_agg.disk.frame <- function(x, ...) {
  stats::quantile(x, ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom stats quantile
quantile_df.collected_agg.disk.frame <- function(listx, ...) {
  stats::quantile(unlist(listx), ...)
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom stats quantile
IQR_df.chunk_agg.disk.frame <- function(x, na.rm = FALSE, ...) {
  stats::quantile(x, c(0.25, 0.75), na.rm = na.rm)
  #100
}

#' @export
#' @rdname one-stage-group-by-verbs
#' @importFrom stats quantile
IQR_df.collected_agg.disk.frame <- function(listx, ...) {
  q25 = unlist(listx)[c(TRUE, FALSE)]
  q75 = unlist(listx)[c(FALSE, TRUE)]
  quantile(q75, 0.75) - quantile(q25, 0.25)
}


#' A function to parse the summarize function
#' @importFrom dplyr filter select pull
#' @importFrom purrr map_dfr
#' @rdname group_by
#' @export
summarise.grouped_disk.frame <- function(.data, ...) {
  
  class(.data) <- c("summarized_disk.frame", "disk.frame")
  
  # get all components of the summarise
  dotdotdot = rlang::enexprs(...)
  
  # convert any quosure to labels
  for (i in seq_along(dotdotdot)) {
    dotdotdot[[i]] <- rlang::as_label(dotdotdot[[i]]) %>% 
      parse(text = .) %>% 
      .[[1]]
  }
  
  attr(.data, "summarize_code") = dotdotdot
  
  # detect any global variables
  args_str = sapply(dotdotdot, function(code) {
    deparse(code) %>% 
      paste0(collapse="")
  }) %>% paste(collapse = ", ")
  
  
  attr(.data, "summarize_globals_and_pkgs") = 
    find_globals_recursively(
      parse(text=sprintf("list(%s)", args_str))[[1]], 
      parent.frame()
    )
  
  return(.data)
}

#' @export
#' @rdname group_by
summarize.grouped_disk.frame = summarise.grouped_disk.frame

#' Group by within each disk.frame
#' @description
#' The disk.frame group by operation perform group WITHIN each chunk. This is
#' often used for performance reasons. If the user wishes to perform group-by,
#' they may choose to use the `hard_group_by` function which is expensive as it
#' reorganizes the chunks by the shard key.
#' @seealso hard_group_by
#' @param .data a disk.frame
#' @param .add from dplyr
#' @param .drop from dplyr
#' @param ... same as the dplyr::group_by
#' @importFrom dplyr group_by_drop_default
#' @importFrom rlang enexpr
#' @export
#' @rdname group_by
# learning from https://docs.dask.org/en/latest/dataframe-groupby.html
group_by.disk.frame <- function(.data, ..., .add = FALSE, .drop = stop("disk.frame does not support `.drop` in `group_by` at this stage")) {
  class(.data) <- c("grouped_disk.frame", "disk.frame")
  
  # using rlang is a neccesary evil here as I need to deal with !!! that is supported by group_by etc
  group_by_cols = rlang::enexprs(...)
  
  # convert any quosure to labels
  for (i in seq_along(group_by_cols)) {
    group_by_cols[[i]] <- group_by_cols[[i]] %>% 
      rlang::as_label() %>% 
      parse(text=.) %>% 
      .[[1]]
  }
  
  attr(.data, "group_by_cols") = group_by_cols
  
  # detect any global variables
  args_str = sapply(group_by_cols, function(code) {
    deparse(code) %>% 
      paste0(collapse="")
  }) %>% paste(collapse = ", ")
  
  
  attr(.data, "group_by_globals_and_pkgs") = find_globals_recursively(parse(text=sprintf("list(%s)", args_str))[[1]], parent.frame())
  
  .data
}


#' @export
#' @importFrom dplyr summarize
#' @rdname group_by
summarize.disk.frame <- function(.data, ...) {
  
  class(.data) <- c("summarized_disk.frame", "disk.frame")
  
  # get all components of the summarise
  dotdotdot = rlang::enexprs(...)
  
  # convert any quosure to labels
  for (i in seq_along(dotdotdot)) {
    dotdotdot[[i]] <- rlang::as_label(dotdotdot[[i]]) %>% 
      parse(text=.) %>% 
      .[[1]]
  }
  
  attr(.data, "summarize_code") = dotdotdot
  
  # detect any global variables
  args_str = sapply(dotdotdot, function(code) {
    deparse(code) %>% 
      paste0(collapse="")
  }) %>% paste(collapse = ", ")
  
  
  attr(.data, "summarize_globals_and_pkgs") = 
    find_globals_recursively(
      parse(text=sprintf("list(%s)", args_str))[[1]], 
      parent.frame()
    )
  
  return(.data)
}


#' @export
#' @importFrom dplyr summarize
#' @rdname group_by
summarise.disk.frame <- summarize.disk.frame

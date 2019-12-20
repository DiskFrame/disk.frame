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
  ca_code = generate_summ_code(...)
  
  chunk_summ_code = ca_code$chunk_summ_code
  agg_summ_code = ca_code$agg_summ_code
  
  # get the by variables
  group_by_cols = purrr::map_chr(attr(.data, "group_by_cols"), ~{deparse(.x)})
  
  list(group_by_cols = group_by_cols, chunk_summ_code = chunk_summ_code, agg_summ_code = agg_summ_code)
  
  # generate full code
  code_to_run = glue::glue("chunk_group_by({group_by_cols}) %>% chunk_summarize({chunk_summ_code}) %>% collect %>% group_by({group_by_cols}) %>% summarize({agg_summ_code})")
  
  class(.data) <- c("summarized_disk.frame", "disk.frame")
  attr(.data, "summarize_code") = code_to_run
  .data
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
#' @param add from dplyr
#' @param .drop from dplyr
#' @param ... same as the dplyr::group_by
#' @importFrom dplyr group_by_drop_default
#' @export
#' @rdname group_by
# learning from https://docs.dask.org/en/latest/dataframe-groupby.html
group_by.disk.frame <- function(.data, ..., add = FALSE, .drop = dplyr::group_by_drop_default(.data)) {
  class(.data) <- c("grouped_disk.frame", "disk.frame")
  attr(.data, "group_by_cols") = substitute(list(...))[-1]
  .data
}

#' @export
#' @importFrom dplyr summarize
#' @rdname group_by
summarize.disk.frame <- function(.data, ...) {
  # comment summarize.grouped_disk.frame
  #warning("`summarize.disk.frame`'s behaviour has changed. Please use `chunk_summarize` if you wish to `dplyr::summarize` to each chunk")
  
  ca_code = generate_summ_code(...)
  
  chunk_summ_code = ca_code$chunk_summ_code
  agg_summ_code = ca_code$agg_summ_code
  
  # generate full code
  code_to_run = glue::glue("chunk_summarize({chunk_summ_code}) %>% collect %>% summarize({agg_summ_code})")
  
  class(.data) <- c("summarized_disk.frame", "disk.frame")
  attr(.data, "summarize_code") = code_to_run
  .data
}

#' Helper function to generate summarisation code
#' @importFrom data.table setDT setkey
#' @importFrom utils methods
#' @noRd
generate_summ_code <- function(...) {
  
  code = substitute(list(...))[-1]
  expr_id = 0
  temp_varn = 0
  
  list_of_chunk_agg_fns <- as.character(utils::methods(class = "chunk_agg.disk.frame"))
  list_of_collected_agg_fns <- as.character(utils::methods(class = "collected_agg.disk.frame"))
  
  # generate the chunk_summarize_code
  summarize_code = purrr::map_dfr(code, ~{
    
    expr_id <<- expr_id  + 1
    # parse the function into table form for easy interrogration
    gpd = getParseData(parse(text = deparse(.x)), includeText = TRUE); 
    grp_funcs = gpd %>% filter(token == "SYMBOL_FUNCTION_CALL") %>% select(text) %>% pull
    grp_funcs = grp_funcs %>% paste0("_df")
    
    # search in the space to find functions name `fn`.chunk_agg.disk.frame
    # only allow one such functions for now TODO improve it
    num_of_chunk_functions = sum(sapply(unique(grp_funcs), function(x) exists(paste0(x, ".chunk_agg.disk.frame"))))
    num_of_collected_functions= sum(sapply(unique(grp_funcs), function(x) exists(paste0(x, ".collected_agg.disk.frame"))))
    
    # the number chunk and aggregation functions must match
    stopifnot(num_of_chunk_functions == num_of_collected_functions)
    
    # keep only grp_functions
    grp_funcs= grp_funcs[sapply(grp_funcs, function(x) exists(paste0(x, ".chunk_agg.disk.frame")))]
    
    if(num_of_chunk_functions == 0) {
      stop(sprintf("There must be at least one summarization function in %s", deparse(.x)))
    } else if (num_of_chunk_functions > 1) {
      stop(sprintf("Two or more summarisation functions are detected in \n\n```\n%s\n```\n\nThese are currently not supported by {disk.frame} at the moment \n    * Nestling (like mean(sum(x) + y)) or \n    * combinations (like sum(x) + mean(x))\n\nIf you want this implemented, please leave a comment or upvote at: https://github.com/xiaodaigh/disk.frame/issues/228 \n\n", deparse(.x)))
    }
    
    # check to see if the mean is only two from parent 0, otherwise it would a statement in the form of 1 + mean(x)
    # which isn't supported
    data.table::setDT(gpd)
    data.table::setkey(gpd, parent)
    if (gpd[id == gpd[id == gpd[(paste0(text,"_df") == grp_funcs) & (token == "SYMBOL_FUNCTION_CALL"), parent], parent], parent] != 0) {
      stop(sprintf("Combining summarization with other operations \n\n```\n%s\n```\n\nThese are currently not supported by {disk.frame} at the moment \n    * combinations (like sum(x) + 1)\n* combinations (like list(sum(x)))\n\nIf you want this implemented, please leave a comment or upvote at: https://github.com/xiaodaigh/disk.frame/issues/228 \n\n", deparse(.x)))
    }
    
    temp_varn <<- temp_varn + 1
    grp_funcs_wo_df = sapply(grp_funcs, function(grp_func) substr(grp_func, 1, nchar(grp_func)-3))
    
    tmpcode = deparse(evalparseglue("substitute({deparse(.x)}, list({grp_funcs_wo_df} = quote({grp_funcs}.chunk_agg.disk.frame)))")) %>% paste0(collapse = " ")
    
    chunk_code = data.frame(assign_to = as.character(glue::glue("tmp{temp_varn}")), expr = tmpcode, stringsAsFactors = FALSE)
    
    chunk_code$orig_code = deparse(.x)
    chunk_code$expr_id = expr_id
    chunk_code$grp_fn = grp_funcs
    chunk_code$name = ifelse(is.null(names(code[expr_id])), "", names(code[expr_id]))
    
    # create the aggregation code
    chunk_code$agg_expr = as.character(glue::glue("{grp_funcs}.collected_agg.disk.frame({paste0(chunk_code$assign_to, collapse=', ')})"))
    
    #print(sapply(chunk_code, typeof))
    chunk_code
  })
  
  chunk_summ_code = paste0(summarize_code$assign_to, "=list(", summarize_code$expr, ")") %>% paste0(collapse = ", ")
  
  agg_code_df = summarize_code %>% 
    select(expr_id, name, agg_expr, orig_code) %>% 
    unique %>% 
    transmute(agg_code = paste0(ifelse(name == "", paste0("`", orig_code, "` = "), paste0(name, "=")), agg_expr))
  
  agg_summ_code = paste0(agg_code_df$agg_code, collapse = ",")
  
  list(chunk_summ_code = chunk_summ_code, agg_summ_code = agg_summ_code)
}


#' @export
#' @importFrom dplyr summarize
#' @rdname group_by
summarise.disk.frame <- summarize.disk.frame






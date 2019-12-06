# keeping a list of defined group_by operations ---------------------------
disk.frame_chunk_fns <- list(
  sd = c("sum_sqr", "sum", "length"),
  var = c("sum_sqr", "sum", "length"),
  mean = c("sum", "length"),
  sum = "sum",
  max = "max",
  min = "min",
  median = "median",
  length = "length"
)

disk.frame_agg_fns <- list(
  mean = "mean_agg",
  sum = "sum",
  max = "max",
  min = "min",
  median = "median",
  sd = "sd_agg",
  var = "var_agg",
  length = "sum"
)


mean_agg <- function(sumx, lengthx) {
  sum(sumx)/sum(lengthx)
}

sum_sqr <- function(x) {
  sum(x^2)
}

var_agg <- function(sum_sqr_x, sumx, lengthx) {
  # print(sum(lengthx))
  sumlengthx = sum(lengthx)
  
  first_part = sum(sum_sqr_x) / sumlengthx
  second_part = sum(sumx) / sumlengthx
  
  # unbiased adjustment
  (first_part - second_part^2) * sumlengthx / (sumlengthx-1)
}

sd_agg <- function(...) {
  sqrt(var_agg(...))
}



# #' Register a new group_by function
# #' @export
# register_group_by_fn <- function(list_chunk_fns, list_agg_fns) {
#   disk.frame_chunk_fns <<- c(disk.frame_chunk_fns, list_chunk_fns)
#   disk.frame_agg_fns <<- c(disk.frame_chunk_fns, list_agg_fns)
# }


#' A function to parse the summarize function
#' @importFrom dplyr filter select pull
#' @imporFrom purr map_dfr
#' @export
summarise.grouped_disk.frame <- function(.data, ...) {
  code = substitute(list(...))[-1]
  expr_id = 0
  temp_varn = 0
  # generate the chunk_summarize_code
  summarize_code = purrr::map_dfr(code, ~{
    expr_id <<- expr_id  + 1
    # parse the function into table form for easy interrogration
    gpd = getParseData(parse(text = deparse(.x)), includeText = TRUE); 
    grp_funcs = gpd %>% filter(token == "SYMBOL_FUNCTION_CALL") %>% select(text) %>% pull
    
    # only allow one such functions
    stopifnot(length(grp_funcs) == 1)
    
    # only allow supported functions
    stopifnot(grp_funcs %in% names(disk.frame_chunk_fns))
    
    # only allow supported functions
    stopifnot(grp_funcs %in% names(disk.frame_agg_fns))
    
    # look up the corresponding chunk function
    chunk_fns = disk.frame_chunk_fns[[grp_funcs]]
    
    if(length(chunk_fns) == 1) {
      chunk_fns = list(chunk_fns)
    } 
      
    code1 = .x
    
    chunk_code = purrr::map_dfr(chunk_fns, ~{
      chunk_fn = .x
      temp_varn <<- temp_varn + 1
      tmpcode = deparse(evalparseglue("substitute({deparse(code1)}, list({grp_funcs} = {chunk_fn}))")) %>% paste0(collapse = " ")
      data.frame(assign_to = as.character(glue::glue("tmp{temp_varn}")), expr = tmpcode, stringsAsFactors = FALSE)
    }); chunk_code
    
    chunk_code$orig_code = deparse(.x)
    chunk_code$expr_id = expr_id
    chunk_code$grp_fn = grp_funcs
    chunk_code$name = ifelse(is.null(names(code[expr_id])), "", names(code[expr_id]))
    
    # create the aggregation code
    agg_fns = disk.frame_agg_fns[[grp_funcs]]
    chunk_code$agg_expr = glue::glue("{agg_fns}({paste0(chunk_code$assign_to, collapse=', ')})")
    
    #print(sapply(chunk_code, typeof))
    chunk_code
  })
  
  #summarize_code
  
  chunk_summ_code = paste0(summarize_code$assign_to, "=", summarize_code$expr) %>% paste0(collapse = ", ")
  
  agg_code_df = summarize_code %>% 
    select(expr_id, name, agg_expr, orig_code) %>% 
    unique %>% 
    transmute(agg_code = paste0(ifelse(name == "", paste0("`", orig_code, "` = "), paste0(name, "=")), agg_expr))
    
  agg_summ_code = paste0(agg_code_df$agg_code, collapse = ",")
  
  # get the by variables
  group_by_cols = purrr::map_chr(attr(.data, "group_by_cols"), ~{deparse(.x)})
  
  list(group_by_cols = group_by_cols, chunk_summ_code = chunk_summ_code, agg_summ_code = agg_summ_code)
  
  # generate full code
  code_to_run = glue::glue("chunk_group_by({group_by_cols}) %>% chunk_summarize({chunk_summ_code}) %>% collect() %>% group_by({group_by_cols}) %>% summarize({agg_summ_code})")
  
  class(.data) <- c("summarized_disk.frame", "disk.frame")
  attr(.data, "summarize_code") = code_to_run
  .data
}

#' @export
summarize.grouped_disk.frame = summarise.grouped_disk.frame

#' Group by within each disk.frame
#' @description
#' The disk.frame group by operation perform group WITHIN each chunk. This is
#' often used for performance reasons. If the user wishes to perform group-by,
#' they may choose to use the `hard_group_by` function which is expensive as it
#' reorganizes the chunks by the shard key.
#' @seealso hard_group_by
#' @param .data a disk.frame
#' @param ... same as the dplyr::group_by
#' @export
#' @rdname group_by
# learning from https://docs.dask.org/en/latest/dataframe-groupby.html
group_by.disk.frame <- function(.data, ..., add = FALSE, .drop = group_by_drop_default(.data)) {
  class(.data) <- c("grouped_disk.frame", "disk.frame")
  attr(.data, "group_by_cols") = substitute(list(...))[-1]
  .data
}






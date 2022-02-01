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
#' @importFrom globals findGlobals
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
collect.summarized_disk.frame <-
  function(x, ..., parallel = !is.null(attr(x, "recordings"))) {
    dotdotdot <- attr(x, 'summarize_code')
    group_by_vars = attr(x, "group_by_cols")
    
    # look at the group by and summarise codes and figure out which columns need to be 
    # srckeep
    df_to_find_cols = fst::read_fst(get_chunk_ids(x, full.names = TRUE)[1], from=1, to=1)
    
    cols_in_summ = lapply(dotdotdot, function(one) {
      globals::findGlobals(one, envir = list2env(df_to_find_cols, parent=globalenv()))
    }) %>% unlist %>% unique
    
    cols_in_group_by = lapply(group_by_vars, function(one) {
      globals::findGlobals(one, envir = list2env(df_to_find_cols, parent=globalenv()))
    }) %>% unlist %>% unique
    
    src_keep_cols = intersect(names(df_to_find_cols), c(cols_in_summ, cols_in_group_by) %>% unique)
    
    x = srckeep(x, src_keep_cols)
  
    # make a copy
    dotdotdot_chunk_agg <- dotdotdot
    dotdotdot_collected_agg <- dotdotdot
    
    i = 1
    for (a_call in dotdotdot) {
      # obtain the function call name
      func_call_str = paste0(deparse(a_call[[1]]), collapse = "")
      
      # parse(...) returns an expression, but I just want the sole symbol which
      # can be extracted with [[1]]
      func_call_chunk_agg = parse(text = paste0(func_call_str, "_df.chunk_agg.disk.frame"))[[1]]
      # replace the function call with the chunk_agg_function
      dotdotdot_chunk_agg[[i]][[1]] = func_call_chunk_agg
      
      func_call_collected_agg = paste0(func_call_str, "_df.collected_agg.disk.frame")
      # replace the function call with the chunk_agg_function
      dotdotdot_collected_agg[[i]] = parse(text = sprintf(
        "%s(%s)",
        func_call_collected_agg,
        paste0(".disk.frame.tmp", i)
      ))[[1]]
      i = i + 1
      # TODO extract global variables from here and store them in the global
    }
    
    group_by_vars = attr(x, "group_by_cols")
    
    # figure out how many group by arguments there are
    n_grp_args = length(group_by_vars)
    
    # generate a function call with as many arguments
    x_as.disk.frame = x
    class(x_as.disk.frame) = "disk.frame"
    first_stage_code = eval(parse(
      text = sprintf(
        "quote(chunk_group_by(x_as.disk.frame, %s))",
        paste0(rep_len("NULL", n_grp_args), collapse = ", ")
      )
    ))
    
    if (n_grp_args >= 1) {
      for (i in 1:n_grp_args) {
        first_stage_code[[i + 2]] = group_by_vars[[i]]
      }
    }
    
    group_by_globals_list = attr(x_as.disk.frame, "group_by_globals_and_pkgs")$globals
    
    if(is.null(group_by_globals_list)) {
      eval_clos = parent.frame()
    } else {
      eval_clos = list2env(group_by_globals_list, parent=parent.frame())
    }
      
    # TODO add appropriate environment
    # tmp_df = eval(first_stage_code, envir=environment(), enclos = eval_clos
    tmp_df = eval(first_stage_code, group_by_globals_list)
    
    
    n_summ_args = length(dotdotdot_chunk_agg)
    
    chunk_summ_code =
      eval(parse(text = sprintf(
        "quote(chunk_summarise(tmp_df, %s))",
        paste0("NULL", 1:n_summ_args, collapse = ", ")
      )))
    
    
    chunk_summ_code_str = chunk_summ_code %>%
      deparse %>%
      paste0(collapse = "")
    
    for (i in 1:n_summ_args) {
      lhs = sprintf(".disk.frame.tmp%d", i)
      rhs = paste0(deparse(dotdotdot_chunk_agg[[i]]), collapse = "")
      
      tmp_code = paste0("NULL", i)
      chunk_summ_code_str = gsub(
        pattern = tmp_code,
        sprintf("%s=list(%s)", lhs, rhs),
        chunk_summ_code_str,
        fixed = TRUE
      )
    }
    
    summarize_globals_list = attr(x_as.disk.frame, "summarize_globals_and_pkgs")$globals
    
    if(is.null(summarize_globals_list)) {
      summ_eval_clos = parent.frame()
    } else {
      summ_eval_clos = list2env(summarize_globals_list, parent=parent.frame())
    }
    
    #tmp2 = collect(eval(parse(text = chunk_summ_code_str), envir = environment(), enclos=summ_eval_clos))
    tmp2 = collect(eval(parse(text = chunk_summ_code_str), envir = summarize_globals_list))
    
    second_stage_code = eval(parse(text = sprintf(
      "quote(group_by(tmp2, %s))", paste0(rep_len("NULL", n_grp_args), collapse = ", ")
    )))
    
    if (n_grp_args >= 1) {
      for (i in 1:n_grp_args) {
        second_stage_code[[i + 2]] = group_by_vars[[i]] %>% 
          deparse() %>% 
          paste0(collapse="") %>% 
          sprintf("`%s`", .) %>% 
          parse(text=.) %>% 
          .[[1]]
      }
    }
    
    tmp3 = eval(second_stage_code)
    
    n_summ2_args = length(dotdotdot_collected_agg)
    # final stage of summary
    chunk_summ2_code =
      eval(parse(text = sprintf(
        "quote(summarise(tmp3, %s))",
        paste0(rep_len("NULL", n_summ2_args), collapse = ", ")
      )))
    
    names_chunk_summ_code = names(dotdotdot_chunk_agg)
    for (i in 1:n_summ_args) {
      chunk_summ2_code[[i + 2]] = dotdotdot_collected_agg[[i]]
    }
    
    tmp4 = eval(chunk_summ2_code)
    
    names_tmp4 = names(tmp4)
    
    orig_names = sapply(dotdotdot, function(code) {
      code %>%
        deparse %>%
        paste0(collapse = "")
    })
    
    
    names(tmp4)[(n_grp_args + 1):length(names_tmp4)] = ifelse(names_chunk_summ_code ==
                                                                "",
                                                              orig_names,
                                                              names_chunk_summ_code)
    
    return(tmp4)
  }

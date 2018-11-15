source("inst/fannie_mae/0_setup.r")
library(disk.frame)
acqall1 = disk.frame(file.path(outpath, "appl_mdl_data"))

# 5d develop a function to test all variables
check_which_is_best <- function(df, target, features, monotone_constraints, format_fns) {
  prev_pred = NULL
  ws = NULL
  vars_scr = NULL
  
  while(length(features) > 0) {
    # try every var
    res = furrr::future_map(1:length(features), ~disk.frame::add_var_to_scorecard(
      df, 
      target, 
      features[.x], 
      monotone_constraints = monotone_constraints[.x],
      prev_pred = prev_pred,
      format_fn = format_fns[[.x]]
    ))
    
    # which has the best auc
    w = which.max(map_dbl(res, ~.x$auc))
    ws = c(ws, w)
    feature_to_fit = features[w]
    print(glue::glue("choosen: {feature_to_fit}"))
    
    var_scr1 = res[w]
    eval(parse(text = glue::glue("vars_scr = c(vars_scr, list({feature_to_fit} = var_scr1))")))
    
    features = features[-w]
    format_fns = format_fns[-w]
    monotone_constraints = monotone_constraints[-w]
    
    prev_pred = var_scr1$prev_pred
  }
  list(vars_scr)
}

res = check_which_is_best(
  acqall1, 
  "default_next_12m", 
  c("oltv", "dti", "orig_trm", "cscore_b", "num_bo"),
  c(1     , 1    ,     1    ,       -1  ,  -1      ),
  c(map(1:4, ~base::I), list(as.numeric)))

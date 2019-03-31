source("inst/fannie_mae_10pct/00_setup.r")
library(disk.frame)
library(xgboost)
acqall_dev = disk.frame(file.path(outpath, "appl_mdl_data_sampled_dev"))
  
add_var_to_scorecard = disk.frame:::add_var_to_scorecard

# 5d develop a function to test all variables
check_which_is_best <- function(df, target, features, monotone_constraints, format_fns, weight=NULL) {
  #browser()
  prev_pred = NULL
  ws = NULL
  vars_scr = NULL
  
  while(length(features) > 0) {
    # try every var
    #browser()
    res = furrr::future_map(1:length(features), ~add_var_to_scorecard(
    #res = purrr::map(1:length(features), ~add_var_to_scorecard(
      df, 
      target,
      features[.x], 
      monotone_constraints = monotone_constraints[.x],
      prev_pred = prev_pred,
      format_fn = format_fns[[.x]],
      weight,
      save_model_fname = glue::glue("{features[.x]}.xgbm")
    ))
    
    # which has the best auc
    w = which.max(map_dbl(res, ~.x$auc))
    ws = c(ws, w)
    feature_to_fit = features[w]
    print(glue::glue("choosen: {feature_to_fit}"))
    
    var_scr1 = res[[w]]
    eval(parse(text = glue::glue("vars_scr = c(vars_scr, list({feature_to_fit} = var_scr1))")))
    
    features = features[-w]
    format_fns = format_fns[-w]
    monotone_constraints = monotone_constraints[-w]
    
    prev_pred = var_scr1$prev_pred
  }
  vars_scr
}

num_vars =     c("mi_pct", "orig_amt", "orig_rt", "ocltv", "mi_type", "cscore_c", "oltv", "dti", "orig_trm", "cscore_b", "num_bo", "num_unit", "orig_dte", "frst_dte")
num_vars_mon = c(0       ,  1        , 1        , 1      , 0        ,  -1        , 1     , 1    ,     1     ,  -1        ,  -1     , 0         ,       0   ,  0)
num_var_fmt_fn = c(map(1:10, ~base::I), map(1:2, ~as.numeric), map(1:2, ~function(x) {
  as.numeric(substr(x, 4, 7))
}))

cat_vars = setdiff(names(acqall_dev), num_vars) %>% 
  setdiff(
    c("loan_id", "default_next_12m", "first_default_date", "mths_to_1st_default", "zip_3", "weight"))

pt = proc.time()
res_all = check_which_is_best(
  acqall_dev, 
  "default_next_12m", 
  features = c(num_vars, cat_vars),
  monotone_constraints = c(num_vars_mon, rep(0, length(cat_vars))),
  format_fns = c(num_var_fmt_fn, map(1:length(cat_vars), ~base::I)),
  weight = "weight"
)
timetaken(pt)

aucs = map_dbl(res_all, ~.x$auc)
plot(
  aucs,
  main = "Fannie Mae Application Scorecard AUC", 
  ylab="AUC", 
  xlab = "Number of features")

if(F) {
  library(gganimate)
  auc_dt = data.table(num_of_vars = 1:length(aucs), auc = aucs)
  auc_dt %>% 
    ggplot(aes(num_of_vars, auc)) + 
    geom_point() +
    transition_states(
      num_of_vars,
      transition_length  = 2,
      state_length = 1
    ) + 
    enter_fade() +
    exit_shrink() + 
    ease_aes('sine-in-out') 
}

#saveRDS(res_all, "model.rds")

if(F) {
  rescat = check_which_is_best(
    acqall_dev, 
    "default_next_12m", 
    c("seller.name", "oltv", "state", "dti", "mi_type"),
    c(0            , 1     , 0      , 1    , 0     ),
    map(1:5, ~base::I)
  )
  
  
  system.time(res <- check_which_is_best(
    acqall1, 
    "default_next_12m", 
    c("ocltv", "cscore_c", "mi_type", "oltv", "dti", "orig_trm", "cscore_b", "num_bo"),
    c(1      , -1        , 0        , 1     , 1    ,     1     , -1        ,  -1     ),
    c(map(1:7, ~base::I), list(as.numeric))))
}



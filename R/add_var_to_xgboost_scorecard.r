#' Add one variable to the XGBoost scorecard
#' @param df a disk.frame
#' @param target a character string indicating the variable name of the target
#' @param feature a character string idnicating the variable name of the feature
#' @param monotone_constraints 1 higher value of feature equals higher success, -1 = the opposite, 0 = neither
#' @param prev_pred a vector of score equal to the nrow(df) which is the previous predictions used for hot-start
#' @param format_fn a function to transform the feature vector before fitting a model
#' @import xgboost
#' @export
add_var_to_scorecard <- function(df, target, feature, monotone_constraints = 0, prev_pred = NULL, format_fn = base::I) {
  #browser()
  xy = df %>%
    srckeep(c(target, feature)) %>%
    collect(parallel = T)
  
  # evaluate
  code = glue::glue("xy = xy %>% mutate({feature} = format_fn({feature}))")
  eval(parse(text = code))
  
  dtrain <- xgboost::xgb.DMatrix(label = xy[,target, with = F][[1]], data = as.matrix(xy[,c(feature), with = F]))
  
  if(is.null(prev_pred)) {
    pt = proc.time()
    m2 <- xgboost::xgboost(
      data=dtrain, 
      nrounds = 1, 
      objective = "binary:logitraw", 
      tree_method="exact",
      monotone_constraints = monotone_constraints,
      base_score = sum(xy[,target, with = F][[1]])/nrow(xy)
    )
    timetaken(pt)
  } else {
    setinfo(dtrain, "base_margin", prev_pred)
    pt = proc.time()
    m2 <- xgboost::xgboost(
      data=dtrain, 
      nrounds = 1, 
      objective = "binary:logitraw", 
      tree_method="exact",
      monotone_constraints = monotone_constraints
    )
    timetaken(pt)
  }
  
  #browser()
  
  # code to obtain the unique for merging
  udtrain_df = eval(parse(text = glue::glue("xy[, .({feature} = unique({feature}))]")))
  udtrain = xgboost::xgb.DMatrix(as.matrix(udtrain_df))
  a3 = predict(m2, udtrain, predcontrib = T) %>% data.table
  
  setnames(a3, names(a3), c("score", "bias"))
  
  
  #if(is.null(prev_pred)) {
  stopifnot(length(unique(a3$bias)) == 1)
  bias = a3[1, bias]
  #}
  
  a4 = cbind(udtrain_df, a3)
  
  # summarise a4 to get the cutting points
  code = glue::glue("a5 = a4[,.({feature} = max({feature})), score][order({feature}),]")
  eval(parse(text  = code))
  list(model = m2, bins = a5, prev_pred = predict(m2, dtrain), bias = bias, auc = auc(xy[,target, with = F][[1]], predict(m2, dtrain)))
}

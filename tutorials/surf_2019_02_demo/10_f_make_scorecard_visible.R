source("inst/fannie_mae/00_setup.r")
library(disk.frame)

auc = disk.frame:::auc
acqall_val = disk.frame(file.path(outpath, "appl_mdl_data_sampled_val2"))
df = acqall_val
#the model
mdl = readRDS("model.rds")

#' score one variable
score_one_var <- function(feature, x, bins, bias, format_fn = base::I) {
  print(feature)
  # numeric features (post format_fn) has only two columns in bins
  if(ncol(bins) == 2) {
    x1 = format_fn(x)
    binsNA = evalparseglue("bins[is.na({feature}),]")
    bins = evalparseglue("bins[!is.na({feature}),]")
    score = bins$score[cut(x1, c(-Inf, bins[[2]], Inf))]
    
    if(nrow(binsNA) == 1) {
      score[is.na(x1)] = binsNA[[1]]
    }
  } else {
    x1 = format_fn(x)
    x1df = evalparseglue("data.table({feature} = x1)")
    x1df_scr = evalparseglue("merge(x1df, bins, by = '{feature}', all.x=T)")
    score = x1df_scr$score
  }
  # if there are still na then assign the bias
  #browser(expr = any(is.na(score)))
  if(any(is.na(score))) {
    warning(glue::glue("the feature `{feature}` has {sum(is.na(score))} unassigned scores out of {length(score)}, assigning them the bias = {bias}"))
    score[is.na(score)] = bias
  }
  
  score
}


#' Socre a scorecard
#' @param mdl the xg scorecard model
#' @param df a disk.frame
score_xg_scorecard <- function(df, mdl) {
  res = furrr::future_map_dfc(mdl, ~{
  #res = purrr::map_dfc(mdl, ~{
    var = df %>% 
      srckeep(.x$feature) %>% 
      collect(parallel = F)
    x = var[[1]]
    feature = .x$feature
    bins = .x$bins
    bias = .x$bias
    format_fn = .x$format_fn
    score_one_var(feature, x, bins, bias, format_fn)
  }) %>% rowSums
  res
}

plot_auc <- function(df, line = F) {
  target = df %>% 
    srckeep("default_next_12m") %>% 
    collect(parallel = F)
  
  pt = proc.time()
  target$score = score_xg_scorecard(df, mdl)
  timetaken(pt)
  
  AUC = auc(target$default_next_12m, target$score)
  GINI = 2*AUC-1
  
  target[,negscore := -score]
  setkey(target, negscore)
  cts = target[,.(ctot = (1:.N)/.N, cbad = cumsum(default_next_12m)/sum(default_next_12m))]
  
  if(line == F) {
    plot(cts[seq(1,.N, length.out=100),], type="l", xlim=c(0,1),
         main=glue::glue("On Validation - AUC: {AUC %>% round(2)}; GINI: {GINI %>% round(2)}"))
    abline(a=0,b=1,lty=2)
    abline(h=0)
    abline(v=0)
  } else {
    lines(cts[seq(1,.N, length.out=100),], lty = 3, col = "blue")
    legend("topleft", c("val AUC", "random", "dev AUC"), lty=c(1,2,3), col=c("black","blue","black"))
  }
}

system.time(plot_auc(acqall_val))

# score on whole ----------------------------------------------------------
acqall_dev = disk.frame(file.path(outpath, "appl_mdl_data_sampled_dev2"))
system.time(plot_auc(acqall_dev, line = T))

scorecard = map_dfr(mdl, ~{
  res = .x$bins
  evalparseglue("res[,feature_lbl := as.character({.x$feature})]")
  res[,variable := .x$feature]
  res %>% 
    select(variable, feature_lbl, score ) %>% 
    mutate(score = round(-score*20/log(2)))
})

saveRDS(scorecard, "scorecard.rds")
scorecard = readRDS("scorecard.rds")
#View(scorecard)

scorecard[,
          low := c(-Inf, feature_lbl[-.N]), variable]
setnames(scorecard, "feature_lbl", "high")

scorecard=scorecard[,.(variable, low, high, score)]

DT::datatable(scorecard, options = list(pageLength=20))

# score on whole ----------------------------------------------------------
# acqall = disk.frame(file.path(outpath, "appl_mdl_data"))
# system.time(plot_auc(acqall))



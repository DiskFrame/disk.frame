source("inst/fannie_mae/0_setup.r")
library(disk.frame)

acqall1 = disk.frame(file.path(outpath, "appl_mdl_data"))

# XGBoost on one var ------------------------------------------------------
system.time(xy <- acqall1[,c("default_next_12m", "oltv"), keep=c("default_next_12m", "oltv")])

# transform into format accepted by XGBoost
dtrain <- xgb.DMatrix(label = xy$default_next_12m, data = as.matrix(xy[,"oltv"]))

portfolio_default_rate = xy[,sum(default_next_12m, na.rm = T)/.N]

pt = proc.time()
m <- xgboost(
  data=dtrain, 
  nrounds = 1, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = portfolio_default_rate)
timetaken(pt)

prev_pred = predict(m, dtrain)

map_chr(xgb.dump(m), ~str_extract(.x,"\\[f0<[\\d]+\\.[\\d]+\\]")) %>% 
  keep(~!is.na(.x)) %>% 
  map_dbl(~str_extract(.x, "[\\d]+\\.[\\d]+") %>% as.numeric) %>% 
  sort %>% 
  floor -> bins

bb = xy[,.(ndef = sum(default_next_12m), .N, m1 = min(oltv), m2 = max(oltv)), .(bins = cut(oltv,c(-Inf,bins,Inf)))]
new_bins = sort(unique( bb$m2))
bb = xy[,.(ndef = sum(default_next_12m), .N), .(bins = cut(oltv,c(-Inf,new_bins,Inf)))][order(bins)]

setkey(bb, bins)
bb %>%
  filter(!is.na(bins)) %>% 
  mutate(`Orig LTV Band` = bins, `Default Rate%` = ndef/N) %>% 
  ggplot +
  geom_bar(aes(x = `Orig LTV Band`, y = `Default Rate%`), stat = 'identity') + 
  coord_flip()

if(F) {
  system.time(xyz <- acqall1[,c("default_next_12m", "oltv", "frst_dte"), keep=c("default_next_12m", "oltv", "frst_dte")])
  
  xyz[,frst_yr := substr(frst_dte,4,7) %>% as.integer]
  xyz[,frst_dte:=NULL]
  
  bb = xyz[,.(ndef = sum(default_next_12m), .N), .(bins = cut(oltv,c(-Inf,bins,Inf)), frst_yr)]
  
  bb[,binton := sum(N), bins]
  bb[,dr := ndef/binton]
  
  setkey(bb, bins)
  bb[order(frst_yr,decreasing = T),text_y_pos := cumsum(dr) - dr/2, bins]
  bb[,text := substr(frst_yr, 3,4)]
  
  bb %>%
    filter(!is.na(bins)) %>% 
    mutate(`Yr of Orig` = as.factor(frst_yr), `Orig LTV Band` = bins, `Default Rate%` = dr) %>% 
    ggplot +
    geom_bar(aes(x = `Orig LTV Band`, y = `Default Rate%`, fill = `Yr of Orig`), stat = 'identity') + 
    geom_text(aes(x = `Orig LTV Band`, y = text_y_pos, label = text)) +
    coord_flip()
}


# what if we fit them in rounded number ---------------------------------------------------------

xy[oltv >  80, oltv_round := ceiling(oltv/5)*5]
xy[oltv <= 80, oltv_round := ceiling(oltv/10)*10]
xy[oltv <= 40, oltv_round := ceiling(oltv/20)*20]
xy = xy[!is.na(oltv_round),]

xy[is.na(oltv_round),]

xy[,.N, oltv_round]

# transform into format accepted by XGBoost
dtrain <- xgb.DMatrix(label = xy$default_next_12m, data = as.matrix(xy[,"oltv_round"]))

pt = proc.time()
m <- xgboost(
  data=dtrain, 
  nrounds = 1, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = portfolio_default_rate)
timetaken(pt)

map_chr(xgb.dump(m), ~str_extract(.x,"\\[f0<[\\d]+[\\.]{0,1}[\\d]+\\]")) %>% 
  keep(~!is.na(.x)) %>% 
  map_dbl(~str_extract(.x, "[\\d]+[\\.]{0,1}[\\d]+") %>% as.numeric) %>% 
  sort -> bins

bb = xy[,.(ndef = sum(default_next_12m), .N, m1 = min(oltv_round), m2 = max(oltv_round)), .(bins = cut(oltv_round,c(-Inf,bins,Inf)))]
new_bins = sort(unique( bb$m2))
bb = xy[,.(ndef = sum(default_next_12m), .N), .(bins = cut(oltv_round,c(-Inf,new_bins,Inf)))][order(bins)]

setkey(bb, bins)
bb %>%
  filter(!is.na(bins)) %>% 
  mutate(`Orig LTV Band` = bins, `Default Rate%` = ndef/N) %>% 
  ggplot +
  geom_bar(aes(x = `Orig LTV Band`, y = `Default Rate%`), stat = 'identity') + 
  coord_flip()

# add another variable ----------------------------------------------------
target = "default_next_12m"
feature = "dti"
df = acqall1

add_var_to_scorecard <- function(df, target, feature, monotone_constraints = 0, existing_model = NULL, format_fn = base::I) {
  xy = df %>% 
    srckeep(c(target, feature)) %>%
    collect(parallel = T)
  
  # evaluate
  code = glue::glue("xy = xy %>% mutate({feature} = format_fn({feature}))")
  eval(parse(text = code))
  
  dtrain <- xgb.DMatrix(label = xy[,target, with = F][[1]], data = as.matrix(xy[,feature, with = F]))
  
  if(is.null(existing_model)) {
    pt = proc.time()
    m2 <- xgboost(
      data=dtrain, 
      nrounds = 1, 
      objective = "binary:logitraw", 
      tree_method="exact",
      monotone_constraints = monotone_constraints
    )
    timetaken(pt)
  } else {
    pt = proc.time()
    m2 <- xgboost(
      data=dtrain, 
      nrounds = 1, 
      objective = "binary:logitraw", 
      tree_method="exact",
      monotone_constraints = monotone_constraints,
      xgb_model = existing_model
    )
    timetaken(pt)
  }
  
  map_chr(xgb.dump(m2), ~str_extract(.x,"\\[f0<[\\d]+[\\.]{0,1}[\\d]+\\]")) %>% 
    keep(~!is.na(.x)) %>% 
    map_dbl(~str_extract(.x, "[\\d]+[\\.]{0,1}[\\d]+") %>% as.numeric) %>% 
    sort -> bins
  
  bb = xy[,.(ndef = sum(default_next_12m), .N, m1 = min(oltv_round), m2 = max(oltv_round)), .(bins = cut(oltv_round,c(-Inf,bins,Inf)))]
  new_bins = sort(unique( bb$m2))
  bb = xy[,.(ndef = sum(default_next_12m), .N), .(bins = cut(oltv_round,c(-Inf,new_bins,Inf)))][order(bins)]
  
  setkey(bb, bins)
  bb %>%
    filter(!is.na(bins)) %>% 
    mutate(`Orig LTV Band` = bins, `Default Rate%` = ndef/N) %>% 
    ggplot +
    geom_bar(aes(x = `Orig LTV Band`, y = `Default Rate%`), stat = 'identity') + 
    coord_flip()
}

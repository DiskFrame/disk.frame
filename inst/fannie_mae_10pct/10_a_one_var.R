source("inst/fannie_mae_10pct/00_setup.r")
library(disk.frame)

acqall1 = disk.frame(file.path(outpath, "appl_mdl_data"))

# XGBoost on one var ------------------------------------------------------
system.time(xy <- acqall1[,c("default_next_12m", "oltv"), keep=c("default_next_12m", "oltv")])

# transform into format accepted by XGBoost
dtrain <- xgb.DMatrix(label = xy$default_next_12m, data = as.matrix(xy[,"oltv"]))

portfolio_default_rate = xy[,sum(default_next_12m, na.rm = T)/.N]


# xgboost fit -------------------------------------------------------------


###################################################
# the xgboost fit
###################################################
pt = proc.time()
m <- xgboost(
  data=dtrain, 
  nrounds = 1, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = portfolio_default_rate)
timetaken(pt)

# plotting ----------------------------------------------------------------


###################################################
# plotting
###################################################
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

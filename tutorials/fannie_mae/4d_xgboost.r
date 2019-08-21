source("inst/fannie_mae/0_setup.r")
library(disk.frame)
fmdf_2yr = disk.frame(file.path(outpath,"fmdf_2yr"))

system.time(xy <- fmdf_2yr[,.(dh12m, oltv),keep=c("dh12m","oltv")])


# show how to binning -----------------------------------------------------
system.time(xy <- fmdf_2yr[,c("dh12m", "oltv"), keep=c("dh12m","oltv")])

dtrain <- xgb.DMatrix(label = xy$dh12m, data = as.matrix(xy[,"oltv"]))

pt = proc.time()
m <- xgboost(
  data=dtrain, 
  nrounds = 1, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = 32662/10067411)
timetaken(pt)


map_chr(xgb.dump(m), ~str_extract(.x,"\\[f0<[\\d]+\\.[\\d]+\\]")) %>% 
  keep(~!is.na(.x)) %>% 
  map_dbl(~str_extract(.x, "[\\d]+\\.[\\d]+") %>% as.numeric) %>% 
  sort %>% 
  floor -> xy[,"oltv"]

bb = xy[,sum(dh12m)/.N, .(bins = cut(oltv,c(-Inf,bins,Inf)))]

setkey(bb, bins)

barplot(height = bb$V1, names.arg = bb$bins)

pt = proc.time()
m2 <- xgboost(
  data=x, 
  label = y, 
  nrounds = 3, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = 32662/10067411)
timetaken(pt)


map_chr(xgb.dump(m2), ~str_extract(.x,"\\[f0<[\\d]+\\.[\\d]+\\]")) %>% 
  keep(~!is.na(.x)) %>% 
  map_dbl(~str_extract(.x, "[\\d]+\\.[\\d]+") %>% as.numeric) %>% 
  sort %>% 
  floor %>% 
  unique -> bins

bb = xy[,sum(dh12m)/.N, .(bins = cut(oltv,c(-Inf,bins,Inf)))]

setkey(bb, bins)

barplot(height = bb$V1, names.arg = bb$bins)

print(length(bins))

xgb.plot.tree(model = m)


# with penalty ------------------------------------------------------------
system.time(xy <- fmdf_2yr[,.(dh12m, oltv),keep=c("dh12m","oltv")])

dtrain <- xgb.DMatrix(label = xy$dh12m, data = as.matrix(xy$oltv))

system.time(xycv <- 
              xgb.cv(dtrain, objective="binary:logitraw", nfold = 5, nrounds=1))
xycv

pt = proc.time()
m <- xgboost(
  data=dtrain, 
  nrounds = 1, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = 32662/10067411
)
timetaken(pt)


map_chr(xgb.dump(m), ~str_extract(.x,"\\[f0<[\\d]+\\.[\\d]+\\]")) %>% 
  keep(~!is.na(.x)) %>% 
  map_dbl(~str_extract(.x, "[\\d]+\\.[\\d]+") %>% as.numeric) %>% 
  sort %>% 
  floor -> bins

bb = xy[,sum(dh12m)/.N, .(bins = cut(oltv,c(-Inf,bins,Inf)))]

setkey(bb, bins)

barplot(height = bb$V1, names.arg = bb$bins)

xgb.plot.tree(model = m)

# do a variable with NA ---------------------------------------------------
system.time(xy <- fmdf_2yr[,.(dh12m, mi_pct),keep=c("dh12m","mi_pct")])

dtrain <- xgb.DMatrix(label = xy$dh12m, data = as.matrix(xy$mi_pct))

pt = proc.time()
m <- xgboost(
  data= dtrain, 
  nrounds = 1, 
  objective = "binary:logitraw", 
  tree_method="exact",
  monotone_constraints = 1,
  base_score = 32662/10067411)
timetaken(pt)


map_chr(xgb.dump(m), ~str_extract(.x,"\\[f0<[\\d]+\\.[\\d]+\\]")) %>% 
  keep(~!is.na(.x)) %>% 
  map_dbl(~str_extract(.x, "[\\d]+[\\.[\\d]+]") %>% as.numeric) %>% 
  sort %>% 
  floor %>% 
  unique -> bins

bb = xy[,sum(dh12m)/.N, .(bins = cut(mi_pct,c(-Inf,bins,Inf)) %>% addNA)]

setkey(bb, bins)

barplot(height = bb$V1, names.arg = bb$bins)


# do other stuff ----------------------------------------------------------


# took 142
a = select_if(as.tibble(sfmdf_2yr), is.numeric) %>% 
  select(-c(mi_type))

system.time(x <- as.matrix(a))


dtrain <- xgb.DMatrix(label = sfmdf_2yr$dh12m, data = x)


xgboost(
  data=x, 
  label = sfmdf_2yr$dh12m, 
  nrounds = 2, 
  objective = "binary:logitraw", 
  tree_method="exact")



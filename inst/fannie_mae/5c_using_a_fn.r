source("inst/fannie_mae/0_setup.r")
library(disk.frame)

acqall1 = disk.frame(file.path(outpath, "appl_mdl_data"))

pt <- proc.time()
firstm <- add_var_to_scorecard(acqall1, "default_next_12m", "oltv", monotone_constraints = 1, format_fn = function(v) {
  ceiling(v / 5) * 5
})
timetaken(pt)

# now add the second variable ---------------------------------------------
pt <- proc.time()
secondm <- add_var_to_scorecard(
  acqall1, 
  "default_next_12m", 
  "dti", 
  prev_pred = firstm$prev_pred, 
  monotone_constraints = 1)
timetaken(pt)

pt <- proc.time()
thirdm <- add_var_to_scorecard(
  acqall1, 
  "default_next_12m", 
  "orig_trm", 
  prev_pred = secondm$prev_pred, 
  monotone_constraints = 1)
timetaken(pt)

pt <- proc.time()
fourthm <- add_var_to_scorecard(
  acqall1, 
  "default_next_12m", 
  "num_bo", 
  prev_pred = thirdm$prev_pred, 
  monotone_constraints = -1,
  format_fn = as.numeric)
timetaken(pt)

pt <- proc.time()
fifthm <- add_var_to_scorecard(
  acqall1, 
  "default_next_12m", 
  "cscore_b", 
  prev_pred = fourthm$prev_pred, 
  monotone_constraints = -1)
timetaken(pt)


# plot improvement --------------------------------------------------------
plot(map_dbl(list(firstm, secondm, thirdm, fourthm, fifthm), ~.x$auc), type="b", ylim = c(0.4,0.9))
abline(h = 0.7, lty = 2)


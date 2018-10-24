# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

tmp4 = disk.frame("tmp4")

head(tmp4)

system.time(a <- tmp4[
  ,.(.N, n_defaults = sum(default_12m, na.rm = T), n_goto_harp = sum(harp_12m, na.rm=T))
  ,.(monthly.rpt.prd), keep=c("default_12m","harp_12m","monthly.rpt.prd")])

a1 = a[,.(odr = sum(n_defaults)/sum(N), oh = sum(n_goto_harp)/sum(N)), monthly.rpt.prd]

setnames(a1, c("monthly.rpt.prd","odr", "oh"), c("Date", "Observed Default Rate%", "Enter HARP Rate%"))

library(ggplot2)
ggplot(a1) + 
  geom_line(aes(x=Date, y = `Observed Default Rate%`)) +
  geom_line(aes(x=Date, y = `Enter HARP Rate%`)) +
  ggtitle("Fannie Mae Observed Default Rate over time")

a[,.(sum(n_defaults), sum(N))]



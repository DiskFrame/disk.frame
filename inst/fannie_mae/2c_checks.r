# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

tmp3 = disk.frame("tmp3")

head(tmp3)

system.time(a <- tmp3[
  ,.(.N, n_defaults = sum(default_12m, na.rm = T))
  ,.(monthly.rpt.prd), keep=c("default_12m","monthly.rpt.prd")])

a1 = a[,.(odr = sum(n_defaults)/sum(N)), monthly.rpt.prd]

setnames(a1, c("monthly.rpt.prd","odr"), c("Date", "Observed Default Rate%"))

library(ggplot2)
ggplot(a1) + 
  geom_line(aes(x=Date, y = `Observed Default Rate%`)) +
  ggtitle("Fannie Mae Observed Default Rate over time")

a[,.(sum(n_defaults), sum(N))]



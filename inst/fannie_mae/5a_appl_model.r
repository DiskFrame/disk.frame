source("inst/fannie_mae/0_setup.r")
library(disk.frame)

defaults = disk.frame(file.path(outpath,"defaults.df")) %>%
  srckeep(c("loan_id", "start_date")) %>% 
  collect

defaults[,first_default_date := start_date]
month(defaults$first_default_date) <- month(defaults$first_default_date)+12

defaults = defaults[,.(loan_id, first_default_date)]

acqall = disk.frame(file.path(outpath,"acq_all.df"))

# 1:14
pt <- proc.time()
acqall1 <- left_join(
  acqall, 
  defaults, 
  by = "loan_id") %>% 
  compute(outdir = file.path(outpath, "appl_mdl_data"), overwrite = T)
timetaken(pt)

acqall2 = acqall1 %>%
  srckeep(c("orig_dte", "first_default_date")) %>% 
  mutate(orig_yr = substr(orig_dte, 4,7), yr_1st_d = year(first_default_date)) %>% 
  group_by(orig_yr, yr_1st_d, hard = F) %>% 
  summarise(n=n()) %>% 
  collect %>% 
  group_by(orig_yr, yr_1st_d) %>% 
  summarise(n = sum(n))

acqall2[,tot_n := sum(n), orig_yr]

acqall3 <- acqall2[!is.na(yr_1st_d),]

acqall3[,dr := n/tot_n]

acqall3 %>% 
  filter(orig_yr > 1999) %>% 
  mutate(`Origination Year` = orig_yr) %>% 
  ggplot +
  geom_line(aes(x = yr_1st_d, y = dr, colour = `Origination Year`)) +
  xlab("Year of Observation") +
  ylab("Ratio of defaulted accounts vs # of accts at orig") +
  scale_x_continuous(breaks=2000:2017, labels=as.character(2000:2017)) + 
  scale_y_continuous(expand = c(0, 0)) + 
  ggtitle("Fannie Mae Single Family Loans: Ratio of defaults vs # of accounts in same year of origination")






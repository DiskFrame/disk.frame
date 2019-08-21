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
  delayed(~{
    library(lubridate)
    .x[
      !is.na(first_default_date), 
      mths_to_1st_default := interval(
        as.Date(paste0("01/", frst_dte),"%d/%m/%Y"), 
        first_default_date) 
      %/% months(1)]
    
    .x[,default_next_12m := F]
    .x[!is.na(first_default_date), default_next_12m := mths_to_1st_default <= 12]
    
    res = .x[substr(frst_dte,4,7) < 2016, ]
    
    res
  }) %>% 
  compute(outdir = file.path(outpath, "appl_mdl_data"), overwrite = T)
timetaken(pt)

# default rate by year of origination
system.time(drbyyr <- acqall1 %>% 
              srckeep(c("frst_dte", "default_next_12m")) %>% 
              mutate(frst_yr = substr(frst_dte, 4, 7)) %>% 
              group_by(frst_yr, hard = F) %>% 
              summarise(ndef = sum(default_next_12m, na.rm=T), n = n()) %>% 
              collect(parallel  = T) %>% 
              group_by(frst_yr) %>% 
              summarise(ndef = sum(ndef), n = sum(n)) %>% 
              mutate(odr = ndef/n))

drbyyr %>% 
  ggplot +
  geom_line(aes(x = frst_yr %>% as.numeric, y = odr))

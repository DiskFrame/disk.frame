# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fm_with_harp = disk.frame(file.path(outpath, "fm_with_harp"))

head(fm_with_harp)

nrow(fm_with_harp)

# need a two stage summary
system.time(a_wh1 <- fm_with_harp %>% 
  srckeep(c("default_12m","monthly.rpt.prd")) %>% 
  group_by(monthly.rpt.prd) %>% 
  summarise(
    N = n(), 
    n_defaults = sum(default_12m, na.rm = T)) %>% 
  collect(parallel  = T) %>%
  group_by(monthly.rpt.prd) %>% 
  summarise(
    odr = sum(n_defaults)/sum(N)
  ) %>% 
  rename(
    Date = monthly.rpt.prd,
    `Observed Default Rate%` = odr
  ))
  

a_wh2 = a_wh1 %>% gather(key = type, value=rate, -Date)

ggplot(a_wh2) + 
  geom_line(aes(x=Date, y = rate, colour = type)) +
  ggtitle("Fannie Mae Observed Default Rate over time & HARP Conversion Rate")



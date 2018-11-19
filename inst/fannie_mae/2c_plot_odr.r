# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fm_with_harp = disk.frame(file.path(outpath, "fm_with_harp"))

head(fm_with_harp)

# need a two stage summary
system.time(a_wh1 <- fm_with_harp %>% 
  keep(c("default_12m","harp_12m","monthly.rpt.prd")) %>% 
  group_by(monthly.rpt.prd, hard = F) %>% 
  summarise(
    N = n(), 
    n_defaults = sum(default_12m, na.rm = T),
    n_goto_harp = sum(harp_12m, na.rm=T)) %>% 
  collect(parallel  = T) %>%
  group_by(monthly.rpt.prd) %>% 
  summarise(
    odr = sum(n_defaults)/sum(N),
    oh = sum(n_goto_harp)/sum(N)
  ) %>% 
  rename(
    Date = monthly.rpt.prd,
    `Observed Default Rate%` = odr,
    `HARP Conversion Rate%` = oh
  ))
  

if(F) {
  # data.table syntax
  system.time(a_wh <- tmp4[
    ,.(.N, n_defaults = sum(default_12m, na.rm = T), )
    ,.(monthly.rpt.prd), keep=c("default_12m","harp_12m","monthly.rpt.prd")])
  
  a_wh1 = a_wh[,.(odr = sum(n_defaults)/sum(N), oh = sum(n_goto_harp)/sum(N)), monthly.rpt.prd]
  setnames(a_wh1, c("monthly.rpt.prd","odr", "oh"), c("Date", "Observed Default Rate%", "HARP Conversion Rate%"))
}

a_wh2 = a_wh1 %>% gather(key = type, value=rate, -Date)

ggplot(a_wh2) + 
  geom_line(aes(x=Date, y = rate, colour = type)) +
  ggtitle("Fannie Mae Observed Default Rate over time & HARP Conversion Rate")



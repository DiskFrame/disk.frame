source("inst/fannie_mae/0_setup.r")
library(disk.frame)
library(keras)

fmdf = disk.frame(file.path(outpath, "fm_with_harp"))

head(fmdf)

nrow(fmdf)

pt = proc.time()
default_rate_over_time <- fmdf %>% 
  srckeep(c("default_12m", "monthly.rpt.prd")) %>% 
  group_by(monthly.rpt.prd, hard = F) %>% 
  summarise(ndef = sum(default_12m, na.rm=T), n = n()) %>% 
  collect %>% 
  group_by(monthly.rpt.prd) %>% 
  summarise(ndef = sum(ndef), n = sum(n)) %>% 
  mutate(default_rate = ndef/n)
timetaken(pt)

default_rate_over_time %>% 
  ggplot +
  geom_line(aes(x = monthly.rpt.prd, y = default_rate)) +
  ggtitle("Fannie Mae Default Rate%")

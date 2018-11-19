source("inst/fannie_mae/0_setup.r")
library(disk.frame)
acqall = disk.frame(file.path(outpath, "appl_mdl_data"))

pt = proc.time()
acqall_all = acqall %>%
  map.disk.frame(~{
    defs = .x[default_next_12m == T,]
    defs[,weight:=1]
    non_defs = .x[default_next_12m == F, ]
    non_defs[,weight:=10]
    
    rbindlist(list(defs, sample_frac(non_defs, 0.1)), fill = T, use.names = T)
  }, lazy = F, outdir = file.path(outpath, "appl_mdl_data_sampled"), overwrite = T)
timetaken(pt)

#acqall_all = disk.frame(file.path(outpath, "appl_mdl_data_sampled"))

acqall_dev = sample_frac(acqall_all, 0.7) %>% 
  write_disk.frame(file.path(outpath, "appl_mdl_data_sampled_dev"))

#acqall_dev = disk.frame(file.path(outpath, "appl_mdl_data_sampled_dev"))


uid_dev = acqall_dev %>% 
  srckeep("loan_id") %>% 
  mutate(loan_id = unique(loan_id)) %>% 
  collect(parallel = T) %>% 
  mutate(loan_id = unique(loan_id))
  

acqall_val = acqall_all %>% 
  anti_join(uid_dev, by = "loan_id") %>% 
  write_disk.frame(file.path(outpath, "appl_mdl_data_sampled_val"))

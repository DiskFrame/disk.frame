source("inst/fannie_mae/0_setup.r")
library(disk.frame)

vars_to_keep = c(
  Performance_Variables[1:15] %>% tolower,
  Acquisitions_Variables[-1] %>% tolower,
  c("default_12m", "harp_12m")
) %>% unique

# create a new variable dh12m which is a concatenation of default in next 12 months or going into harp next 12 months
fmdf_all = disk.frame(file.path(outpath, "fmdf_appl")) %>% 
  srckeep(vars_to_keep) %>%
  delayed(~{
    .x[,dh12m := default_12m | harp_12m]
    .x[is.na(dh12m), dh12m:=F]
    .x
  })

fmdf1 = disk.frame(file.path(outpath, "fmdf_appl"))

# took 2 mins
system.time(uid <- unique(fmdf1[substr(orig_dte ,4,7) >= "2014",unique(loan_id), keep=c("loan_id","orig_dte")]))

# set.seed(1)
# suid = sample(uid, length(uid)/100)

uiddf = data.table(loan_id = uid)
setkey(uiddf, loan_id)

#fmdf_all = disk.frame(file.path(outpath, "fmdf_appl"))
# took about 10 minutes
system.time(fmdf_2yr <- fmdf_all %>%
  map.disk.frame(~{
    setkey(.x, loan_id)
    merge(.x, uiddf, by="loan_id")
  }, 
  lazy = F, 
  outdir = file.path(outpath,"fmdf_2yr"),
  overwrite = T))

# fmdf_2yr = disk.frame(file.path(outpath,"fmdf_2yr"))
system.time(sfmdf_2yr <- sample_frac(fmdf_2yr, 1) %>% collect(parallel = F))



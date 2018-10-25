source("inst/fannie_mae/0_setup.r")

check_res = future_lapply(dir("test_fm", full.names = T), function(x) {
  tryCatch({
    map(dir(x,full.names = T), ~{
      read_fst(.x);
      NULL
    })
    return("ok")
  }, error = function(e) {
    return("error")
  })
})

# check again for errors
source("inst/fannie_mae/1b_read_from_csv.r")

# map_dfr is faster

# system.time(a1 <- lapply(dir("test_fm", full.names = T), function(dir) {
#   a = disk.frame(dir)
#   a[,.N,monthly.rpt.prd,keep=c("monthly.rpt.prd")]
# }) %>% rbindlist)
# 
# system.time(a2 <- map_dfr(dir("test_fm", full.names = T), ~{
#   a = disk.frame(.x)
#   a[,.N,monthly.rpt.prd,keep=c("monthly.rpt.prd")]
# }))

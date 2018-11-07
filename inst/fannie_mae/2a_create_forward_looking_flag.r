# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fmdf = disk.frame(file.path(outpath, "fm.df"))
harp = disk.frame(file.path(outpath, "harp.df"))

# harp contains the first date on which a loan enters into HARP
pt <- proc.time()
harp1 <- harp %>% 
  dfkeep(c("loan_id","monthly.rpt.prd")) %>% 
  group_by(loan_id, hard = F) %>% # the data is sharded by loan_id hence hard = F is fine
  summarise(first_harp_date = min(as.Date(monthly.rpt.prd, "%m/%d/%Y"))) %>% 
  collect(parallel = T) # performs the collection in parallel

cat(glue("Creating first harp date took: {timetaken(pt)}"))

if(F) {
  # data.table syntax
  harp[,.(first_harp_date = min(as.Date(monthly.rpt.prd, "%m/%d/%Y"))), loan_id, 
       keep=c("loan_id", "monthly.rpt.prd")]
}

harp1[,before_12m_first_harp_date:=first_harp_date]
# 164 seconds
system.time(lubridate::month(harp1$before_12m_first_harp_date) <- lubridate::month(harp1$before_12m_first_harp_date) - 12)
harp1[,harp_12m := TRUE]

# break it out into smaller chunks for even faster merging
# took 27
pt <- proc.time()
harp2 <- shard(
  harp1, 
  "loan_id", 
  nchunks = nchunks(fmdf), 
  outdir = file.path(outpath, "first_harp_date.df"), 
  overwrite=T)
cat(glue::glue("sharding HARP defaults took {timetaken(pt)}"))

# no need to hard group by it's already sharded by loan_id
# took about 503 seconds
# took about 11:52
pt <- proc.time()
defaults <- map.disk.frame(fmdf, function(df) {
  # create the default flag
  df[,date:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
  setkey(df,loan_id, date)
  df[,delq.status := as.integer(delq.status)]
  df[,default:=delq.status>=3]
  df[is.na(default),default:=F]
  
  df2 = df[default==T,.(loan_id, start_date =date, end_date = date)]
  lubridate::month(df2$start_date) <- lubridate::month(df2$start_date)-12
  
  # get rid of overlaps
  df2[order(start_date),lag_end_date := shift(end_date,1), loan_id]
  
  df2[,grp_incr := 1]
  df2[start_date <= lag_end_date, grp_incr := 0, loan_id]
  
  df2[,grp:=cumsum(grp_incr)]
  df3 = df2[,.(start_date = min(start_date), end_date = max(end_date), default_12m=T),.(loan_id,grp)]
  
  df3[,grp:=NULL]
  
  # create the hardship flag
  # whether the customer goes into in the next 12 months hardship 
  df3
  }, 
  keep = c("monthly.rpt.prd", "delq.status", "loan_id"), 
  outdir = file.path(outpath, "defaults.df"),
  lazy = F, 
  overwrite = T)
cat(glue::glue("Creating forward looking default flag took {timetaken(pt)}"))

# 
# a = df[loan_id == "100513171914", ]
# 
# a[,monthly.rpt.prd := as.Date(monthly.rpt.prd,"%m/%d/%Y")]
# a[,monthly.rpt.prd2:=monthly.rpt.prd]
# 
# 
# setkey(df3,"loan_id","start_date","end_date")
# 
# a1= foverlaps(a, df3, by.x = c("loan_id","monthly.rpt.prd","monthly.rpt.prd2"),
#           by.y=c("loan_id","start_date","end_date"))
# 
# a1[,.(loan_id,monthly.rpt.prd,start_date, end_date)]

# create a list that show defaults ----------------------------------------
# system.time(defaults <- chunk_lapply(fmdf, function(df) {
#   df[,date:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
#   df[,delq.status := as.integer(delq.status)]
#   df[,default:=delq.status>=3]
#   df[is.na(default),default:=F]
#   df1 = df[default == F]
#   df1
# }, outdir="tmp1"))

# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fmdf = disk.frame("fmdf")
harp = disk.frame("harp.df")

# create forward looking flag ---------------------------------------------
#df = get_chunk.disk.frame(fmdf,1)
#df = df[loan_id == "100513171914", ] 
harp1 = delayed(harp, function(df) {
  df[,date:=as.Date(monthly.rpt.prd, "%m/%d/%Y")]
  df[,.(loan_id, date)]
})

harp2 <- hard_group_by(harp1, by = "loan_id", outdir = "tmp_harp2")

# find out the first date on which an account enters into harp
harp1[,.(min_date = min(date)),loan_id,keep=c("monthly.rpt.prd","loan_id")]
harp1[,.(min = min(date))]

system.time(defaults <- chunk_lapply(fmdf, function(df) {
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
}, keep=c("monthly.rpt.prd", "delq.status", "loan_id"), outdir="tmp2"))

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

# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fmdf = disk.frame("fmdf")

# create forward looking flag ---------------------------------------------
plan(multiprocess(workers=6))

#df = get_chunk.disk.frame(fmdf,1)
#df = df[loan_id == "100513171914", ]

#plan(sequential)
system.time(defaults <- chunk_lapply(fmdf, function(df) {
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

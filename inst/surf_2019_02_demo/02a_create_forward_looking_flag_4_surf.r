# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")
outpath
fmdf = disk.frame(file.path(outpath, "fm_with_harp"))

nrow(fmdf)


# no need to hard group by it's already sharded by loan_id
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

head(defaults)

defaults %>% 
  filter(loan_id == "107150255179") %>% 
  collect

# 2_exploratory.r
source("inst/fannie_mae/0_setup.r")

fmdf = disk.frame("fmdf0")

system.time(a <- fmdf[,.N,delq.status,keep="delq.status"][,.(N=sum(N)),delq.status])
setkey(a,delq.status); a

str(a)

str(head(fmdf))

b = fmdf[,.N,monthly.rpt.prd,keep="monthly.rpt.prd"][,sum(N),monthly.rpt.prd]; b


names(fmdf)
servicer.name_cnt = fmdf[,.N,servicer.name,keep=c("servicer.name")]

servicer.name_cnt[,sum(N),servicer.name]

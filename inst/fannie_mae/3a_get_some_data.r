library(disk.frame)

source("inst/fannie_mae/0_setup.r")

a = disk.frame(file.path(outpath, "fm_with_harp"))

pt = proc.time()
a1 <- a %>% 
  srckeep(c("loan_id","monthly.rpt.prd","delq.status", "default_12m")) %>% 
  delayed(~{
  setkey(.x, loan_id)
  uid = sample_frac(.x[,.(loan_id = unique(loan_id))], 0.01)
  .x[uid,]
}) %>% collect(parallel = T)
timetaken(pt)

a1[delq.status != "X",delq.statusn := as.numeric(delq.status)]
a1[delq.status %in% c("","X"),delq.statusn := 0]

#a1[, delq.statusn := pmin(delq.statusn, 3)]

a1[,.N,delq.statusn]

a1[is.na(delq.statusn)]

# remove those that have already defulat
a2 = a1

a2[is.na(default_12m), default_12m := FALSE]

a3 = a2[,.(loan_id, monthly.rpt.prd, delq.statusn, default_12m)]
setkey(a3, loan_id, monthly.rpt.prd)

a3[,sum(default_12m)/.N,delq.statusn]

# create worst delq status last 12 months
system.time(a4 <- a3[, shift(delq.statusn,n=1:12), by=loan_id])

eval(parse(text=glue::glue("a4[,worst_delq_last_12m := pmax({paste0(paste0('V', 1:12), collapse=',')}, na.rm=T)]")))

a5 = bind_cols(a3, a4[,.(worst_delq_last_12m)])

a6 = a5#[delq.statusn < 3,]

a6[,worst_delq_last_12m_capped := pmin(worst_delq_last_12m, 6)]

a6[,.N, worst_delq_last_12m_capped]

a6devid = sample_frac(a6[,.(loan_id = unique(loan_id))], 0.7)

a6dev = a6[a6devid]
a6val = a6[!a6devid]

a6devpd = a6dev[,.(pd = sum(default_12m)/.N), worst_delq_last_12m_capped]

a6val = left_join(a6val, a6devpd, by = "worst_delq_last_12m_capped")
a6dev = left_join(a6dev, a6devpd, by = "worst_delq_last_12m_capped")

setkey(a6val, loan_id, monthly.rpt.prd)
setkey(a6dev, loan_id, monthly.rpt.prd)

for_write <- function(a6val) {
  a6val1 = a6val[as.Date("2015-06-01") < monthly.rpt.prd &  monthly.rpt.prd <= as.Date("2016-06-01") & !is.na(worst_delq_last_12m_capped), ]
  a6val1[,N:=.N,loan_id]
  
  a6val2 = a6val1[N == 12,]
  setkey(a6val2, loan_id, monthly.rpt.prd)
  
  toremove = a6val2[,.SD[12,delq.statusn], loan_id]
   
  toremove1 = toremove[V1 >= 3, loan_id]
  
  a6val2 = a6val2[!(loan_id %in% toremove1), ]
  
  a6val2[order(loan_id), id := rleid(loan_id)]
  setkey(a6val2, loan_id, monthly.rpt.prd)
  a6val2
}


a6val2 <- for_write(a6val)
a6dev2 <- for_write(a6dev)

a6dev2[,.N, delq.statusn]
a6val2[,.N, delq.statusn]

fwrite(a6val2[,.(id, delq_status = delq.statusn, default_12m)], "val.csv")
fwrite(a6dev2[,.(id, delq_status = delq.statusn, default_12m)], "dev.csv")


gini <- function(a6val) {
  setkey(a6val, id, monthly.rpt.prd)
  a6val = a6val[seq(12,.N, by=12),]
  a6_auc = a6val[order(worst_delq_last_12m_capped), .(bads = sum(default_12m), tots =.N), pd]
  
  a6_auc[,score:=-pd]
  setkey(a6_auc, score)
  
  a6_auc[order(score), height := bads/sum(bads)]
  a6_auc[order(score), width := tots/sum(tots)]
  plot(a6_auc[,.(cumsum(width), cumsum(height))], type="l")
  abline(0,1)
  a6_auc2 = bind_rows(data.table(height=0, width=0), a6_auc)
  a6_auc2[,cheight := cumsum(height)]
  a6_auc2[,cwidth := cumsum(width)]
  2*a6_auc2[,sum((cheight+lag(cheight))*c(0,diff(cwidth))/2, na.rm=T)]-1
}

gini(a6val2)
gini(a6dev2)


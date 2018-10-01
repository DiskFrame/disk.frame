#1c2
source("inst/fannie_mae/0_setup.r")

# load fmdf 
fmdf <- disk.frame("fmdf")

#system.time(harp <- fread("D:/data/fannie_mae/HARP_Files/Performance_HARP.txt", colClasses = Performance_ColClasses, col.names = Performance_Variables))
harp_mapping <- fread("D:/data/fannie_mae/HARP_Files/Loan_Mapping.txt", colClasses = "c", col.names = c("loan_id", "harp_id"))
setkey(harp_mapping, harp_id)
fst::write_fst(harp_mapping,"harp_mapping.fst")

# too about 438.65
system.time(
  harp <- csv_to_disk.frame("D:/data/fannie_mae/HARP_Files/Performance_HARP.txt", inmapfn = function(df) {
    setnames(df, "loan_id", "harp_id")
    
    merge(df, harp_mapping, by="harp_id")
  },
  nchunks = nchunk(fmdf),
  in_chunk_size = 1e7,
  shardby = "loan_id",
  outdir = "harp.df",
  colClasses = Performance_ColClasses,
  col.names = Performance_Variables,
  sep="|"))

if(F) {
  # it can be seen that some accounts can start n harp the month the same that it ends in the dataset
  harp1 = get_chunk.disk.frame(harp,1)
  fmdf1 = get_chunk.disk.frame(fmdf,1)
  
  harp1[,date:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
  fmdf1[,date:=as.Date(monthly.rpt.prd,"%m/%d/%Y")]
  
  fmdf2 = fmdf1[loan_id %in% unique(harp1$loan_id)]
  
  fmdf3 = merge(
    fmdf2[,.(max_date = max(date)),loan_id],
    harp1[,.(min_date = min(date)),loan_id],
    by = "loan_id"
  )
  
  fmdf3[max_date == min_date,]
  
  fmdf3[max_date > min_date,]
  fmdf3[max_date <= min_date,]
  
  harp4 = harp[,.N, delq.status, keep="delq.status"]
}


# print(nrow(harp))
# system.time(harp <- merge(harp, harp_mapping, by="harp_id"))
# print(nrow(harp))
# setkey(harp,"loan_id")
# system.time(fst::write_fst(harp,"harp.fst")) # 16.63
#system.time(fst::write_fst(harp,"harp100.fst",100)) # 391 seconds

# check if the harp_id -> loan_id is successful
if(F) {
  harp_acq <- fread("d:/data/fannie_mae/HARP_Files/Acquisition_HARP.txt")
  fst::write_fst(harp_mapping,"harp_mapping.fst")
  
  harp[,date:=as.Date(month,"%m/%d/%Y")]
  harp[,.N,date][order(date)]
  fmdf = disk.frame("fmdf")
  system.time(uid <- fmdf[,.(loan_id = unique(loan_id)), keep = "loan_id"][,unique(loan_id)])
  harp_uid = unique(harp_mapped$loan_id)
  def = intersect(uid, harp_uid)
  def  
}
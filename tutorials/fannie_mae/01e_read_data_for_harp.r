#1c2
source("tutorials/fannie_mae/00_setup.r")

pt <- proc.time()

# rows to read in one go
rows_to_read = 1e7

# load the Fannie Mae disk.frame
fmdf <- disk.frame(file.path(outpath, "fm.df"))

#system.time(harp <- fread("C:/data/HARP_Files/Performance_HARP.txt", colClasses = Performance_ColClasses, col.names = Performance_Variables))
harp_mapping <- fread(file.path(raw_harp_data_path,"Loan_Mapping.txt"), colClasses = "c", col.names = c("loan_id", "harp_id"))
setkey(harp_mapping, harp_id)
fst::write_fst(harp_mapping,"harp_mapping.fst.tmp")
fs::file_move("harp_mapping.fst.tmp", "harp_mapping.fst")
print("reading in and saving HARP mapping file took: ")
print(data.table::timetaken(pt))

# took about 438.65 on laptop 500 chunks
# took about 205 on desktop 56 chunks
pt <- proc.time()
system.time(
  harp <- csv_to_disk.frame(file.path(raw_harp_data_path,"Performance_HARP.txt"), inmapfn = function(df) {
    setnames(df, "loan_id", "harp_id")
    
    merge(df, harp_mapping, by="harp_id")
  },
  nchunks = nchunks(fmdf),
  in_chunk_size = rows_to_read,
  shardby = "loan_id",
  outdir = file.path(outpath, "harp.df"),
  colClasses = Performance_ColClasses,
  col.names = Performance_Variables,
  sep="|"))

print("reading in and saving HARP mapping file took: ")
print(data.table::timetaken(pt))


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
  harp_acq <- fread("C:/data/HARP_Files/Acquisition_HARP.txt")
  fst::write_fst(harp_mapping,"harp_mapping.fst")
  
  harp[,date:=as.Date(month,"%m/%d/%Y")]
  harp[,.N,date][order(date)]
  fmdf = disk.frame("fmdf")
  system.time(uid <- fmdf[,.(loan_id = unique(loan_id)), keep = "loan_id"][,unique(loan_id)])
  harp_uid = unique(harp_mapped$loan_id)
  def = intersect(uid, harp_uid)
  def  
}
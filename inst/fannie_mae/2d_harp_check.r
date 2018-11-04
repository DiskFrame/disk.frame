#2d_harp_check.r
source("inst/fannie_mae/0_setup.r")

system.time(harp <- fread("D:/data/fannie_mae/HARP_Files/Performance_HARP.txt", select = c("V1","V2"), colClasses = "c"))
setnames(harp,names(harp), c("harp_id","month"))

harp_mapping <- fread("D:/data/fannie_mae/HARP_Files/Loan_Mapping.txt", colClasses = "c", col.names = c("loan_id", "harp_id"))
fst::write_fst(harp_mapping,"harp_mapping.fst")

harp_mapped = merge(harp, harp_mapping, by="harp_id")
c(nrow(harp), nrow(harp_mapped))

harp_acq <- fread("d:/data/fannie_mae/HARP_Files/Acquisition_HARP.txt")
fst::write_fst(harp_mapping,"harp_mapping.fst")

harp[,date:=as.Date(month,"%m/%d/%Y")]
harp[,.N,date][order(date)]

fmdf = disk.frame("fmdf")

system.time(uid <- fmdf[,.(loan_id = unique(loan_id)), keep = "loan_id"][,unique(loan_id)])

harp_uid = unique(harp_mapped$loan_id)

def = intersect(uid, harp_uid)
def

system.time(harp.df <- shard(harp,"loan_id", nchunks = nchunks(fmdf), outdir = "harp.df", overwrite = T))

system.time(fmdfh <- rbindlist.disk.frame(list(fmdf, harp.df), outdir = "fmdf1_w_harp"))

# nothing in the intersection
# so either HARP ids were changed once there are in the harp program
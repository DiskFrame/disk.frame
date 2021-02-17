source("tutorials/fannie_mae/00_setup.r")
library(disk.frame)


Acquisitions_Variables = c("LOAN_ID", "ORIG_CHN", "Seller.Name", "ORIG_RT", "ORIG_AMT", "ORIG_TRM", "ORIG_DTE"
                           ,"FRST_DTE", "OLTV", "OCLTV", "NUM_BO", "DTI", "CSCORE_B", "FTHB_FLG", "PURPOSE", "PROP_TYP"
                           ,"NUM_UNIT", "OCC_STAT", "STATE", "ZIP_3", "MI_PCT", "Product.Type", "CSCORE_C", "MI_TYPE", "RELOCATION_FLG") %>% tolower()

Acquisition_ColClasses = c("character", "character", "character", "numeric", "numeric", "integer", "character", "character", "numeric",
                           "numeric", "character", "numeric", "numeric", "character", "character", "character", "character", "character",
                           "character", "character", "numeric", "character", "numeric", "numeric", "character")



fs::dir_delete(file.path(outpath, "acq.df"))

fmdf <- disk.frame(file.path(outpath,"fm_with_harp"))

pt = proc.time()
acq <- zip_to_disk.frame(
  acqzip_file_path, 
  outdir =  file.path(outpath, "acq.df"),
  col.names = Acquisitions_Variables, 
  colClasses = Acquisition_ColClasses,
  shardby = "loan_id",
  nchunks = nchunks(fmdf))
timetaken(pt)

pt = proc.time()
acqall <- rbindlist.disk.frame(acq, outdir = file.path(outpath, "acq_all.df"))
timetaken(pt)

# took 50 minutes
pt = proc.time()
fmdf_all = left_join(
  fmdf, 
  acqall,
  by = "loan_id", 
  merge_by_chunk_id = T,
  outdir = file.path(outpath, "fmdf_appl")
  )
timetaken(pt)

acqall = disk.frame(file.path(outpath, "acq_all.df"))

fmdf <- disk.frame(file.path(outpath,"fm_with_harp"))


for(i in 1:nchunks(fmdf)) {
  print(i)
  a1 = get_chunk(acqall,i, keep="loan_id") %>% unique
  a2 = get_chunk(fmdf,i, keep="loan_id") %>% unique
  stopifnot(nrow(a1) == nrow(a2))
  stopifnot(nrow(setdiff(a1,a2)) == 0)
  stopifnot(nrow(setdiff(a2,a1)) == 0)
}


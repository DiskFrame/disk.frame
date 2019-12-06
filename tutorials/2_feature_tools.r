library(disk.frame)
library(data.table)
library(drake)
# this willl set disk.frame with multiple workers
setup_disk.frame()
# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)

loadd(gd)

system.time(corgd = cor(gd))

library(data.table)

system.time(
  a <- 
    data.table::fread(
      "C:/Users/ZJ.DAI/Documents/git/disk.frame/tutorials/gd.csv"))


if(nchunks(df) != 32) {
  rechunk(df, 32, shardby= "sk_id_curr")
}

# rechunk(df, 32)
plan <- drake::drake_plan(
  df = disk.frame(file_in("d:/data/ft.df")),
  df = {
    if(nchunks(df) != 32) {
      rechunk(df, 32, shardby= "sk_id_curr")
    }
  },
  bads = df %>% 
    filter(target == 1) %>%
    collect(parallel=FALSE)
)



system.time()

sfrac = nrow(bads)/(nrow(df) - nrow(bads))

# sfrac = 0.07490269
system.time(df1 <- df %>% 
  filter(target == 0) %>% 
  sample_frac(size = sfrac) %>% 
  collect(parallel = FALSE))

goods = df1

gd = rbindlist(list(bads, goods))
gd


pryr::object_size(gd)
arrow::write_parquet(gd, "gd.parquet")
fst::write_fst(gd, "gd.fst")
data.table::fwrite(gd, "gd.csv")

sample_frac(gd, 0.25) %>% 
  data.table::fwrite("gd.sample.csv")

gd = fst::read_fst("gd.fst")
library(DataExplorer)
DataExplorer::create_report(gd)


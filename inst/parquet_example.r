library(sparklyr)
sc <- spark_connect(master = "local")
# install.packages(c("nycflights13", "Lahman"))


sapply(dir("d:/data/fannie_mae//Acquisition_All/", full.names = T), function(dpath) {
  print(dpath)
  system.time(fm_tbl <- spark_read_csv(sc, "fm_csv", dpath, delimiter = "|"))
  spark_write_parquet(fm_tbl, "d:/data/fannie_mae/fm.parquet", mode = "append" )
})
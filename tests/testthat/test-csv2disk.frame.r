context("test-csv2disk.frame")

setup({
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, "tmp_pls_delete_csv2df.csv")
  data.table::fwrite(df, "tmp_pls_delete_csv2df2.csv")
  data.table::fwrite(df, "tmp_pls_delete_csv2df3.csv")
})

test_that("csv2disk.frame works with no shard", {
  dff = csv_to_disk.frame(
    "tmp_pls_delete_csv2df.csv", 
    "tmp_pls_delete_csv2df.df", 
    overwrite=TRUE, 
    nchunks=max(2, recommend_nchunks(file.size("tmp_pls_delete_csv2df.csv"))))
  dff1 = dff[,sum(v1), id1]
  dff2 = dff1[,sum(V1), id1]
  expect_false(nrow(dff1) == nrow(dff2))
  expect_equal(nrow(dff), 1e3+11)
  expect_equal(ncol(dff), 9)
})

test_that("csv2disk.frame works with shard", {
  dff = csv_to_disk.frame("tmp_pls_delete_csv2df2.csv", "tmp_pls_delete_csv2df2.df", shardby = "id1", overwrite = T)
  dff1 = dff[,sum(v1), id1]
  dff2 = dff1[,sum(V1), id1]
  expect_true(nrow(dff1) == nrow(dff2))
  expect_equal(nrow(dff), 1e3+11)
  expect_equal(ncol(dff), 9)
  
  dff = csv_to_disk.frame("tmp_pls_delete_csv2df3.csv", "tmp_pls_delete_csv2df3.df", shardby = c("id1","id2"))
  dff1 = dff[,sum(v1), .(id1,id2)]
  dff2 = dff1[,sum(V1), .(id1,id2)]
  expect_true(nrow(dff1) == nrow(dff2))
  expect_equal(nrow(dff), 1e3+11)
  expect_equal(ncol(dff), 9)
})

test_that("csv2disk.frame tests readr", {
  library(dplyr)
  library(disk.frame)
  library(data.table)
  library(nycflights13)
  
  # convert from a data frame
  flights <- flights %>%
    dplyr::mutate(date = as.Date(paste(year, month, day, sep = "-")))
  str(flights) # time_hour is POSIXct
  
  flights.df <- as.disk.frame(
    flights,
    outdir = file.path(tempdir(), "tmp_flights.df"),
    overwrite = TRUE)
  flights.df
  str(collect(flights.df)) 
})

teardown({
  fs::dir_delete("tmp_pls_delete_csv2df.df")
  fs::dir_delete("tmp_pls_delete_csv2df2.df")
  fs::dir_delete("tmp_pls_delete_csv2df3.df")
  fs::file_delete("tmp_pls_delete_csv2df.csv")
  fs::file_delete("tmp_pls_delete_csv2df2.csv")
  fs::file_delete("tmp_pls_delete_csv2df3.csv")
})
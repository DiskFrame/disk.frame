context("test-sql-query")

test_that("testing sql_query", {
  ## setup a dummy database
  # library(RSQLite)
  # library(DBI)
  # 
  # con <- dbConnect(RSQLite::SQLite(), ":memory:")
  # 
  # dbWriteTable(con, "iris", iris)
  # 
  # diskf1 <- sql_query_to_disk.frame(con, "select * from iris")
  # 
  # diskf2 <- db_table_to_disk.frame(con, "iris")
  # 
  # testthat::expect_equal(nrow(diskf1), 150)
  # testthat::expect_equal(nrow(diskf2), 150)
  # 
  # DBI::dbDisconnect(con)
  # 
})

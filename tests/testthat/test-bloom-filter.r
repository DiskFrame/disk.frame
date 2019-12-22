context("test-bloomfilter")

test_that("bloomfilter should fail here", {
  expect_error(make_bloomfilter(df, c("origin", "dest")))
})

test_that("bloomfilter should succeed", {
  df = nycflights13::flights %>% as.disk.frame(shardby = c("carrier"))
  make_bloomfilter(df, "carrier")
  expect_true(length(bf_likely_in_chunks(df, "carrier", "UA")) == 1)
  
  expect_equal(nrow(collect(use_bloom_filter(df, "carrier", "UA"))), nrow(filter(nycflights13::flights, carrier == "UA")))
  
  # clean up
  delete(df)
})

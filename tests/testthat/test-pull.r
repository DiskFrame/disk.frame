context("test-pull")

test_that("pull",  {
  flights_df = as.disk.frame(nycflights13::flights)
  
  a = flights_df %>% 
    pull(carrier)
  b = flights_df %>% collect() %>% pull(carrier)
  
  expect_setequal(a, b)
  
  a = flights_df %>% 
    pull(2)
  b = flights_df %>% collect() %>% pull(2)
  
  expect_setequal(a, b)
  
  a = flights_df %>% 
    pull(-1)
  b = flights_df %>% collect() %>% pull(-1)
  expect_setequal(a, b)
  
  delete(flights_df)
})
context("test-partitions")

setup({
  setup_disk.frame(workers = 1)
})

test_that("test partitions", {
  a = as.disk.frame(cars) %>% 
    write_disk.frame(partitionby="speed", outdir=tempfile())
  
  a = a %>% 
    partition_filter(speed < 10) %>%
    collect %>% 
    arrange(speed, dist)
  
  
  b = cars %>% 
    filter(speed < 10) %>% 
    arrange(speed, dist)
  
  for(n in names(a)) {
    expect_equal(a %>% select(!!n), b %>% select(!!n))
  }
})

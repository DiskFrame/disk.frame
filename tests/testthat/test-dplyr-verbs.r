context("test-dplyr-verbs")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, "tmp_b_dv.df", nchunks = 5, overwrite = T)
})

test_that("testing select", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>% 
    select(a) %>% 
    collect
  
  expect_equal(ncol(df), 1)
})

test_that("testing rename", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>% 
    rename(a_new_name = a) %>% 
    collect
  
  expect_setequal(colnames(df), c("a_new_name", "b"))
})

test_that("testing filter", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>% 
    filter(a <= 100, b <= 10) %>% 
    collect
  
  expect_setequal(nrow(df), 10)
})

test_that("testing mutate", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>% 
    mutate(d = a + b) %>% 
    collect
  
  expect_setequal(sum(df$d), sum(df$a, df$b))
})

test_that("testing trans_mutate", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>% 
    transmute(d = a + b) %>% 
    collect
  
  expect_setequal(names(df), c("d"))
})

test_that("testing arrange", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>%
    mutate(random_unif = runif(dplyr::n())) %>% 
    arrange(desc(random_unif))
  
  x = purrr::map_lgl(1:nchunks(df), ~{
    is.unsorted(.x) == FALSE
  })
  
  expect_true(all(x))
})

test_that("testing summarise", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>%
    summarise(suma = sum(a)) %>% 
    collect %>% 
    summarise(suma = sum(suma))
  
  expect_equal(df$suma, collect(b)$a %>% sum)
})

test_that("testing do", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>%
    do(mlm = lm(a~b, data = .)) %>% 
    collect
  
  x = purrr::map_chr(df$mlm, ~{
    class(.x)
  })
    
  expect_true(all(x == "lm"))
})

teardown({
  fs::dir_delete("tmp_d_dv.df")
})
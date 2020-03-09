context("test-tidytable-verbs")

setup({
  b = data.frame(a = 51:150, b = 1:100)
  as.disk.frame(b, file.path(tempdir(), "tmp_b_dv.df"), nchunks = 5, overwrite = T)
})

test_that("testing select", {
  b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  
  df = b %>% 
    dt_select(a) %>% 
    collect
  
  expect_equal(ncol(df), 1)
})

test_that("testing rename", {
  # b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  # 
  # df = b %>% 
  #   rename(a_new_name = a) %>% 
  #   collect
  # 
  # expect_setequal(colnames(df), c("a_new_name", "b"))
})

test_that("testing filter", {
  b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  
  df = b %>% 
    dt_filter(a <= 100, b <= 10) %>% 
    collect
  
  expect_setequal(nrow(df), 10)
})

test_that("testing filter - global vars", {
  b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  
  one_hundred = 100
  
  df = b %>% 
    dt_filter(a <= one_hundred, b <= 10) %>% 
    collect
  
  df_orig = b %>% 
    filter(a <= one_hundred, b <= 10) %>% 
    collect

  expect_setequal(nrow(df), 10)
})

test_that("testing mutate", {
  library(disk.frame)
  setup_disk.frame()
  library(testthat)
  library(tidytable)
  b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  
  df_orig = b %>% 
    dt_mutate(d = a + b) %>% 
    collect
  
  df = b %>% 
    dt_mutate(d = a + b) %>% 
    collect
  
  expect_setequal(sum(df$d), sum(df$a, df$b))
  
  df = b %>% 
    dt_mutate(e = rank(desc(a))) %>%
    collect
  
  expect_equal(nrow(df), 100)
  
  # need to test
  value <- as.disk.frame(tibble(char = LETTERS,
                                num = 1:26))
  df2 = value %>%
    dt_mutate(b =  case_when(
      char %in% c("A", "B", "C") ~ "1",
      TRUE ~ char)) %>% 
    collect
  
  expect_equal(ncol(df2), 3)
  
  # testing
  fn = function(a, b) {
    a+b
  }
  
  df3 = value %>%
    dt_mutate(b =  fn(num, num)) %>%
    collect
  
  expect_equal(ncol(df3), 3)
  
  
  global_var = 100
  
  df4 = value %>%
    dt_mutate(b =  fn(num, num), d = global_var*2) %>%
    collect
  
  expect_equal(ncol(df4), 4)
  expect_true(all(df4$d == 200))
})

test_that("testing mutate user-defined function", {
   b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
   
   
   udf = function(a1, b1) {
     a1 + b1
   }
   
   df = b %>%
     dt_mutate(d = udf(a,b)) %>%
     collect
   
   df_orig = b %>%
     mutate(d = udf(a,b)) %>%
     collect
   
   expect_setequal(sum(df$d), sum(df$a, df$b))
})

test_that("testing transmute", {
  # b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  # 
  # df = b %>% 
  #   transmute(d = a + b) %>% 
  #   collect
  # 
  # expect_setequal(names(df), c("d"))
})

test_that("testing arrange", {
  # b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  # 
  # expect_warning(df <- b %>%
  #   mutate(random_unif = runif(dplyr::n())) %>% 
  #   arrange(desc(random_unif)))
  # 
  # df <- b %>%
  #   mutate(random_unif = runif(dplyr::n())) %>% 
  #   chunk_arrange(desc(random_unif))
  # 
  # x = purrr::map_lgl(1:nchunks(df), ~{
  #   is.unsorted(.x) == FALSE
  # })
  # 
  # expect_true(all(x))
})

test_that("testing chunk_summarise", {
  # b = disk.frame(file.path(tempdir(), "tmp_b_dv.df"))
  # 
  # df = b %>%
  #   chunk_summarise(suma = sum(a)) %>% 
  #   collect %>% 
  #   summarise(suma = sum(suma))
  # 
  # expect_equal(df$suma, collect(b)$a %>% sum)
})

test_that("testing mutate within function works", {
  test_f <- function(params, x_df){
    x_df %>% mutate(aha = params[1]*cyl + params[2]*disp)
  }
  
  expect_true("aha" %in% names(test_f(c(1, 2), mtcars)))

  test_f <- function(params, x_df){
    x_df %>% dt_mutate(aha = params[1]*cyl + params[2]*disp)
  }
  
  expect_true("aha" %in% names(test_f(c(1, 2), mtcars)))
})

test_that("filter failure: prevent github #191 regression",  {
  flights_df = as.disk.frame(nycflights13::flights)
  
  # expect error due to syntax error
  expect_warning(expect_error(flights_df %>% 
    dt_filter(tailnum %in% paste0(unique(nycflights13::flights$tailnum)[1:60]), "") %>% 
    collect))
  
  delete(flights_df)
})


teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_b_dv.df"))
})
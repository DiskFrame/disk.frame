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

test_that("testing filter - global vars", {
  b = disk.frame("tmp_b_dv.df")
  
  one_hundred = 100
  
  df = b %>% 
    filter(a <= one_hundred, b <= 10) %>% 
    collect
  
  expect_setequal(nrow(df), 10)
})

test_that("testing mutate", {
  b = disk.frame("tmp_b_dv.df")
  
  
  df = b %>% 
    mutate(d = a + b) %>% 
    collect
  
  expect_setequal(sum(df$d), sum(df$a, df$b))
  
  df = b %>% 
    mutate(e = rank(desc(a))) %>%
    collect
  
  expect_equal(nrow(df), 100)
  
  # need to test
  value <- as.disk.frame(tibble(char = LETTERS,
                                num = 1:26))
  df2 = value %>%
    dplyr::mutate(b =  case_when(
      char %in% c("A", "B", "C") ~ "1",
      TRUE ~ char)) %>% 
    collect
  
  expect_equal(ncol(df2), 3)
  
  # testing
  fn = function(a, b) {
    a+b
  }
  
  df3 = value %>%
    dplyr::mutate(b =  fn(num, num)) %>%
    collect
  
  expect_equal(ncol(df3), 3)
  
  
  global_var = 100
  
  df4 = value %>%
    dplyr::mutate(b =  fn(num, num), d = global_var*2) %>%
    collect
  
  expect_equal(ncol(df4), 4)
  expect_true(all(df4$d == 200))
})

test_that("testing mutate user-defined function", {
   b = disk.frame("tmp_b_dv.df")
   
   
   udf = function(a1, b1) {
     a1 + b1
   }
   
   df = b %>%
     mutate(d = udf(a,b)) %>%
     collect
   
   expect_setequal(sum(df$d), sum(df$a, df$b))
})

test_that("testing transmute", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>% 
    transmute(d = a + b) %>% 
    collect
  
  expect_setequal(names(df), c("d"))
})

test_that("testing arrange", {
  b = disk.frame("tmp_b_dv.df")
  
  expect_warning(df <- b %>%
    mutate(random_unif = runif(dplyr::n())) %>% 
    arrange(desc(random_unif)))
  
  df <- b %>%
    mutate(random_unif = runif(dplyr::n())) %>% 
    chunk_arrange(desc(random_unif))
  
  x = purrr::map_lgl(1:nchunks(df), ~{
    is.unsorted(.x) == FALSE
  })
  
  expect_true(all(x))
})

test_that("testing chunk_summarise", {
  b = disk.frame("tmp_b_dv.df")
  
  df = b %>%
    chunk_summarise(suma = sum(a)) %>% 
    collect %>% 
    summarise(suma = sum(suma))
  
  expect_equal(df$suma, collect(b)$a %>% sum)
})

test_that("testing mutate within function works", {
  test_f <- function(params, x_df){
    x_df %>% mutate(aha = params[1]*cyl + params[2]*disp)
  }
  
  expect_true("aha" %in% names(test_f(c(1, 2), mtcars)))
})


teardown({
  fs::dir_delete("tmp_b_dv.df")
})
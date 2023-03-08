context("test-group_by")

setup({
  setup_disk.frame(2)
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_gb.csv"))
})


test_that("new group_by framework", {
  iris.df = iris %>%
    as.disk.frame
  
  grpby = iris.df %>%
    group_by(Species) %>%
    summarize(
      mean(Petal.Length),
      sumx = sum(Petal.Length / Sepal.Width),
      sd(Sepal.Width / Petal.Length),
      var(Sepal.Width / Sepal.Width)
    ) %>%
    collect
  
  grpby2 = iris %>%
    group_by(Species) %>%
    summarize(
      mean(Petal.Length),
      sumx = sum(Petal.Length / Sepal.Width),
      sd(Sepal.Width / Petal.Length),
      var(Sepal.Width / Sepal.Width)
    ) %>%
    arrange()
  
  for (n in names(grpby)) {
    expect_true(all(grpby2[, n] == grpby[, n]) ||
                  all(abs(grpby2[, n] - grpby[, n]) < 0.0001))
  }
  
  delete(iris.df)
})


test_that("new group_by framework - no group-by just summarise", {
  iris.df = iris %>%
    as.disk.frame
  
  grpby = iris.df %>%
    summarize(
      mean(Petal.Length),
      sumx = sum(Petal.Length / Sepal.Width),
      sd(Sepal.Width / Petal.Length),
      var(Sepal.Width / Sepal.Width)
    ) %>%
    collect
  
  grpby2 = iris %>%
    summarize(
      mean(Petal.Length),
      sumx = sum(Petal.Length / Sepal.Width),
      sd(Sepal.Width / Petal.Length),
      var(Sepal.Width / Sepal.Width)
    ) %>%
    arrange()
  
  for (n in names(grpby)) {
    expect_true(all(grpby2[, n] == grpby[, n]) ||
                  all(abs(grpby2[, n] - grpby[, n]) < 0.0001))
  }
  
  delete(iris.df)
})

# test_that("new group_by framework - nested-group-by", {
  # if(interactive()) {
  #   iris.df = iris %>% 
  #     as.disk.frame
  #   
  #   expect_error(grpby <- iris.df %>% 
  #     summarize(mean(Petal.Length + max(Petal.Length))) %>% 
  #     collect)
  #   
  #   expect_error(grpby <- iris.df %>% 
  #     summarize(mean(Petal.Length) + max(Petal.Length)) %>% 
  #     collect)
  #   
  #   expect_error(grpby <- iris.df %>% 
  #     summarize(mean(Petal.Length) + 1) %>% 
  #     collect)
  #   
  #   expect_error(grpby <- iris.df %>% 
  #     summarize(list(mean(Petal.Length))) %>% 
  #     collect)
  #   
  #   fn_tmp = function(x) x + 1
  #   grpby <- iris.df %>% 
  #       summarize(mean(fn_tmp(Petal.Length))) %>% 
  #       collect
  #   
  #   grpby2 <- iris %>% 
  #     summarize(mean(fn_tmp(Petal.Length)))
  #   
  #   for (n in names(grpby)) {
  #     expect_true(all(grpby2[, n] == grpby[, n]) || all(abs(grpby2[, n] - grpby[, n]) < 0.0001))
  #   }
  #   delete(iris.df)
  # }
  # expect_true(TRUE)
# })

test_that("guard against github #241", {
  # I suspect there was an issue with number of chunk = 1
  result_from_disk.frame = iris %>%
    as.disk.frame(nchunks = 1) %>%
    group_by(Species) %>%
    summarize(
      mean(Petal.Length),
      sumx = sum(Petal.Length / Sepal.Width),
      sd(Sepal.Width / Petal.Length),
      var(Sepal.Width / Sepal.Width),
      l = length(Sepal.Width / Sepal.Width + 2),
      max(Sepal.Width),
      min(Sepal.Width),
      median(Sepal.Width)
    ) %>%
    collect
  
  testthat::expect_true(TRUE)
})


# TODO turn these on
# test_that("test hard_group_by on disk.frame (sort)", {
#   dff = csv_to_disk.frame(
#     file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
#     file.path(tempdir(), "tmp_pls_delete_gb.df"))
#   
#   dff_res = dff %>% 
#     collect %>% 
#     group_by(id1, id2) %>% 
#     summarise(mv1 = mean(v1))
#   
#   dff1 <- dff %>% 
#     hard_group_by(id1, id2, shardby_function="sort") %>%
#     chunk_summarise(mv1 = mean(v1)) %>% collect
#   
#   expect_equal(nrow(dff1), nrow(dff_res))
# })
# 
# test_that("test hard_group_by on data.frame (sort)", {
#   df = disk.frame:::gen_datatable_synthetic(1e3+11)
#   
#   df1 = df %>% 
#     group_by(id1, id2) %>% 
#     summarise(mv1 = mean(v1))
#   
#   dff1 <- df %>% 
#     hard_group_by(id1, id2, shardby_function="sort") %>%
#     summarise(mv1 = mean(v1))
#   
#   expect_equal(nrow(dff1), nrow(df1))
# })

test_that("guard against github 256", {
  test2 <- tibble::tibble(
    date = lubridate::ymd(rep(c("2019-01-02", "2019-02-03", "2019-03-04"), 4)),
    uid = as.factor(rep(c(uuid::UUIDgenerate(), uuid::UUIDgenerate()), 6)),
    proto = as.factor(rep(c("TCP", "UDP", "ICMP"), 4)),
    port = as.double(rep(c(22, 21, 0), 4))
  )
  
  correct_result = test2 %>%
    group_by(date, uid, proto, port) %>%
    summarize(n=n()) %>% 
    ungroup %>% 
    collect
  
  test_df = as.disk.frame(test2, nchunks = 2, overwrite=TRUE)
  
  incorrect_result = test_df %>%
    group_by(date, uid, proto, port) %>%
    summarize(n=n()) %>% 
    collect
  
  expect_equal(dim(incorrect_result), dim(correct_result))
})

test_that("guard against github 256 #2", {
  test2 <- tibble::tibble(
    date = lubridate::ymd(rep(c("2019-01-02", "2019-02-03", "2019-03-04"), 4)),
    uid = as.factor(rep(c(uuid::UUIDgenerate(), uuid::UUIDgenerate()), 6)),
    proto = as.factor(rep(c("TCP", "UDP", "ICMP"), 4)),
    port = as.double(rep(c(22, 21, 0), 4))
  )
  
  test_df = as.disk.frame(test2, nchunks = 2, overwrite=TRUE)
  
  correct_result = test_df %>%
    group_by(!!!syms(names(test_df))) %>%
    summarize(n=n()) %>% 
    collect
  
  incorrect_result = test_df %>%
    group_by(date, uid, proto, port) %>%
    summarize(n=n()) %>% 
    collect
  
  expect_equal(dim(incorrect_result), dim(correct_result))
})

test_that("guard against github 256 #3", {
  test2 <- tibble::tibble(
    date = sample(1:10, 20, replace = TRUE),
    uid = sample(1:10, 20, replace = TRUE)
  )
  
  test_df = as.disk.frame(test2, nchunks = 2, overwrite=TRUE)
  
  ntd = names(test_df)
  
  correct_result = test_df %>%
    group_by(!!!syms(ntd)) %>%
    summarize(n=n()) %>% 
    collect
  
  incorrect_result = test_df %>%
    group_by(date, uid) %>%
    summarize(n=n()) %>% 
    collect
  
  expect_equal(dim(incorrect_result), dim(correct_result))
})

test_that("tests for github #250", {
  aggregate_expressions <- list(n = quote(n()))
  
  result1 = iris %>% 
    as.disk.frame() %>% 
    group_by(Species) %>%
    summarise(n = n()) %>% 
    collect
  
  result2 <- iris %>% 
    as.disk.frame() %>% 
    group_by(Species) %>%
    summarize(!!!(aggregate_expressions)) %>% 
    collect
  
  expect_equal(result1, result2)
})

test_that("tests for github #250 2", {
  aggregate_expressions <- list(n = quote(n()), quote(n()))
  
  result1 = iris %>% 
    as.disk.frame() %>% 
    group_by(Species) %>%
    summarise(n = n(), n()) %>% 
    collect; result1
  
  result2 <- iris %>% 
    as.disk.frame() %>% 
    group_by(Species) %>%
    summarize(!!!(aggregate_expressions)) %>% 
    collect
  
  expect_equal(result1, result2)
})


test_that("tests for across", {
  # TODO use a prototype approach?
  result2 <- iris %>% 
    group_by(across(where(is.numeric))) %>% 
    summarize(length(Species))
  
  testthat::expect_error(result1 <- iris %>% 
    as.disk.frame() %>% 
    group_by(across(where(is.numeric))) %>% 
    summarize(length(Species)) %>% 
    collect
  )
  
  #expect_equal(result1, result2)
})

test_that("tests for {{}}", {
  # TODO make this work
  bracket_groupby <- function(input_data, grp_cols) {
    input_data %>% 
      group_by({{grp_cols}}) %>% 
      summarize(mean(Petal.Length)) %>% 
      collect
  }
  
  a = bracket_groupby(iris, Species)
  
  b = bracket_groupby(as.disk.frame(iris), Species)
  
  expect_equal(a, b)
})


test_that("tests for global", {
  # TODO make this work
  val = 2
  val2 = 2
  
  b = iris %>% 
    as.disk.frame %>% 
    group_by(as.integer(Species) + val, as.integer(Species) + val2) %>% 
    summarize(mean(Petal.Length)) %>% 
    collect
  
  a = iris %>% 
    group_by(as.integer(Species) + val, as.integer(Species) + val2) %>% 
    summarize(mean(Petal.Length)) %>% 
    collect
  
  expect_equal(b, a)
  
  b = iris %>%
    as.disk.frame %>%
    group_by(as.integer(Species) + val, as.integer(Species) + val2) %>%
    summarize(mean(Petal.Length+val2)) %>%
    collect

  a = iris %>%
    group_by(as.integer(Species) + val, as.integer(Species) + val2) %>%
    summarize(mean(Petal.Length+val2)) %>%
    collect

  expect_equal(a, b)
   
   a = iris %>%
    summarize(mean(Petal.Length+val2))

  b = iris %>%
    as.disk.frame %>%
    summarize(mean(Petal.Length+val2)) %>%
    collect

  expect_equal(names(a), names(b))

  expect_equal(a[[1]], b[[1]])
})

teardown({
  # fs::file_delete(file.path(tempdir(), "tmp_pls_delete_gb.csv"))
  # fs::dir_delete(file.path(tempdir(), "tmp_pls_delete_gb.df"))
})

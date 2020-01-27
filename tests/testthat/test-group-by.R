context("test-group_by")

setup({
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_gb.csv"))
})


test_that("new group_by framework", {
  if(interactive()) {
    iris.df = iris %>% 
      as.disk.frame
    
    grpby = iris.df %>% 
        group_by(Species) %>% 
        summarize(mean(Petal.Length), sumx = sum(Petal.Length/Sepal.Width), sd(Sepal.Width/ Petal.Length), var(Sepal.Width/ Sepal.Width)) %>% 
        collect
    
    grpby2 = iris %>% 
      group_by(Species) %>% 
      summarize(mean(Petal.Length), sumx = sum(Petal.Length/Sepal.Width), sd(Sepal.Width/ Petal.Length), var(Sepal.Width/ Sepal.Width)) %>% 
      arrange()
    
    for (n in names(grpby)) {
      expect_true(all(grpby2[, n] == grpby[, n]) || all(abs(grpby2[, n] - grpby[, n]) < 0.0001))
    }
    
    delete(iris.df)
  }
  expect_true(TRUE)
})

test_that("new group_by framework - no group-by just summarise", {
  if(interactive()) {
    iris.df = iris %>% 
      as.disk.frame
    
    grpby = iris.df %>% 
      summarize(mean(Petal.Length), sumx = sum(Petal.Length/Sepal.Width), sd(Sepal.Width/ Petal.Length), var(Sepal.Width/ Sepal.Width)) %>% 
      collect
    
    grpby2 = iris %>% 
      summarize(mean(Petal.Length), sumx = sum(Petal.Length/Sepal.Width), sd(Sepal.Width/ Petal.Length), var(Sepal.Width/ Sepal.Width)) %>% 
      arrange()
    
    for (n in names(grpby)) {
      expect_true(all(grpby2[, n] == grpby[, n]) || all(abs(grpby2[, n] - grpby[, n]) < 0.0001))
    }
    
    delete(iris.df)
  }
  expect_true(TRUE)
})

test_that("new group_by framework - nested-group-by", {
  if(interactive()) {
    iris.df = iris %>% 
      as.disk.frame
    
    expect_error(grpby <- iris.df %>% 
      summarize(mean(Petal.Length + max(Petal.Length))) %>% 
      collect)
    
    expect_error(grpby <- iris.df %>% 
      summarize(mean(Petal.Length) + max(Petal.Length)) %>% 
      collect)
    
    expect_error(grpby <- iris.df %>% 
      summarize(mean(Petal.Length) + 1) %>% 
      collect)
    
    expect_error(grpby <- iris.df %>% 
      summarize(list(mean(Petal.Length))) %>% 
      collect)
    
    fn_tmp = function(x) x + 1
    grpby <- iris.df %>% 
        summarize(mean(fn_tmp(Petal.Length))) %>% 
        collect
    
    grpby2 <- iris %>% 
      summarize(mean(fn_tmp(Petal.Length)))
    
    for (n in names(grpby)) {
      expect_true(all(grpby2[, n] == grpby[, n]) || all(abs(grpby2[, n] - grpby[, n]) < 0.0001))
    }
    delete(iris.df)
  }
  expect_true(TRUE)
})

test_that("guard against github #241", {
  if(interactive()) {
    result_from_disk.frame = iris %>%
      as.disk.frame(nchunks = 1) %>%
      group_by(Species) %>%
      summarize(
        mean(Petal.Length),
        sumx = sum(Petal.Length/Sepal.Width),
        sd(Sepal.Width/ Petal.Length),
        var(Sepal.Width/ Sepal.Width),
        l = length(Sepal.Width/ Sepal.Width + 2),
        max(Sepal.Width),
        min(Sepal.Width),
        median(Sepal.Width)
      ) %>%
      collect
  } else {
    expect_true(TRUE)
  }
})


test_that("group_by", {
  dff = csv_to_disk.frame(
    file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
    file.path(tempdir(), "tmp_pls_delete_gb.df"))
  
  dff_res = dff %>% 
    collect %>% 
    group_by(id1) %>% 
    summarise(mv1 = mean(v1))
  
  dff1 <- dff %>% 
    chunk_group_by(id1, id2) %>%
    chunk_summarise(mv1 = mean(v1)) %>% 
    collect

  
  expect_false(nrow(dff1) == nrow(dff_res))
})

test_that("test hard_group_by on disk.frame", {
  dff = csv_to_disk.frame(
    file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
    file.path(tempdir(), "tmp_pls_delete_gb.df"))
  
  dff_res = dff %>% 
    collect %>% 
    group_by(id1, id2) %>% 
    summarise(mv1 = mean(v1))
  
  dff1 <- dff %>% 
      hard_group_by(id1, id2) %>%
      chunk_summarise(mv1 = mean(v1)) %>% collect
  
  expect_equal(nrow(dff1), nrow(dff_res))
})

test_that("test hard_group_by on data.frame", {
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  
  df1 = df %>% 
    group_by(id1, id2) %>% 
    summarise(mv1 = mean(v1))
  
  dff1 <- df %>% 
    hard_group_by(id1,id2) %>%
    summarise(mv1 = mean(v1))
  
  expect_equal(nrow(dff1), nrow(df1))
})


test_that("test hard_group_by on disk.frame (sort)", {
  dff = csv_to_disk.frame(
    file.path(tempdir(), "tmp_pls_delete_gb.csv"), 
    file.path(tempdir(), "tmp_pls_delete_gb.df"))
  dff_res = dff %>% 
    collect %>% 
    group_by(id1, id2) %>% 
    summarise(mv1 = mean(v1))
  
  dff1 <- dff %>% 
    hard_group_by(id1, id2, shardby_function="sort") %>%
    chunk_summarise(mv1 = mean(v1)) %>% collect
  
  expect_equal(nrow(dff1), nrow(dff_res))
})

test_that("test hard_group_by on data.frame (sort)", {
  df = disk.frame:::gen_datatable_synthetic(1e3+11)
  
  df1 = df %>% 
    group_by(id1, id2) %>% 
    summarise(mv1 = mean(v1))
  
  dff1 <- df %>% 
    hard_group_by(id1, id2, shardby_function="sort") %>%
    summarise(mv1 = mean(v1))
  
  expect_equal(nrow(dff1), nrow(df1))
})

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

teardown({
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_gb.csv"))
  fs::dir_delete(file.path(tempdir(), "tmp_pls_delete_gb.df"))
})

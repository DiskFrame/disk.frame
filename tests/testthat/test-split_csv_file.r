context("test-split_csv_file.frame")
df = disk.frame:::gen_datatable_synthetic(1e3+11)
setup({
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df.csv"))
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df2.csv"))
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df3.csv"))
  
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df.tab"), sep = "\t")
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df2.tab"), sep = "\t")
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df3.tab"), sep = "\t")
  
  data.table::fwrite(nycflights13::flights, file.path(tempdir(), "tmp_pls_delete_flights.csv"))
})

test_that("count_lines works", {
  expect_equal(disk.frame:::count_lines_in_file(file.path(tempdir(), "tmp_pls_delete_csv2df.csv")),
               1e3+11 + 1)
  expect_equal(disk.frame:::count_lines_in_file(file.path(tempdir(), "tmp_pls_delete_flights.csv")),
               336777)
})

test_that("renumber_file_names works", {
  expect_equal(basename(disk.frame:::renumber_file_names(LETTERS)),
               sprintf("%s_%02d", LETTERS, seq_along(LETTERS)))
  
  LETTERS_LONG <- rep(LETTERS, times = 4)
  expect_equal(basename(disk.frame:::renumber_file_names(LETTERS_LONG)),
               sprintf("%s_%03d", LETTERS_LONG, seq_along(LETTERS_LONG)))
  
  expect_equal(basename(disk.frame:::renumber_file_names(LETTERS, suffix = "split")),
               sprintf("%s_split%02d", LETTERS, seq_along(LETTERS)))
  
  expect_equal(basename(disk.frame:::renumber_file_names(LETTERS, extension = "test")),
               sprintf("%s_%02d.test", LETTERS, seq_along(LETTERS)))
})

test_that("line_first_match works", {
  expect_equal(disk.frame:::line_first_match(file.path(tempdir(), "tmp_pls_delete_flights.csv"),
                                             pattern = "sched_dep_time"),
               1)
  expect_equal(disk.frame:::line_first_match(file.path(tempdir(), "tmp_pls_delete_flights.csv"),
                                             pattern = "N39463"),
               7)
               
  expect_equal(disk.frame:::line_first_match(file.path(tempdir(), "tmp_pls_delete_flights.csv"),
                                             pattern = "Pattern Not Found"),
               0)
})

test_that("header_row_index works", {
  expect_equal(disk.frame:::header_row_index(file.path(tempdir(), "tmp_pls_delete_csv2df.csv"),
                                             method = "data.table"),
               1)
  
  expect_equal(disk.frame:::header_row_index(file.path(tempdir(), "tmp_pls_delete_csv2df.tab"),
                                             method = "data.table",
                                             sep = "\t"),
               1)
  
  expect_equal(disk.frame:::header_row_index(file.path(tempdir(), "tmp_pls_delete_csv2df.csv"),
                                             method = "readr"),
               1)
  
  expect_equal(disk.frame:::header_row_index(file.path(tempdir(), "tmp_pls_delete_csv2df.tab"),
                                             method = "readr",
                                             delim = "\t"),
               1)
  
  expect_equal(disk.frame:::header_row_index(file.path(tempdir(), "tmp_pls_delete_csv2df.tab"),
                                             method = "LaF",
                                             header = TRUE),
               1)
  
  expect_equal(disk.frame:::header_row_index(file.path(tempdir(), "tmp_pls_delete_csv2df.tab"),
                                             method = "LaF",
                                             header = TRUE,
                                             sep = "\t"),
               1)
  
}) 

test_that("split_csv_file, bigreader method, works with single file", {
  infile <- file.path(tempdir(), "tmp_pls_delete_csv2df.csv")
  
  split_files <- split_csv_file(infile,
                                num_splits = 3,
                                header = TRUE,
                                split_method = "bigreadr",
                                outdir = tempdir(),
                                suffix = "split")
  
  expect_equal(length(split_files), 3)
  expect_equal(split_files,
               sprintf("%s_split%02d.csv", fs::path_ext_remove(infile), seq_len(3)))
  expect_equal(readLines(split_files[[1]], n = 1),
               readLines(infile, n = 1))
  expect_equal(readLines(split_files[[2]], n = 1),
               readLines(infile, n = 1))
  expect_equal(readLines(split_files[[3]], n = 1),
               readLines(infile, n = 1))
  
  expect_equal(readLines(split_files[[1]], n = 2),
               readLines(infile, n = 2))
  
  combined_df <- bind_rows(lapply(split_files, data.table::fread))
  # data.table imports dates as character; revert
  combined_df$date1 <- as.Date(combined_df$date1)
  expect_equal(combined_df,
               df)
  
  fs::file_delete(split_files)
  
})

test_that("split_csv_file, unix method, works with single file", {
  infile <- file.path(tempdir(), "tmp_pls_delete_csv2df.csv")
  
  split_files <- split_csv_file(infile,
                                num_splits = 3,
                                header = TRUE,
                                split_method = "unix",
                                outdir = tempdir(),
                                suffix = "split")
  
  expect_equal(length(split_files), 3)
  expect_equal(split_files,
               sprintf("%s_split%02d.csv", fs::path_ext_remove(infile), seq_len(3)))
  expect_equal(readLines(split_files[[1]], n = 1),
               readLines(infile, n = 1))
  expect_equal(readLines(split_files[[2]], n = 1),
               readLines(infile, n = 1))
  expect_equal(readLines(split_files[[3]], n = 1),
               readLines(infile, n = 1))
  
  expect_equal(readLines(split_files[[1]], n = 2),
               readLines(infile, n = 2))
  
  combined_df <- bind_rows(lapply(split_files, data.table::fread))
  # data.table imports dates as character; revert
  combined_df$date1 <- as.Date(combined_df$date1)
  expect_equal(combined_df,
               df)
  
  fs::file_delete(split_files)
})

test_that("split_csv_file, bigreader method, works with multiple files", {
  infile <- c(file.path(tempdir(), "tmp_pls_delete_csv2df.csv"),
              file.path(tempdir(), "tmp_pls_delete_csv2df2.csv"),
              file.path(tempdir(), "tmp_pls_delete_csv2df3.csv"))
  
  split_files <- split_csv_file(infile,
                                num_splits = 3,
                                header = TRUE,
                                split_method = "bigreadr",
                                outdir = tempdir(),
                                suffix = "split")
  
  expect_equal(length(split_files), 3)
  expect_equal(sum(sapply(split_files, length)), 9)
  expect_equal(names(split_files),
               infile)
  
  expect_equal(split_files[[2]],
               sprintf("%s_split%02d.csv", fs::path_ext_remove(infile[[2]]), seq_len(3)))
  expect_equal(readLines(split_files[[2]][[1]], n = 1),
               readLines(infile[[2]], n = 1))
  expect_equal(readLines(split_files[[2]][[2]], n = 1),
               readLines(infile[[2]], n = 1))
  expect_equal(readLines(split_files[[2]][[3]], n = 1),
               readLines(infile[[2]], n = 1))
  
  expect_equal(readLines(split_files[[2]][[1]], n = 2),
               readLines(infile[[2]], n = 2))
  
  combined_df <- bind_rows(lapply(split_files[[2]], data.table::fread))
  # data.table imports dates as character; revert
  combined_df$date1 <- as.Date(combined_df$date1)
  expect_equal(combined_df,
               df)
  
  fs::file_delete(split_files %>% unlist)
  
})

teardown({
  df_files <- file.path(tempdir(), c("tmp_pls_delete_csv2df.df",
                                     "tmp_pls_delete_csv2df2.df",
                                     "tmp_pls_delete_csv2df3.df",
                                     "tmp_pls_delete_flights.df"))
  
  # should already be removed except due to errors
  lapply(df_files, function(f) { if(dir.exists(f)) fs::dir_delete(f) })
  
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_csv2df.csv"))
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_csv2df2.csv"))
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_csv2df3.csv"))
  
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_csv2df.tab"))
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_csv2df2.tab"))
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_csv2df3.tab"))
  
  fs::file_delete(file.path(tempdir(), "tmp_pls_delete_flights.csv"))
})
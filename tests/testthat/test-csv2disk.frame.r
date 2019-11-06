context("test-csv2disk.frame")

df = disk.frame:::gen_datatable_synthetic(1e3+11)

CHUNK_READERS <- c("bigreadr", "data.table", "readr", "readLines", "LaF")
BACKENDS <- c("data.table", "readr", "LaF")
NCHUNKS <- 3

setup({
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df.csv"))
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df2.csv"))
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df3.csv"))
  
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df.tab"), sep = "\t")
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df2.tab"), sep = "\t")
  data.table::fwrite(df, file.path(tempdir(), "tmp_pls_delete_csv2df3.tab"), sep = "\t")
  
  data.table::fwrite(nycflights13::flights, file.path(tempdir(), "tmp_pls_delete_flights.csv"))
})



##### single file #####
for(backend in BACKENDS) {
  test_that(glue::glue("{backend} works with single file"), {

    args <- list()
    args$infile <- file.path(tempdir(), "tmp_pls_delete_csv2df.csv")
    args$outdir <- file.path(tempdir(), "tmp_pls_delete_csv2df.df")
    args$overwrite <- TRUE
    args$backend <- backend
    args$chunk_reader <- backend
    
    # LaF defaults to no header; fix here
    if(backend == "LaF") args$backend_args$header <- TRUE
    
    expect_s3_class(dff <- do.call(csv_to_disk.frame, args), "disk.frame")
    expect_equal(nrow(dff), nrow(df))
    expect_equal(ncol(dff), ncol(df))
    expect_equal(as.character(data.table::as.data.table(dff)$id1), df$id1)
    
    delete(dff)
  })
}

##### chunked single file #####
for(backend in BACKENDS) {
  for(chunk_reader in CHUNK_READERS) {
    test_that(glue::glue("Single file: {backend} works with chunk_reader {chunk_reader}"), {
      args <- list()
      args$infile <- file.path(tempdir(), "tmp_pls_delete_csv2df.csv")
      args$outdir <- file.path(tempdir(), "tmp_pls_delete_csv2df.df")
      args$overwrite <- TRUE
      args$backend <- backend
      args$chunk_reader <- chunk_reader
      args$in_chunks <- NCHUNKS
      
      # LaF defaults to no header; fix here
      if(backend == "LaF") args$backend_args$header <- TRUE
      
      if(chunk_reader == "LaF" & backend == "data.table") {
        # for LaF with backend readr, readr will be used instead
        expect_error(dff <- do.call(csv_to_disk.frame, args))
        
      } else {
        
        dff <- suppressWarnings(do.call(csv_to_disk.frame, args))
        expect_s3_class(dff, "disk.frame")
        expect_equal(nrow(dff), nrow(df))
        expect_equal(ncol(dff), ncol(df))
        
        dff_id1 <- suppressWarnings(as.character(data.table::as.data.table(dff)$id1))
        expect_equal(dff_id1, df$id1)
        expect_equal(nchunks(dff), NCHUNKS)
        
        delete(dff)
      }      
    })
  }
}


##### Multiple files #####
for(backend in BACKENDS) {
  test_that(glue::glue("{backend} works with multiple files"), {
    df_expected <- bind_rows(df, df, df)
    
    args <- list()
    args$infile <- c(file.path(tempdir(), "tmp_pls_delete_csv2df.csv"),
                     file.path(tempdir(), "tmp_pls_delete_csv2df2.csv"),
                     file.path(tempdir(), "tmp_pls_delete_csv2df3.csv"))
                     
    args$outdir <- file.path(tempdir(), "tmp_pls_delete_csv2df.df")
    args$overwrite <- TRUE
    args$backend <- backend
    args$chunk_reader <- backend
    
    # LaF defaults to no header; fix here
    if(backend == "LaF") args$backend_args$header <- TRUE
    
    expect_s3_class(dff <- do.call(csv_to_disk.frame, args), "disk.frame")
    expect_equal(nrow(dff), nrow(df_expected))
    expect_equal(ncol(dff), ncol(df_expected))
    expect_equal(as.character(data.table::as.data.table(dff)$id1), df_expected$id1)
    
    delete(dff)
  })
}

##### Multiple files, chunked #####
for(backend in BACKENDS) {
  for(chunk_reader in CHUNK_READERS) {
    test_that(glue::glue("{backend} works with multiple files, chunk_reader {chunk_reader}"), {
      df_expected <- bind_rows(df, df, df)
      
      args <- list()
      args$infile <- c(file.path(tempdir(), "tmp_pls_delete_csv2df.csv"),
                       file.path(tempdir(), "tmp_pls_delete_csv2df2.csv"),
                       file.path(tempdir(), "tmp_pls_delete_csv2df3.csv"))
      
      args$outdir <- file.path(tempdir(), "tmp_pls_delete_csv2df.df")
      args$overwrite <- TRUE
      args$backend <- backend
      args$chunk_reader <- chunk_reader
      args$in_chunks <- c(1, NCHUNKS, NCHUNKS * 2)
      
      # LaF defaults to no header; fix here
      if(backend == "LaF") args$backend_args$header <- TRUE
      
      
      if(chunk_reader == "LaF" & backend == "data.table") {
        # for LaF with backend readr, readr will be used instead
        expect_error(dff <- do.call(csv_to_disk.frame, args))
        
      } else {
        
        dff <- suppressWarnings(do.call(csv_to_disk.frame, args))
        expect_s3_class(dff, "disk.frame")
        expect_equal(nrow(dff), nrow(df_expected))
        expect_equal(ncol(dff), ncol(df_expected))
        
        dff_id1 <- suppressWarnings(as.character(data.table::as.data.table(dff)$id1))
        expect_equal(dff_id1, df_expected$id1)
        expect_equal(nchunks(dff), sum(args$in_chunks))
        
        delete(dff)
      }      
      
    })
  }
}

##### Multiple files, chunked, tabbed delimiter #####
for(backend in BACKENDS) {
  for(chunk_reader in CHUNK_READERS) {
    test_that(glue::glue("{backend} works with multiple files, chunk_reader {chunk_reader}"), {
      df_expected <- bind_rows(df, df, df)
      
      args <- list()
      args$infile <- c(file.path(tempdir(), "tmp_pls_delete_csv2df.tab"),
                       file.path(tempdir(), "tmp_pls_delete_csv2df2.tab"),
                       file.path(tempdir(), "tmp_pls_delete_csv2df3.tab"))
      
      args$outdir <- file.path(tempdir(), "tmp_pls_delete_csv2df.df")
      args$overwrite <- TRUE
      args$backend <- backend
      args$chunk_reader <- chunk_reader
      args$in_chunks <- c(1, NCHUNKS, NCHUNKS * 2)
      
      if(backend == "data.table") {
        args$backend_args$sep <- "\t"
      } else if(backend == "readr") {
        args$backend_args$delim <- "\t"
      } else if(backend == "LaF") {
        args$backend_args$sep <- "\t"
        args$backend_args$header <- TRUE
      }
      
      if(chunk_reader == "data.table") {
        args$chunk_reader_args$sep <- "\t"
      } else {
        args$chunk_reader_args$delim <- "\t"
      }
      
      if(chunk_reader == "LaF" & backend == "data.table") {
        # for LaF with backend readr, readr will be used instead
        expect_error(dff <- do.call(csv_to_disk.frame, args))
        
      } else {
        dff <- suppressWarnings(do.call(csv_to_disk.frame, args))
        expect_s3_class(dff, "disk.frame")
        expect_equal(nrow(dff), nrow(df_expected))
        expect_equal(ncol(dff), ncol(df_expected))
        
        dff_id1 <- suppressWarnings(as.character(data.table::as.data.table(dff)$id1))
        expect_equal(dff_id1, df_expected$id1)
        expect_equal(nchunks(dff), sum(args$in_chunks))
        
        delete(dff)
      }      
      
    })
  }
}

##### chunked single file, sharded #####
# for(backend in BACKENDS) {
#   for(chunk_reader in CHUNK_READERS) {
#     test_that(glue::glue("Single file: {backend} works with chunk_reader {chunk_reader}"), {
#       args <- list()
#       args$infile <- file.path(tempdir(), "tmp_pls_delete_csv2df.csv")
#       args$outdir <- file.path(tempdir(), "tmp_pls_delete_csv2df.df")
#       args$overwrite <- TRUE
#       args$backend <- backend
#       args$chunk_reader <- chunk_reader
#       args$in_chunks <- NCHUNKS
#       # args$shardby <- "id3"
#       
#       # LaF defaults to no header; fix here
#       if(backend == "LaF") args$backend_args$header <- TRUE
#       
#       if(chunk_reader == "LaF" & backend == "data.table") {
#         # for LaF with backend readr, readr will be used instead
#         expect_error(dff <- do.call(csv_to_disk.frame, args))
#         
#       } else {
#         if(backend == "LaF" & chunk_reader == "LaF" |
#            backend == "readr" & chunk_reader %in% c("bigreadr", "data.table", "readr") |
#            backend == "data.table") {
#           expect_silent(dff <- do.call(csv_to_disk.frame, args))
#         } else {
#           expect_message(dff <- do.call(csv_to_disk.frame, args))
#         }
#         
#         expect_s3_class(dff, "disk.frame")
#         expect_equal(nrow(dff), nrow(df))
#         expect_equal(ncol(dff), ncol(df))
#         
#         if(backend == "data.table" | 
#            backend == "readr") {
#           expect_silent(dff_id1 <- as.character(data.table::as.data.table(dff)$id1))
#         } else {
#           # warning re binding character & factor vector
#           expect_warning(dff_id1 <- as.character(data.table::as.data.table(dff)$id1)) 
#         }
#         expect_equal(dff_id1, df$id1)
#         expect_equal(nchunks(dff), NCHUNKS)
#         
#         delete(dff)
#       }      
#     })
#   }
# }

##### Flights test #####
for(backend in BACKENDS) {
  for(chunk_reader in CHUNK_READERS) {
    test_that(glue::glue("Flights file: {backend} works with chunk_reader {chunk_reader}"), {
      args <- list()
      args$infile <- file.path(tempdir(), "tmp_pls_delete_flights.csv")
      args$outdir <- file.path(tempdir(), "tmp_pls_delete_flights.df")
      args$overwrite <- TRUE
      args$backend <- backend
      args$chunk_reader <- chunk_reader
      args$in_chunks <- NCHUNKS
      
      # LaF defaults to no header; fix here
      if(backend == "LaF") args$backend_args$header <- TRUE
      
      if(chunk_reader == "LaF" & backend == "data.table") {
        # for LaF with backend readr, readr will be used instead
        expect_error(dff <- do.call(csv_to_disk.frame, args))
        
      } else {
        dff <- suppressWarnings(do.call(csv_to_disk.frame, args))
        expect_s3_class(dff, "disk.frame")
        expect_equal(nrow(dff), nrow(nycflights13::flights))
        expect_equal(ncol(dff), ncol(nycflights13::flights))
        
        # expect NAs to be treated as ""; see data.table::fread
        dff_tailnum <- suppressWarnings(as.character(data.table::as.data.table(dff)$tailnum))
        dff_tailnum[dff_tailnum == ""] <- NA 
        expect_equal(dff_tailnum, nycflights13::flights$tailnum)
        expect_equal(nchunks(dff), NCHUNKS)
        
        delete(dff)
      }      
    })
  }
}

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
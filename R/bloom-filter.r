# two more hash functions -------------------------------------------------
hashstr2i2 <- function(x, ngrp, i) {
  if(i == 1) {
    hashstr2i(x, ngrp)  
  } else if(i == 2) {
    hashstr2i(x, ngrp, 17, 19, 23)
  } else if(i == 3) {
    hashstr2i(x, ngrp, 29, 31, 37)  
  }
}

#' Use bloomfilters to make 
#' @importFrom data.table setDT
#' @param df a disk.frame
#' @export
make_bloomfilter <- function(df, cols, ...) {
  # TODO multicolumn bloomfilter
  if(length(cols) != 1) {
    exp1 = substitute(make_bloomfilter(df, cols, ...))
    stop("You have specified more than one column when creating a bloomfilter with `make_bloomfilter`, this isn't supported at the moment.\n Please raise an issue at https://github.com/xiaodaigh/disk.frame/issues if you want this feature.")
  }

  # create .metadata folder
  p  = attr(df, "path")
  if(!fs::dir_exists(file.path(p,".metadata"))) {
    fs::dir_create(file.path(p,".metadata"))
  }
  
  if(!fs::dir_exists(file.path(p,".metadata", "bloomfilter"))) {
    fs::dir_create(file.path(p,".metadata", "bloomfilter"))
  }
  
  if(!fs::dir_exists(file.path(p,".metadata", "bloomfilter", cols))) {
    fs::dir_create(file.path(p,".metadata", "bloomfilter", cols))
  }
  
  # save down a fst file for checking
  dir_info_for_hashing = setDT(fs::dir_info(p))[type == "file", .(size, modification_time)]
  fst::write_fst(dir_info_for_hashing, file.path(p, ".metadata", "bloomfilter", cols, "info_check.fst"))
  
  fst::write_fst(data.frame(cols = cols), file.path(p, ".metadata", "bloomfilter", cols, "cols.fst"))

  # for each chunk create the 3 hashes
  ok = cimap(df %>% srckeep(cols), ~{
    x = unique(as.character(setDT(.x)[, cols, with = FALSE][[1]]))
    # print(x)
    ngrps = as.integer(2^31-1)

    bfpath = file.path(p, ".metadata", "bloomfilter", cols, paste0("chunk", .y))

    # clean out the bloomfilter
    hash_df = disk.frame(bfpath)
    delete(hash_df)
    hash_df = disk.frame(bfpath)

    add_chunk(hash_df, data.frame(hash = unique(hashstr2i2(x, ngrps, 1))))
    add_chunk(hash_df, data.frame(hash = unique(hashstr2i2(x, ngrps, 2))))
    add_chunk(hash_df, data.frame(hash = unique(hashstr2i2(x, ngrps, 3))))
  }, lazy = FALSE)
  
  df
}

#' Return the chunks that values are in certain chunks
#' @export
bf_likely_in_chunks <- function(df, cols, values) {
  p  = attr(df, "path")
  bfpath = file.path(p, ".metadata", "bloomfilter", cols)
  
  
  if(!fs::dir_exists(bfpath)) {
    stop(sprintf("bloomfilter does not exist for column `%s`", cols))
  }
  
  dir_info_for_hashing_now = setDT(fs::dir_info(p))[type == "file", .(size, modification_time)]
  
  dir_info_for_hashing_orig = file.path(p, ".metadata", "bloomfilter", cols, "info_check.fst") %>% fst::read_fst(as.data.table = TRUE)
  
  rows_same = nrow(dir_info_for_hashing_now) == nrow(dir_info_for_hashing_orig)
  
  size_time_same = sapply(names(dir_info_for_hashing_now), function(n) {
    all(dir_info_for_hashing_now[, n, with = FALSE] == dir_info_for_hashing_orig[, n, with = FALSE])
  }) %>% all
  
  if((!rows_same) || (!size_time_same)) {
    stop("bloomfilter cannot be used: the bloomfilter was likely built on a different dataset")
  }
  
  
  
  
  cvalues = as.character(unique(values))
  
  
  cols = as.character(fst::read_fst(file.path(bfpath, "cols.fst"))[[1]])
  
  dfpath = list.dirs(bfpath, full.names = TRUE, recursive = FALSE)[2]
  
  future.apply::future_sapply(list.dirs(bfpath, full.names = TRUE, recursive = FALSE), function(dfpath) {
    df1 = disk.frame(dfpath)
    
    cimap(df1, ~{
      values_hashed = hashstr2i2(cvalues, 2^31-1, .y)
      
      values_hashed %in% .x$hash
    }, lazy = FALSE) %>% unlist %>% all
  }, USE.NAMES = FALSE) %>% which
}

#' Use bloom filter
#' @export
use_bloom_filter <- function(df, cols, values) {
  chunks = bf_likely_in_chunks(df, cols, values)
  df = srckeepchunks(df, chunks)
  
  p  = attr(df, "path")
  bfpath = file.path(p, ".metadata", "bloomfilter", cols)
  cols = as.character(fst::read_fst(file.path(bfpath, "cols.fst"))[[1]])
  
  df %>% 
    filter(!!sym(cols) %in% values)
}


if (FALSE) {
  library(disk.frame)
  setup_disk.frame()
  plan(sequential)
  df = nycflights13::flights %>% as.disk.frame(shardby = c("carrier"))
  df
  
  cols = "carrier"

  make_bloomfilter(df, c("origin", "dest"))
  
  values = "UA"
  system.time(make_bloomfilter(df, "carrier"))
  bf_likely_in_chunks(df, "carrier", values)
  
  system.time(bf_likely_in_chunks(df, "carrier", "values"))
  
  system.time(d1 <- df %>% 
    use_bloom_filter("carrier",  "UA") %>% 
      collect)
  
  attr(d1, "keep_chunks")
  
  system.time(d1 %>% collect)
  
  system.time(d2 <- df %>% 
    filter(carrier == "UA") %>% 
    collect)
  
  df = disk.frame("C:/data/airontimecsv.df")
  
  
  
  df2 = df %>% 
    shard(shardby = "UNIQUE_CARRIER", outdir = "c:/data/airontime.sharded.df")
  
  #move_to(df2, "c:/data/airontime.sharded.df")
  df2 %>% 
    make_bloomfilter("UNIQUE_CARRIER")
  
  system.time(bf_likely_in_chunks(df2, "AA"))
  
  a <- df2 %>% 
    use_bloom_filter("AA") %>% 
    collect
  
}



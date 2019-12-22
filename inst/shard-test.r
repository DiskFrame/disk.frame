library(disk.frame)
a = data.frame(id = sample(1:100, 1000, replace=TRUE), values = runif(1000))
adf = as.disk.frame(a, nchunks = 6)

adf_sharded = adf %>% 
  mutate(rand_chunk = sample(1:2,n(), replace=TRUE)) %>% # create a new column to sharding into sub-shards
  shard(shardby =  c("id", "rand_chunk")) 

adf_with_bloomfilter = adf_sharded %>% 
  make_bloomfilter("id")

adf_with_bloomfilter %>% 
  bf_likely_in_chunks("id", 1)

adf_with_bloomfilter %>% 
  use_bloom_filter("id", 1) %>% 
  collect

adf_with_bloomfilter %>% 
  filter(id %in% 1) %>% 
  collect


a = data.frame(id3 = sample(letters, 1000, replace=TRUE), values = runif(1000))
adf = as.disk.frame(a, nchunks = 6)

adf_sharded = adf %>% 
  #mutate(rand_chunk = sample(1:2,n(), replace=TRUE)) %>% # create a new column to sharding into sub-shards
  #shard(shardby =  c("id3", "rand_chunk"))
  shard(shardby =  c("id3"))

df = adf_sharded %>% 
  make_bloomfilter("id3")

df %>% 
  bf_likely_in_chunks("id3", "a")

df %>% 
  use_bloom_filter("id3", "a") %>% 
  collect

hashstr2i = disk.frame:::hashstr2i

df1 = nycflights13::flights %>% as.disk.frame(shardby = c("carrier"))
make_bloomfilter(df1, "carrier")
expect_true(length(bf_likely_in_chunks(df1, "carrier", "UA")) == 1)

use_bloom_filter(df1, "carrier", "UA") %>% collect

expect_equal(nrow(collect(use_bloom_filter(df, "carrier", "UA"))), nrow(filter(nycflights13::flights, carrier == "UA")))

df %>% 
  use_bloom_filter("carrier", "UA") %>% collect

# one-value check ---------------------------------------------------------
mehmeh <- function(df, id, values) {
  df %>% filter(!!sym(id) %in% values)
}

mehmeh(adf_with_bloomfilter, "id", 1) %>% 
  collect


id=  "id"
values = 1
adf %>% filter((!!sym(id)) %in% values) %>% collect

# which chunks contain id == 1
adf_with_bloomfilter %>% 
  bf_likely_in_chunks("id", 1)

# check
adf_with_bloomfilter %>% 
  cmap(~any(.x$id==1)) %>% 
  collect_list %>% 
  unlist %>% 
  which

# use blom filter to filter for rows
adf_with_bloomfilter %>%
  use_bloom_filter("id", 1) %>% 
  collect

a = adf_with_bloomfilter %>%
  use_bloom_filter("id", 1)

a

attr(a, "lazyfn")

adf_with_bloomfilter %>% 
  filter(id == 1) %>% 
  collect

# multi-value check -------------------------------------------------------


# which chunks contain id %in% 1:2
adf_with_bloomfilter %>% 
  bf_likely_in_chunks("id", 1:2)

adf_with_bloomfilter %>% 
  cmap(~any(.x$id %in% 1:2)) %>% 
  collect_list %>% 
  unlist %>% 
  which


# use blom filter to filter for rows
adf_with_bloomfilter %>%
  use_bloom_filter("id", 1:2) %>% 
  collect


adf_with_bloomfilter %>% 
  filter(id %in% 1:2) %>% 
  collect

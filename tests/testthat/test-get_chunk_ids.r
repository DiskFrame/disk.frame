context("test-get_chunk_ids")

setup({
  #setup_disk.frame(workers = 1)
  df = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), "tmp_del_delete", overwrite=T)
})

test_that("get_chunk_ids", {
  df = disk.frame("tmp_del_delete")
  
  gci = get_chunk_ids(df)
  expect_type(get_chunk_ids(df), "character")

  gcis = get_chunk_ids(df, strip_extension = F)
  expect_true("1.fst" %in% gcis)
})

teardown({
  fs::dir_delete("tmp_del_delete")
})
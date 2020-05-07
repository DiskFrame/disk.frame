context("test-get_chunk")

setup({
  #setup_disk.frame(workers = 1)
  diskf = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_del_delete"), overwrite=T)
})

test_that("get_chunk", {
  diskf = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_del_delete"), overwrite=T)
  diskf = disk.frame(file.path(tempdir(), "tmp_del_delete"))
  expect_s3_class(get_chunk(diskf, 1), "data.frame")

  expect_s3_class(get_chunk(diskf, "1.fst"), "data.frame")
})

test_that("get_chunk try to obtain variables not present", {
  diskf = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_del_delete"), overwrite=T)
  expect_error(get_chunk(diskf, 1, keep = "id_doesnt_exist"))
})

test_that("get_chunk try to obtain variables not in keep", {
  diskf = as.disk.frame(disk.frame:::gen_datatable_synthetic(1e5+11), file.path(tempdir(), "tmp_del_delete"), overwrite=T)
  diskf2 = diskf %>% 
     srckeep(c("id1", "id2", "id3"))
   
  expect_error(get_chunk(diskf2, 1, keep = c("id1", "id3", "id4")))
})

test_that("get_chunk warning due to table not existing", {
  diskf = disk.frame(file.path(tempdir(), "tmp_del_delete"))
  expect_s3_class(get_chunk(diskf, 1), "data.frame")
  
  expect_s3_class(get_chunk(diskf, "1.fst"), "data.frame")
})

teardown({
  fs::dir_delete(file.path(tempdir(), "tmp_del_delete"))
})
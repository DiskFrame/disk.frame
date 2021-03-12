context("test-foverlaps")

setup({
  #setup_disk.frame(workers = 1)
})

# TODO currently it's not possible to do 
test_that("test foverlap with data.frame", {
  x = as.disk.frame(data.table(start=c(5,31,22,16), end=c(8,50,25,18), val2 = 7:10))
  y = as.disk.frame(data.table(start=c(10, 20, 30), end=c(15, 35, 45), val1 = 1:3))
  byxy = c("start", "end")
  xy.df = foverlaps.disk.frame(
    x, y, by.x = byxy, by.y = byxy,
    merge_by_chunk_id = TRUE, overwrite = TRUE)
  
  collect(xy.df)
  
  testthat::expect_equal(nrow(xy.df), 3)
})
  

# TODO this is also not a good test case
# test_that("test foverlap with disk.frame", {  
#   x = data.table(start=c(5,31,22,16), end=c(8,50,25,18), val2 = 7:10)
#   y = data.table(start=c(10, 20, 30), end=c(15, 35, 45), val1 = 1:3)
#   setkey(y, start, end)
#   
#   dx = shard(x, "tmp_fo.df", overwrite = T, shardby=c("start","end"))
#   dy = shard(y, "tmp_to.df", overwrite = T, shardby=c("start","end"))
#   
#   xy1 = foverlaps(x,y, type="any", which = T)
#   
#   dxy1 = foverlaps.disk.frame(dx, dy, type="any", outdir="tmp_fo_out2.df") ## return overlap join
#   dxy1c = dxy1 %>% collect
#   
#   foverlaps.disk.frame(dx, dy, type="any", mult="first", outdir="tmp_fo_out2.df") ## returns only first match
#   foverlaps.disk.frame(dx, dy, type="within", outdir="tmp_fo_out3.df") ## matches iff 'x' is within 'y'
# })

teardown({
  
})
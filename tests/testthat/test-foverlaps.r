context("test-foverlaps")

setup({
  setup_disk.frame(workers = 1)
})

test_that("data.table .N", {
  x = data.table(start=c(5,31,22,16), end=c(8,50,25,18), val2 = 7:10)
  y = data.table(start=c(10, 20, 30), end=c(15, 35, 45), val1 = 1:3)
  setkey(y, start, end)
  
  dx = as.disk.frame(x, "tmp_fo.df", overwrite = T)
  
  
  foverlaps(x,y, type="any", which = T)
  plan(transparent)
  collect(foverlaps.disk.frame(dx, y, type="any", which=TRUE, outdir="tmp_fo_out1.df")) ## return overlap indices
  
  
  
  foverlaps.disk.frame(dx, y, type="any", outdir="tmp_fo_out2.df") ## return overlap join
  foverlaps.disk.frame(dx, y, type="any", mult="first", outdir="tmp_fo_out2.df") ## returns only first match
  foverlaps.disk.frame(dx, y, type="within", outdir="tmp_fo_out3.df") ## matches iff 'x' is within 'y'
})

teardown({
  
})
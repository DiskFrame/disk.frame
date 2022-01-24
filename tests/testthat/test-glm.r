context("test-glm")

setup({
  #setup_disk.frame(workers = 1)
})

test_that("glm", {
  cars.df = as.disk.frame(cars, outdir = file.path(tempdir(), "cars.df"), overwrite = TRUE)
  
  majorv = as.integer(version$major)
  minorv = as.integer(strsplit(version$minor, ".", fixed=TRUE)[[1]][1])
  
  if((majorv == 3) & (minorv < 6)) {
    expect_warning({m <- dfglm(dist~speed, cars.df, glm_backend = "biglm")})
  } else {
    m <- dfglm(dist~speed, cars.df, glm_backend = "biglm")
  }
  summary(m)
  
  if((majorv == 3) & (minorv >= 6) ) {
    broom::tidy(m)
  }
  
  m <- dfglm(dist~speed, cars.df, glm_backend = "speedglm")
  summary(m)
})

teardown({
  fs::dir_delete(file.path(tempdir(), "cars.df"))
})
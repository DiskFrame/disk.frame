# df_bookdown_build <- function() {
#   rmds = list.files("vignettes", pattern = "*.Rmd")
#   sapply(rmds, function(file) {
#     fs::file_copy(
#       file.path("book", file), 
#       file.path("vignettes", file),
#       overwrite = TRUE
#       )
#   })
#   if(fs::dir_exists("book/_bookdown_files")) {
#     fs::dir_delete("book/_bookdown_files")
#   }
#   while(fs::dir_exists("book/_bookdown_files")) {
#     Sys.sleep(1)
#   }
#   rmarkdown::render_site("book", encoding = 'UTF-8')
# }

df_build_site <- function() {
  df_setup_vignette()
  devtools::document()
  #devtools::build_readme()
  pkgdown::build_site()
}

# setup vignette but does not build
df_setup_vignette <- function(excl = "", strip_number = FALSE) {
  # remove cache
  purrr::walk(list.dirs("vignettes/",recursive = FALSE), ~{
    fs::dir_delete(.x)
  })
  
  # remove vignette
  purrr::walk(list.files("vignettes/", pattern = "*.Rmd", full.names = TRUE), fs::file_delete)

  # copy the RMD from book
  lf = list.files("book", pattern="*.Rmd")
  # strips the names
  lf = lf[!is.na(as.integer(sapply(lf, function(x) substr(x, 1, 2))))]
  
  
  lf = lf[sapply(lf, function(x) !(x %in% excl))]
  
  purrr::walk(lf, function(file) {
    fs::file_copy(
      file.path("book", file), 
      ifelse(strip_number,file.path("vignettes", substr(file, 4, nchar(file))),file.path("vignettes", file))
      , overwrite = TRUE)
  })
  NULL
}

df_test <- function() {
  # rename tests
  if(fs::dir_exists("tests_manual")) {
    fs::dir_copy("tests_manual", "tests")
    fs::dir_delete("tests_manual")
  }
  
  devtools::test()
  
  if(fs::dir_exists("tests")) {
    fs::dir_copy("tests", "tests_manual")
    fs::dir_delete("tests")
  }
}

df_build_vignettes_for_cran <- function() {
  rmd_files = list.files("vignettes/", pattern = "*.Rmd", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)
  df_ready_for_cran()
  devtools::build_vignettes()
}

df_ready_for_cran <- function() {
  devtools::clean_vignettes()
  df_setup_vignette(excl = c("08-more-epic.Rmd", "06-vs-dask-juliadb.Rmd", "01-intro.Rmd"), strip_number = TRUE)
  
  devtools::document()
  #devtools::build_readme()
  
  # rename tests
  if(fs::dir_exists("tests")) {
    fs::dir_copy("tests", "tests_manual")
    fs::dir_delete("tests")
  }
}

df_check <- function() {
  df_ready_for_cran()
  devtools::check(args = c('--as-cran'))
}

df_release <- function() {
  df_ready_for_cran()
  devtools::release()
}

if(F) {
  df_check()
}


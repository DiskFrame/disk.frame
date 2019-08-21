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
  pkgdown::build_site()
}

df_setup_vignette <- function() {
  # remove cache
  purrr::walk(list.dirs("vignettes/",recursive = FALSE), ~{
    fs::dir_delete(.x)
  })

  # copy the RMD from book
  lf = list.files("book", pattern="*.Rmd")
  lf = lf[!is.na(as.integer(sapply(lf, function(x) substr(x, 1, 2))))]
  
  purrr::walk(lf, function(file) {
    fs::file_copy(
      file.path("book", file), 
      file.path("vignettes", substr(file, 4, nchar(file))), overwrite = TRUE)
  })
  NULL
}

df_check <- function() {
  df_setup_vignette()
  
  # rename tests
  if(fs::dir_exists("tests")) {
    fs::dir_copy("tests", "tests_manual")
    fs::dir_delete("tests")
  }
  
  # run check
  devtools::check(args = c('--as-cran'))
}

if(F) {
  df_check()
}


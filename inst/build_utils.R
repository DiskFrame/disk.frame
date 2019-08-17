df_bookdown_build <- function() {
  rmds = list.files("vignettes", pattern = "*.Rmd")
  sapply(rmds, function(file) {
    fs::file_copy(
      file.path("vignettes", file), 
      file.path("book", file),
      overwrite = TRUE
      )
  })
  if(fs::dir_exists("book/_bookdown_files")) {
    fs::dir_delete("book/_bookdown_files")
  }
  while(fs::dir_exists("book/_bookdown_files")) {
    Sys.sleep(1)
  }
  rmarkdown::render_site("book", encoding = 'UTF-8')
}

df_build_site <- function() {
  dir("vignettes")
  tidyselect::ends_with()
  pkgdown::build_site()
}

df_check <- function() {
  lf = list.files("book", pattern="*.Rmd")
  lf = lf[!is.na(as.integer(sapply(lf, function(x) substr(x, 1, 2))))]
  
  purrr::map(lf, function(file) {
    fs::file_copy(
      file.path("book", file), 
      file.path("vignettes", substr(file, 4, nchar(file))), overwrite = T)
  })
  
  devtools::check(args = c('--as-cran'))
}
df_check()

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
  df_build_readme()
  df_setup_vignette()
  devtools::document()
  #devtools::build_readme()
  pkgdown::build_site()
}

df_build_readme <- function() {
  if(fs::dir_exists("README_cache")) {
    fs::dir_delete("README_cache")
  }
  # 
  # if(fs::file_exists("README.md")) {
  #   fs::file_delete("README.md")
  # }
  
  rmarkdown::render("README.rmd", output_file = "README.md")

  if(fs::file_exists("README.html")) {
    fs::file_delete("README.html")
  }

  if(fs::dir_exists("README_cache")) {
    fs::dir_delete("README_cache")
  }
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
  # if(fs::dir_exists("tests_manual")) {
  #   fs::dir_copy("tests_manual", "tests")
  #   Sys.sleep(3) # allow enough time for it to happen
  #   fs::dir_delete("tests_manual")
  # }
  
  devtools::test()
  
  # if(fs::dir_exists("tests")) {
  #   fs::dir_copy("tests", "tests_manual")
  #   Sys.sleep(8) # allow enough time for it to happen
  #   fs::dir_delete("tests")
  # }
}

df_build_vignettes_for_cran <- function() {
  rmd_files = list.files("vignettes/", pattern = "*.Rmd", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)

  rmd_files = list.files("vignettes/", pattern = "*.tex", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)

  rmd_files = list.files("vignettes/", pattern = "*.pdf", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)
  
  df_ready_for_cran()
  
  # knit all files
  lapply(
    list.files("vignettes/", pattern = "*.Rmd", full.names = TRUE),
    rmarkdown::render
  )

  rmd_files = list.files("vignettes/", pattern = "*.pdf", full.names = TRUE)
  rmd_files = paste0(substr(rmd_files, 1, nchar(rmd_files)-3),"Rmd")
  purrr::map(rmd_files, function(x) {
    if (file.exists(x)) {
      fs::file_delete(x)
    }
  })
  
  rmd_files = list.files("vignettes/", pattern = "*.tex", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)
  
  rmd_files = list.dirs("vignettes/", recursive = FALSE)
  purrr::map(rmd_files, fs::file_delete)
  
  # create the as is files
  mergedf = data.frame(
    pdfs = c("concepts", "convenience-features", "custom-group-by", "data-table-syntax", "glm", "group-by", "ingesting-data", "intro-disk-frame"),
    asis = c("concepts", "convenience-features", "custom-group-by", "data-table-syntax", "glm", "group-by", "ingesting-data", "intro-disk-frame"),
    index_entry = c(
      "Key disk.frame concepts",
      "Convenience Features",
      "Custom Group-by",
      "Using data.table syntax",
      "Generalized Linear Models (GLMs)",
      "Group-by",
      "Ingesting Data",
      "Quick-start"
      )
  )
  
  mergedf$rmd_files = paste0("vignettes/", mergedf$pdfs, ".pdf")
  
  abc = data.frame(rmd_files = list.files("vignettes/", pattern = "*.pdf", full.names = TRUE))
  
  mergedf = mergedf %>% left_join(abc, by = "rmd_files")

  purrr::walk2(paste0(mergedf$rmd_files, ".asis"), mergedf$index_entry,  function(x, y) {
   xf = file(x)
   writeLines(c(
     glue::glue("%\\VignetteIndexEntry{|y|}", .open="|", .close="|"),
     glue::glue("%\\VignetteEngine{R.rsp::asis}", .open="|", .close="|")),
     xf)
   close(xf)
  })
 
  # purrr::walk2(mergedf$pdf, mergedf$index_entry,  function(x, y) {
  #   xf = file(file.path("vignettes", paste0(x, ".Rnw")))
  #   writeLines(c(
  #     glue::glue("\\documentclass{article}", .open="|", .close="|"),
  #     glue::glue("\\usepackage{pdfpages}", .open="|", .close="|"),
  #     glue::glue("%\\VignetteIndexEntry{|y|}", .open="|", .close="|"),
  #     glue::glue("\\begin{document}", .open="|", .close="|"),
  #     glue::glue("\\includepdf[pages=-, fitpaper=true]{|x|.pdf}", .open="|", .close="|"),
  #     glue::glue("\\end{document}", .open="|", .close="|")),
  #     xf)
  #   close(xf)
  # })
}

df_ready_for_cran <- function() {
  df_build_readme()
  
  devtools::clean_vignettes()
  df_setup_vignette(excl = c("08-more-epic.Rmd", "06-vs-dask-juliadb.Rmd", "01-intro.Rmd"), strip_number = TRUE)
  
  devtools::document()
  
  # rename tests
  if(fs::dir_exists("tests")) {
    fs::dir_copy("tests", "tests_manual")
    fs::dir_delete("tests")
  }
  
  # remove README_cache
  if(fs::dir_exists("README_cache")) {
    fs::dir_delete("README_cache")
  }
}

df_check <- function() {
  df_ready_for_cran()
  # remove the rmd files
  rmd_files = list.files("vignettes/", pattern = "*.Rmd", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)
  
  devtools::check(args = c('--as-cran'))
}

df_release <- function() {
  df_ready_for_cran()
  
  # remove the rmd files
  rmd_files = list.files("vignettes/", pattern = "*.Rmd", full.names = TRUE)
  purrr::map(rmd_files, fs::file_delete)
  
  
  devtools::release()
}

df_ultimate <- function() {
  df_check()
  df_release()
  df_build_site()
}

if(F) {
  df_check()
}


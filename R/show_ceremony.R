#' Show the code to setup disk.frame
#' @export
show_ceremony <- function() {
  glue::glue(crayon::green(ceremony_text()))
}


#' @rdname show_ceremony
#' @export
ceremony_text <- function() {
"
# this willl set disk.frame with multiple workers
setup_disk.frame()
# this will allow unlimited amount of data to be passed from worker to worker
options(future.globals.maxSize = Inf)
"
}

#' @rdname show_ceremony
#' @export
show_boilerplate <- function() show_ceremony()

#' @rdname show_ceremony
#' @export
insert_ceremony <- function() {
  if(requireNamespace("rstudioapi")) {
    rstudioapi::insertText(ceremony_text())
  } else {
    stop("insert ceremony can only be used inside RStudio")
  }
}

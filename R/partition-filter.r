#' Filter the dataset based on folder partitions
#' @param x a disk.frame
#' @param ... filtering conditions for filtering the disk.frame at (folder) partition level
#' @importFrom dplyr filter
#' @export
partition_filter <- function(x, ...) {
  expr = bquote(dplyr::filter(dataframe, .(substitute(...))))
  globals = find_globals_recursively(expr, parent.frame())
  
  attr(x, "partition_filter") = list(expr=expr, globals=globals)
  
  return(x)
}

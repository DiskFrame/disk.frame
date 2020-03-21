#' The tidy verbs implemented for disk.frame
#' @description 
#' Please see the tidyfast document for their usage
#' @export
#' @importFrom tidyfast dt_count dt_uncount dt_hoist dt_nest dt_unnest dt_fill dt_separate
#' @param ... Same as the tidyfast functions
#' @param .data a disk.frame
#' @rdname tidyfast_verbs
#' @family tidyfast verbs
#' @examples
#' library(tidyfast)
#' library(data.table)
#' 
#' #' create a disk.frame
#' disk.frame_to_split <- as.disk.frame(data.table(
#'   x = paste(letters, LETTERS, sep = ".")
#' ))
#' 
#' disk.frame_to_split %>% 
#'   dt_separate(x, into = c("lower", "upper")) %>% 
#'   collect
#' 
#' #' clean up
#' delete(disk.frame_to_split)
chunk_dt_count <- create_chunk_mapper(tidyfast::dt_count, as.data.frame = FALSE)

#' dt_count working on whole disk.frame
dt_count.disk.frame <- function(dt_, ..., na.rm = FALSE, wt = NULL) {
  stop("ZJ: I was up to here, and I need better understanding of NSE. Why?
       ifelse(is.null(wt), NULL, wt) is not going to work if wt is a column name")
  
  dt_ %>% 
    chunk_dt_count(..., na.rm = force(na.rm), wt = ifelse(is.null(wt), NULL, wt)) %>% 
    collect
}

#' @rdname tidyfast_verbs
#' @export
chunk_dt_uncount <- create_chunk_mapper(tidyfast::dt_uncount, as.data.frame = FALSE)

#' @rdname tidyfast_verbs
#' @export
chunk_dt_unnest = create_chunk_mapper(tidyfast::dt_unnest, as.data.frame = FALSE)

#' @rdname tidyfast_verbs
#' @export
chunk_dt_nest = create_chunk_mapper(tidyfast::dt_nest, as.data.frame = FALSE)

#' @rdname tidyfast_verbs
#' @export
chunk_dt_hoist = create_chunk_mapper(tidyfast::dt_hoist, as.data.frame = FALSE)

#' @rdname tidyfast_verbs
#' @export
chunk_dt_fill = create_chunk_mapper(tidyfast::dt_fill, as.data.frame = FALSE)

#' @rdname tidyfast_verbs
#' @export
dt_separate.disk.frame = create_chunk_mapper(tidyfast::dt_separate, as.data.frame = FALSE)

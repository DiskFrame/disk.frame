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
chunk_dt_count.disk.frame <- create_chunk_mapper(tidyfast::dt_count, as.data.frame = FALSE)

#' @rdname tidyfast_verbs
#' @export
chunk_dt_uncount.disk.frame <- create_chunk_mapper(tidyfast::dt_uncount, as.data.frame = FALSE)

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

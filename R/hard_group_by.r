#' #' Show a progress bar of the action being performed
#' #' @importFrom utils txtProgressBar setTxtProgressBar
#' #' @param df a disk.frame
#' #' @noRd
#' progressbar <- function(df) {
#'   if(attr(df,"performing", exact=TRUE) == "hard_group_by") {
#'     # create progress bar
#'     
#'     shardby = "acct_id"
#'     #list.files(
#'     fparent = attr(df,"parent", exact=TRUE)
#'     
#'     #tmp = file.path(fparent,".performing","inchunks")
#'     tmp = "tmphardgroupby2"
#'     
#'     l = length(list.files(fparent))
#'     pt_begin_split = proc.time()
#'     doprog <- function(pt_from, sleep = 1) {
#'       #tkpb = winProgressBar(title = sprintf("Hard Group By Stage 1(/2) - %s", shardby), label = "Checking completeness",
#'       #                      min = 0, max = l*1.5, initial = 0, width = 500)
#'       pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
#'       
#'       on.exit(close(pb))
#'       # on.exit(close(tkpb))
#'       while(length(list.files(file.path(tmp,l))) < l) {
#'         wl = length(list.files(file.path(tmp,1:l)))/l
#'         tt <- proc.time()[3] - pt_from[3]
#'         #list.files(
#'         avg_speed = tt/wl
#'         pred_speed = avg_speed*(l-wl) + avg_speed*l/2
#'         elapsed = round(tt/60,1)
#'         
#'         #setWinProgressBar(tkpb, wl, 
#'         #                  title = sprintf("Hard Group By Stage 1(/2) - %s", shardby),
#'         #                  label = sprintf("%.0f out of %d; avg speed %.2f mins; elapsed %.1f mins; another %.1f mins", wl,l, round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
#'         setTxtProgressBar(pb, length(list.files(file.path(tmp,l))), 
#'                           title = sprintf("Group By - %s", shardby))
#'         Sys.sleep(sleep)
#'       }
#'     }
#'     doprog(pt_begin_split, 1)
#'     
#'     pt_begin_collate = proc.time()
#'     doprog2 <- function(pt_from, sleep = 1) {
#'       # tkpb = winProgressBar(title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating", shardby), label = "Checking completeness",
#'                             # min = 0, max = l*1.5, initial = 0, width = 600)
#'       pb <- txtProgressBar(min = 0, max = l*1.5, style = 3)
#'       
#'       on.exit(close(pb))
#'       # on.exit(close(tkpb))
#'       while(length(list.files("large_sorted")) < l) {
#'         wl = length(list.files("large_sorted"))
#'         tt <- proc.time()[3] - pt_from[3]
#'         #list.files(
#'         avg_speed = tt/wl
#'         pred_speed = avg_speed*(l-wl)
#'         elapsed = round(tt/60,1)
#'         
#'         # setWinProgressBar(tkpb, l + wl/2, 
#'         #                   title = sprintf("Hard Group By - %s -- Stage 2 (of 2) collating -- %.0f out of %d chunks processed;", shardby, wl, l),
#'         #                   label = sprintf("avg %.2f min/chunk; %.1f mins elapsed; %.1f mins remaining;", round(avg_speed/60,2), elapsed, round(pred_speed/60,2)))
#'         setTxtProgressBar(pb, length(list.files("large_sorted")), 
#'                           title = sprintf("Hard Group By - %s", shardby))
#'         Sys.sleep(sleep)
#'       }
#'     }
#'     doprog2(pt_begin_collate, 1)
#'   }
#' }
#' 
#' #' Perform a hard group
#' #' @description
#' #' A hard_group_by is a group by that also reorganizes the chunks to ensure that
#' #' every unique grouping of `by`` is in the same chunk. Or in other words, every
#' #' row that share the same `by` value will end up in the same chunk.
#' #' @param df a disk.frame
#' #' @param ... grouping variables
#' #' @param outdir the output directory
#' #' @param nchunks The number of chunks in the output. Defaults = nchunks.disk.frame(df)
#' #' @param overwrite overwrite the out put directory
#' #' @param .add same as dplyr::group_by
#' #' @param .drop same as dplyr::group_by
#' #' @param shardby_function splitting of chunks: "hash" for hash function or "sort" for semi-sorted chunks
#' #' @param sort_splits for the "sort" shardby function, a dataframe with the split values.
#' #' @param desc_vars for the "sort" shardby function, the variables to sort descending.
#' #' @param sort_split_sample_size for the "sort" shardby function, if sort_splits is null, the number of rows to sample per chunk for random splits.
#' #' @export
#' #' @examples
#' #' iris.df = as.disk.frame(iris, nchunks = 2)
#' #' 
#' #' # group_by iris.df by specifies and ensure rows with the same specifies are in the same chunk
#' #' iris_hard.df = hard_group_by(iris.df, Species)
#' #' 
#' #' get_chunk(iris_hard.df, 1)
#' #' get_chunk(iris_hard.df, 2)
#' #' 
#' #' # clean up cars.df
#' #' delete(iris.df)
#' #' delete(iris_hard.df)
#' hard_group_by <- function(df, ..., .add = FALSE, .drop = FALSE) {
#'   UseMethod("hard_group_by")
#' }
#' 
#' #' @rdname hard_group_by
#' #' @export
#' #' @importFrom dplyr group_by
#' hard_group_by.data.frame <- function(df, ..., .add = FALSE, .drop = FALSE) {
#'   dplyr::group_by(df, ..., .add = FALSE, .drop = FALSE)
#' }
#' 
#' #' @rdname hard_group_by
#' #' @importFrom purrr map
#' #' @export
#' hard_group_by.disk.frame <- function(
#'     df, 
#'     ..., 
#'     outdir=tempfile("tmp_disk_frame_hard_group_by"), 
#'     nchunks = disk.frame::nchunks(df), 
#'     overwrite = TRUE, 
#'     shardby_function="hash", 
#'     sort_splits=NULL, 
#'     desc_vars=NULL, 
#'     sort_split_sample_size=100
#'   ) {
#'   overwrite_check(outdir, overwrite)
#'   
#'   ff = list.files(attr(df, "path"))
#'   stopifnot(shardby_function %in% c("hash", "sort"))
#'   
#'   if (shardby_function == "sort" && is.null(sort_splits)){
#'     # Sample enough per chunk to generate reasonable splits
#'     sample_size_per_chunk = ceiling(nchunks / disk.frame::nchunks(df)) * sort_split_sample_size
#'     
#'     # Sample and sort
#'     sort_splits_sample <- cmap(df, dplyr::sample_n, size=sample_size_per_chunk, replace=TRUE) %>% 
#'       select(...) %>%
#'       collect()
#'     
#'     # NSE
#'     tryCatch({
#'       sort_splits_sample <- sort_splits_sample %>%
#'         arrange(!!!syms(...))
#'     }, error = function(e) {
#'       sort_splits_sample <- sort_splits_sample %>%
#'         arrange(...)
#'     })
#'     
#'     # If 100 chunks, this return get 99 splits based on percentiles.
#'     ntiles <- round((1:(nchunks-1)) * (nrow(sort_splits_sample) / (nchunks)))
#'     
#'     # Get splits. May lead to less than nchunks if duplicates are selected.
#'     sort_splits <- sort_splits_sample %>% 
#'       dplyr::slice(ntiles) %>%
#'       distinct()
#'   }
#'   
#'   # test if the unlist it will error
#'   
#'   tryCatch({
#'     # This will return the variable names
#'     
#'     # TODO use better ways to do NSE
#'     # the below will fail if indeed ... can not be list-ed
#'     # there should be a better way to do this
#'     by <- unlist(list(...))
#'     
#'     # shard and create temporary diskframes
#'     tmp_df  = cmap(df, function(df1) {
#'       tmpdir = tempfile()
#'       shard(df1, shardby = by, nchunks = nchunks, outdir = tmpdir, overwrite = TRUE, shardby_function=shardby_function, sort_splits=sort_splits, desc_vars=desc_vars)
#'     }, lazy = FALSE)
#'     
#'     
#'     # now rbindlist
#'     res = rbindlist.disk.frame(tmp_df, outdir=outdir, overwrite = overwrite)
#'     
#'     # clean up the tmp dir
#'     purrr::walk(tmp_df, ~{
#'       fs::dir_delete(attr(.x, "path", exact=TRUE))
#'     })
#'     
#' 
#'     res1 <- NULL
#'     if(typeof(by) == "character") {
#'       eval(parse(text = glue::glue('res1 = chunk_group_by(res, {paste(by,collapse=",")})')))
#'     } else if(length(by) == 1) {
#'       res1 = res %>% dplyr::group_by({{by}}) 
#'     } else {
#'       eval(parse(text = glue::glue('res1 = chunk_group_by(res, {paste(by,collapse=",")})')))
#'     }
#'     
#'     res1
#'   }, error = function(e) {
#'     # message(e)
#'     # This will return the variable names
#'     by = rlang::enquos(...) %>% 
#'       substr(2, nchar(.))
#'     
#'     # shard and create temporary diskframes
#'     tmp_df  = cmap(df, function(df1) {
#'       tmpdir = tempfile()
#'       shard(df1, shardby = by, nchunks = nchunks, outdir = tmpdir, overwrite = TRUE, shardby_function=shardby_function, sort_splits=sort_splits, desc_vars=desc_vars)
#'     }, lazy = FALSE)
#'     
#'     # now rbindlist
#'     res = rbindlist.disk.frame(tmp_df, outdir=outdir, overwrite = overwrite)
#'     
#'     # clean up the tmp dir
#'     purrr::walk(tmp_df, ~{
#'       fs::dir_delete(attr(.x, "path", exact=TRUE))
#'     })
#'     
#'     res1 = res %>% chunk_group_by(!!!syms(by))
#'     
#'     res1
#'   })
#' }

# Alternative to hashstr2i that can produce semi-sorted chunks
# Apply as e.g.:
# split_values <- map(dff, sample_n, size=1) %>% 
#   select(c("id1", "id2")) %>%
#   collect() %>%
#   arrange(id1, id2)
# 
# shard_by_rule <- splitstr2i(split_values)
# code = glue::glue("df[,.out.disk.frame.id := {shard_by_rule}]")

sortablestr2i <- function(split_values, desc_vars){
  escape <- function(x) {
    if(is.character(x)){
      paste0("\"", x, "\"")
    } else {
      x
    }
  }
  
  switchcond <- function(name){ 
      paste0("(",
             name, 
             ifelse(name %in% desc_vars, " < ", " > "),
             escape(split_values[,name]), 
             " | (",
             name, 
             " == ",
             escape(split_values[,name])
             )
  }
  
  do.call(
    paste,
    c(
      lapply(
        as.list(do.call(paste, c(lapply(colnames(split_values), switchcond), sep=" & "))),
        function(x) { paste0("(", x, paste0(rep(")", ncol(split_values) * 2 + 1), collapse = ""), "* 1")}
      ),
    sep = " + "
    )
  )
}
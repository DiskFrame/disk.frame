# Alternative to hashstr2i that can produce semi-sorted chunks
# Apply as e.g.:
# split_values <- cmap(dff, sample_n, size=1) %>% 
#   select(c("id1", "id2")) %>%
#   collect() %>%
#   arrange(id1, id2)
# 
# shard_by_rule <- splitstr2i(split_values)
# code = glue::glue("df[,.out.disk.frame.id := {shard_by_rule}]")

# Check if date
is.date <- function(x) inherits(x, 'Date')

# Escapes names
# Factors are converted to numbers
escape_name <- function(name, x) {
  if(is.factor(x)){
    paste0("as.numeric(", name, ")")
  } else {
    name
  }
}

# Escapes values
# Strings and dates are quoted
# Factors are converted to number
escape_value <- function(x) {
  if(is.character(x) | is.date(x)){
    paste0("\"", x, "\"")
  } else if(is.factor(x)){
    as.numeric(x)
  } else {
    x
  }
}

# Switch condition - returns
# ({name} < {split_value} | ({name} == {split_value} ...

switchcond <- function(name, split_values, desc_vars){ 
  paste0("(",
         escape_name(name, split_values[,name]), 
         ifelse(name %in% desc_vars, " < ", " > "),
         escape_value(split_values[,name]), 
         " | (",
         name, 
         " == ",
         escape_value(split_values[,name])
  )
}

# Composes the switch conditions, so each split row becomes
# ({name1} < {split_value1} | ({name1} == {split_value1} & 
#    ({name2} < {split_value2} | ({name2} == {split_value2} ...)))) * 1
# the sum of the split row is the id
sortablestr2i <- function(split_values, desc_vars){
  # ZJ: need to force it to be a data.frame, because if it's a data.table then it will
  # cause issues
  split_values = data.frame(split_values)
  do.call(
    paste,
    c(
      lapply(
        as.list(do.call(paste, c(lapply(colnames(split_values), switchcond, split_values, desc_vars), sep=" & "))),
        function(x) { paste0("(", x, paste0(rep(")", ncol(split_values) * 2 + 1), collapse = ""), "* 1")}
      ),
    sep = " + "
    )
  )
}
#' Helper function to evalparse some `glue::glue` string
#' @param code the code in character(string) format to evaluate
#' @param env the environment in which to evaluate the code
#' @export
evalparseglue <- function(code, env = parent.frame()) {
  eval(parse(text = glue::glue(code, .envir = env)), envir = env)
}

#' Generate synthetic dataset for testing
#' @param N number of rows. Defaults to 200 million
#' @param K controls the number of unique values for id. Some ids will have K distinct values while others have N/K distinct values
#' @importFrom stats runif
#' @export
gen_datatable_synthetic <- function(N=2e8, K=100) {
  data.table(
    id1 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
    id2 = sample(sprintf("id%03d",1:K), N, TRUE),      # large groups (char)
    id3 = sample(sprintf("id%010d",1:(N/K)), N, TRUE), # small groups (char)
    id4 = sample(K, N, TRUE),                          # large groups (int)
    id5 = sample(K, N, TRUE),                          # large groups (int)
    id6 = sample(N/K, N, TRUE),                        # small groups (int)
    v1 =  sample(5, N, TRUE),                          # int in range [1,5]
    v2 =  sample(5, N, TRUE),                          # int in range [1,5]
    v3 =  sample(round(runif(100,max=100),4), N, TRUE), # numeric e.g. 23.5749
    date1 = sample(seq(as.Date('1970-01-01'), as.Date('2019-01-01'), by = "day"), N, TRUE)  # date
  )
}

#' Used to convert a function to purrr syntax if needed
#' @param .f a normal function or purrr syntax function i.e. `~{ ...code...}`
#' @importFrom purrr as_mapper 
purrr_as_mapper <- function(.f) {
  if(typeof(.f) == "language") {
    if(requireNamespace("purrr")) {
      .f = purrr::as_mapper(.f)
    } else {
      code = paste0(deparse(substitute(.f)), collapse = "")
      stop(
        sprintf(
          "in cmap(.x, %s), it appears you are using {purrr} syntax but do not have {purrr} installed. Try `install.packages('purrr')`",
          code
        )
      )
    }
  }
  return(.f)
}

#' Find globals in an expression by searching through the chain
find_globals_recursively <- function(code, envir) {
  globals_and_pkgs = future::getGlobalsAndPackages(code, envir)
  
  global_vars = globals_and_pkgs$globals
  
  env = parent.env(envir)
  
  done = identical(env, emptyenv()) || identical(env, globalenv())
  
  # keep adding global variables by moving up the environment chain
  while(!done) {
    tmp_globals_and_pkgs = future::getGlobalsAndPackages(code, envir = env)
    new_global_vars = tmp_globals_and_pkgs$globals
    for (name in setdiff(names(new_global_vars), names(global_vars))) {
      global_vars[[name]] <- new_global_vars[[name]]
    }
    
    done = identical(env, emptyenv()) || identical(env, globalenv())
    env = parent.env(env)
  }
  
  globals_and_pkgs$globals = global_vars
  
  return(globals_and_pkgs)
}
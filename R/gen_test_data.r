#' Generate synthetic dataset for testing
#' @examples
#' #system.time(DT <- gen_datatable_synthetic())
#' #system.time(fst::write.fst(DT,file.path(data_path, "DT.fst")))
#' 
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
    v3 =  sample(round(runif(100,max=100),4), N, TRUE) # numeric e.g. 23.5749
  )
}

dirty_work <- function() {
  library(magrittr)
}
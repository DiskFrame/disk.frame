source("inst/fannie_mae/0_setup.r")

check_res = future_lapply(dir("test_fm", full.names = T), function(x) {
  tryCatch({
    purrr::map(dir(x,full.names = T), ~{
      fst::read_fst(.x);
      NULL
    })
    return("ok")
  }, error = function(e) {
    return("error")
  })
})

table(unlist(check_res))

# check again for errors
#source("inst/fannie_mae/1b_validate_csv.r")
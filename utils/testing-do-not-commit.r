if(F) {
  library(disk.frame)
  library(dplyr)
  
  setup_disk.frame()
  a = disk.frame("c:/data/fannie_mae_disk_frame/fm.df/")
  
  pt=proc.time()
  a %>% map(~{
    .x
    NULL
  }
  , lazy = F
  )
  data.table::timetaken(pt)
  
  pt=proc.time()
  srckeep(a,c("monthly.rpt.prd", "loan.age"))[,mean(loan.age), monthly.rpt.prd]
  data.table::timetaken(pt)
  
  pt=proc.time()
  srckeep(b,c("monthly.rpt.prd", "loan.age"))[,mean(loan.age), monthly.rpt.prd]
  data.table::timetaken(pt)
  
  b = disk.frame("d:/fm.df")
  pt=proc.time()
  b %>% map(~{
    .x
    NULL
  }
  , lazy = F
  )
  data.table::timetaken(pt)
}

source("inst/fannie_mae/0_setup.r")

dir.create("fmdf")

system.time(
  rbindlist.disk.frame(
    map(
      dir("test_fm",full.names = T),
      ~disk.frame(.x)
      ),
    "fmdf"
  )
)

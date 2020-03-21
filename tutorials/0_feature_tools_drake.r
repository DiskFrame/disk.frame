library(disk.frame)
setup_disk.frame()

library(drake)

plan = drake::drake_plan(
  df = target(
    disk.frame::csv_to_disk.frame(
      file_in("D:/data/mortgage-risk/feature_matrix_cleaned.csv"),
      outdir = drake::drake_tempfile()
    ),
    format = "diskframe"
  )
)

system.time(make(plan))

loadd(df)

df

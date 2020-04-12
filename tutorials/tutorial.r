library(disk.frame)
setup_disk.frame()

a = disk.frame::disk.frame("c:/data/fannie_mae_disk_frame/fm.df/")


nrow(a)
ncol(a)

head(a)

system.time(asample <- a %>%
  srckeep("loan_id") %>% 
  filter(!duplicated(loan_id)) %>% 
  sample_frac(0.01) %>% 
  collect)

asample = data.frame(loan_id = asample)

system.time(
  a1 <-a %>% 
    cmap(~{
      inner_join(.x, asample, by = "loan_id")
    }, 
    lazy = FALSE, 
    outdir = "c:/data/fannie_mae_disk_frame/fm1pct.df/",
    overwrite = TRUE))


a2 = collect(a1)

data.table::fwrite(a2, "a2.csv")
arrow::write_parquet(a2, "a2.parquet")
fst::write_fst(a2, "a2.fst")

a2$date = as.Date(a2$monthly.rpt.prd, "%m/%d/%Y")
a2$delq.status = as.numeric(a2$delq.status)

a2[,.N,delq.status]
a2[is.na(delq.status),delq.status := 0]

head(a2)

setkey(a2, loan_id, date)

system.time(
  a3 <- 
    setDT(a2)[order(date), apply(.SD[,shift(delq.status, (1:24), type="lead")], 1 , function(x) max(x, na.rm=TRUE)), loan_id]
)

setnames(a3, names(a3), c("loan_id", "default_n24m"))
setDT(a3)[is.infinite(default_n24m), default_n24m := 0]
a3[,.N, default_n24m]

a2$default_n24m = a3$default_n24m >= 4

nrow(a2)

a2[,.N, default_n24m]

arrow::write_parquet(a2, "a2.parquet")
arrow::write_parquet(a2, "data.parquet")
fst::write_fst(a2, "a2.fst")
data.table::fwrite(a2, "a2.csv")

table(sapply(a2, mode))

a2 = a2 %>% 
  janitor::clean_names()

str(a2$date)
table(sapply(a2, class))

map2(a2, names(a2), ~{
  cx = class(.x)
  list(
    name = .y, 
    type = 
      ifelse(
        cx == "character", 
        "text", 
        ifelse(
          cx == "Date", 
          "datetime",
          ifelse(
            cx == "logical",
            "bool",
            ifelse(
              cx == "numeric",
              "numeric",
              "error"
            )
          )
          )),
        semantic_type = 
          ifelse(
            cx == "character", 
            "TextColumn", 
            ifelse(
              cx == "Date", 
              "DateTimeColumn",
              ifelse(
                cx == "logical",
                "BooleanColumn",
                ifelse(
                  cx == "numeric",
                  "NumericColumn",
                  "error"
                )
              )
            ))
    ) %>% jsonlite::toJSON(auto_unbox = TRUE)
}) %>% unlist %>% 
  paste0(collapse = ", ") -> outjson

res  = glue::glue('{"column": [|outjson|]}', .open = "|", .close = "|")

write(res, "schema.json")







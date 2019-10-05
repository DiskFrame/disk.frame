library(cran.stats)
# see https://github.com/arunsrinivasan/cran.stats
start = as.Date("2019-8-27")
end = as.Date("2019-08-27")

days = seq(start, end, by = "days")
files  = cran.stats:::construct_urls(days)
purrr::walk2(files, days, ~{
  download.file(.x, file.path("cran-mirror", paste0(.y, ".csv")))
  })


library(disk.frame)
setup_disk.frame()
system.time(a <- csv_to_disk.frame(list.files("cran-mirror", full.names = T, pattern="*.csv$")))

head(a)

aa = a[package == "disk.frame", ]

library(data.table)
aaa = copy(aa)
aaa[,date := as.Date(date,"%Y-%m-%d")]
a4 = aaa[,.N, date][order(date)]

plot(a4)
plot(a4[N<100])

library(ggplot2)
aaa[,.N, .(date, version)] %>% 
  ggplot + 
  geom_line(aes(x = date, y = N, colour = version))

a5 = aaa[order(date),.N, .(date)]
fwrite(a5, "a.csv")

library(forecast)

a6 = copy(a5)

a6[N>=100, N:=NA]

a6 %>% 
  right_join(a6[,.(date = seq(min(date), max(date), by = "days"))])

a6[, N_fixed:=ifelse(is.na(N), (lead(N,1) + lag(N,1))/2, N)]
a7 = a6[!is.na(N), ]


m  = forecast::auto.arima(log(a7$N))

plot(forecast::forecast(m))

glm(N_fixed~date, data=a7) %>% 
  summary()

b = aaa[,.N, ip_id][,N]
bc = kmeans(b, 4)$cluster

boxplot(b~bc, data.frame(b, bc))
b

d = aaa[,.N, ip_id]

system.time(d <- a[(date >= "2019-08-20") & (ip_id %in% d$ip_id), .N, package, keep=c("package", "ip_id", "date")])

d2 = d[order(N, decreasing = T),.(N = sum(N)), package]
View(d2)


system.time(whole_package <- a[(date >= "2019-08-20"),.N, package, keep=c("package","date")])

whole_package1 = whole_package[,.(N= sum(N)), package]

d2[,pct := N/sum(N)] 
whole_package1[,pct2:=N/sum(N)]

d3 = d2 %>% 
  full_join(whole_package1, by ="package") %>% 
  mutate(pct_ratio = log(pct/pct2)) %>% 
  mutate(abs_pct_ratio = abs(pct_ratio)) %>% 
  arrange(desc(abs_pct_ratio)) %>% 
  filter(N.x != N.y) %>% 
  filter(N.x > 360)

View(d3)


d3 %>% mutate(pct_diff = pct - pct2) %>% 
  arrange(pct_diff) %>% 
  head
  

#   geom_line(aes(x = date, y = N, colour = country))
# 
# 
# aaa[,.N, country][order(N, decreasing = T)]

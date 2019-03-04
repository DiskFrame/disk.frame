
two_var = acqall_dev[,.(cscore_b, ocltv, mi_pct, default_next_12m)]

all_vars = collect(acqall_dev)

non_char = 
  names(all_vars)[sapply(all_vars, typeof) != c("character")] %>% 
  setdiff("first_default_date")

# 
# all_vars = tibble(all_vars)
# library(dplyr)
# cor(
#   all_vars
#   , use = "pairwise.complete.obs")

a = sort(non_char)
alvarsn = all_vars[, ..a]

cdt = cor(alvarsn, use = "pairwise.complete.obs")

cdt2 = cdt[,which(colnames(cdt) == "default_next_12m")]

cdt3 = data.table(names = names(cdt2), cor = cdt2, abs_cor = abs(cdt2))

cdt3 = cdt3[order(abs_cor, decreasing = T)]

DT::datatable(cdt3)


corrplot::corrplot(cdt, method="color", diag=F)

# Binning

two_var1 = two_var[!is.na(ocltv),][order(ocltv),.(ocltv, default_next_12m)]


two_var2 = two_var1[, .(ndef = sum(default_next_12m), .N), ocltv]

two_var3 = two_var2[,.(ocltv, ndef, N, GB_odds = cumsum(N)/cumsum(ndef))]


wm = which(two_var3$GB_odds == max(Filter(is.finite, two_var3$GB_odds)))
wm3 = wm
two_var3[,.(ocltv, GB_odds)] %>% 
  plot(main = "Cumulative Good/Bad Odds", 
       xlab="Originative LTV (ocltv)",
       ylab="Observed Default Rate (ODR)",
       ylim=c(0,600))

abline(v = two_var3[wm, ocltv], lty=2)
legend("bottomright", c("ODR", "max ODR location")
       , lty=c(NA,2)
       , pch = c(1,NA)
       )

## plot
two_var4 = two_var3[-(1:wm),.(ocltv, N, ndef, GB_odds = cumsum(N)/cumsum(ndef))]

two_var3[-(1:wm),.(ocltv, GB_odds = cumsum(N)/cumsum(ndef))] %>% 
  plot(main = "Cumulative Observed Default Rate", xlab="Originative LTV (ocltv)",
       ylab="Observed Default Rate (ODR)", 
       xlim=c(0, 120), 
       ylim = c(0, 600))

lines(two_var3[, .(c(0,two_var3$ocltv[wm]), GB_odds[wm])])


wm = which(two_var4$GB_odds == max(Filter(is.finite, two_var4$GB_odds)))

abline(v = two_var4[wm, ocltv], lty=2)
legend("bottomright", c("ODR", "max ODR location")
       , lty=c(NA,2)
       , pch = c(1,NA)
)

## plot 2
two_var5 = two_var4[-(1:wm),.(ocltv, N, ndef, GB_odds = cumsum(N)/cumsum(ndef))]

two_var4[-(1:wm),.(ocltv, GB_odds = cumsum(N)/cumsum(ndef))] %>% 
  plot(main = "Cumulative Observed Default Rate", xlab="Originative LTV (ocltv)",
       ylab="Observed Default Rate (ODR)", 
       xlim=c(0, 120), 
       ylim = c(0, 600))

lines(two_var3[, .(c(0,two_var3$ocltv[wm3]), GB_odds[wm3])])

lines(two_var4[, .(c(two_var3$ocltv[wm3],two_var4$ocltv[wm]), GB_odds[wm])])

wm = which(two_var4$GB_odds == max(Filter(is.finite, two_var4$GB_odds)))

legend("bottomright", c("ODR", "max ODR location")
       , lty=c(NA,2)
       , pch = c(1,NA)
)

abline(v=51, lty=2)

var1 = res_all[[1]]

all_vars[, cscore_bin := cut(
  all_vars[,var1$feature, with = F][[1]] ,
  var1$bin[[2]] %>% setdiff(c(701,713))) %>% addNA]

bp1 = all_vars[order(cscore_bin),.(sum(default_next_12m)/.N), cscore_bin]
barplot(bp1$V1)

var2 = res_all[[3]]
all_vars[, ocltv_bin := cut(
  all_vars[,var2$feature, with = F][[1]],
  var2$bins[[2]]) %>% addNA]

bp2 = all_vars[order(ocltv_bin),.(sum(default_next_12m)/.N), ocltv_bin]
barplot(bp2$V1)


var2 = res_all[[9]]
var2$feature
all_vars[, dti_bin := cut(
  all_vars[,var2$feature, with = F][[1]],
  var2$bins[[2]]) %>% addNA]

bp2 = all_vars[order(dti_bin),.(sum(default_next_12m)/.N), dti_bin]
barplot(bp2$V1)

m = glm(default_next_12m ~ cscore_bin + ocltv_bin + dti_bin, data=all_vars)

DT::datatable(broom::tidy(m), options=list(pageLength=16))

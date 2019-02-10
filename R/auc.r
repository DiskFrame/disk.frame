#' compute Area under the Curve (AUC)
#' @param target the target
#' @param score the score, the higher the score the less targets
#' @importFrom data.table setkey shift data.table
auc <- function(target, score) {
  a = data.table::data.table(target, score = -score)
  data.table::setkey(a, score)
  a1 = a[,.(h = sum(target), w = .N), score]
  
  
  a1[,height:=cumsum(h)/sum(h)]
  a1[,ctot:=cumsum(w)/sum(w)]
  
  a1[,lag_height:=shift(height,1)]
  a1[1,lag_height := 0]
  
  a1[,area := (lag_height + height)*w/sum(w)/2]
  a1[,sum(area)]
}
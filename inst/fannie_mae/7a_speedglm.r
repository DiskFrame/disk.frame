source("inst/fannie_mae/0_setup.r")
library(disk.frame)

acqall_dev = disk.frame(file.path(outpath, "appl_mdl_data_sampled_dev"))

library(speedglm)

head(acqall_dev)

#' A streaming function for speedglm
#' @param df a disk.frame
stream_shglm <- function(df) {
  i = 0
  is = sample(nchunks(df), replace = F)
  function(reset = F) {
    #browser()
    if(reset) {
      print("you've reset")
      i <<- 0
    } else {
      i <<- i + 1
      #if (i > 4) {
      if (i > nchunks(df)) {
        return(NULL)
      }
      print(glue::glue("{i}/{nchunks(df)}"))
      return(get_chunk(df, is[i]))
      #return(get_chunk(df, i))
    }
  }
}

acqall_dev1 = acqall_dev %>% delayed(~{
  .x[,oltv_band := cut(oltv, c(-Inf, seq(0,100,by=10)))]
  .x[,dti_band := addNA(cut(dti, c(-Inf, seq(0,100,by=10), Inf)))]
  .x
})

build_model <- function() {
  model <- keras_model_sequential() %>%
    layer_dense(units = 2, activation = 'softmax')
  
  
  model %>% compile(
    loss = "categorical_crossentropy",
    optimizer = 'sgd'
  )
  
  model
}


for(i in 1:nchunks(acqall_dev1)) {
  a = get_chunk(acqall_dev1, i)
  a[,.(sum(default_next_12m), .N),oltv_band]
  
  a1 = 
    cbind(
      a[,keras::to_categorical(oltv_band %>% as.integer)]
      a[,keras::to_categorical(dti_band %>% as.integer)]
    )
    
  summary(a1)
  at = a[!is.na(dti),default_next_12m*1]
  Y_train = keras::to_categorical(at)
  
  model = build_model()
  
  hist = model %>% fit(
    a1,
    Y_train,
    epochs = 1,
    validation_split = 0.2,
    verbose = 0
  )
  
  plot(ok)
  
  get_weights(model)
}

model %>% predict(a1)


boston_housing <- dataset_boston_housing()

c(train_data, train_labels) %<-% boston_housing$train
c(test_data, test_labels) %<-% boston_housing$test

acqall_dev1 %>% 
  srckeep(c("oltv", "dti", "default_next_12m")) %>% 
  group_by(oltv_band, hard = F) %>% 
  summarise(n = n(), ndef = sum(default_next_12m)) %>% 
  collect %>% 
  group_by(oltv_band) %>% 
  summarise(n = sum(n), ndef = sum(ndef))

streamacq = stream_shglm(acqall_dev1)
library(speedglm)
library(biglm)

shglm(default_next_12m ~ oltv_band-1, 
      datafun = streamacq, family=binomial())

bigglm(default_next_12m ~ oltv_band-1, data = streamacq, family=binomial())

make.data<-function(filename, chunksize,...){
  conn<-NULL
  function(reset=FALSE){
    if(reset){
      if(!is.null(conn)) 
        close(conn)
      conn<<-file(filename,open="r")} else{
        rval<-read.table(conn, nrows=chunksize,...)
        if ((nrow(rval)==0)) {
          close(conn)
          conn<<-NULL
          rval<-NULL}
        return(rval)}}}
# data1 is a small toy dataset
data(data1)
write.table(data1,"data1.txt",row.names=FALSE,col.names=FALSE)
rm(data1)
da<-make.data("data1.txt",chunksize=50,col.names=c("y","fat1","x1","x2"))
# Caution! make sure to close the connection once you have run command #1
da(reset=T) #1: opens the connection to "data1.txt"
da(reset=F) #2: reads the first 50 rows (out of 100) of the dataset
da(reset=F) #3: reads the second 50 rows (out of 100) of the dataset
da(reset=F) #4: is NULL: this latter command closes the connectionrequire(biglm)# fat1 is a factor with four levels

b1<-shglm(y~factor(fat1)+x1,weights=~I(x2^2),datafun=da,family=Gamma(log))
b2<-bigglm(y~factor(fat1)+x1,weights=~I(x2^2),data=da,family=Gamma(log))
summary(b1)
summary(b2)


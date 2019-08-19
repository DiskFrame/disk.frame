source("inst/fannie_mae_10pct/00_setup.r")
library(disk.frame)
library(keras)

acqall_dev = disk.frame(file.path(outpath, "appl_mdl_data_sampled"))

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
  .x[,oltv_band := cut(oltv, c(-Inf, 60,80, Inf))]
  #.x[,scr_band := addNA(cut(cscore_b, c(-Inf, 627,700, Inf)), ifany=F)]
  .x[,scr_band := addNA(cut(cscore_b, c(-Inf, 700,716,725,742,748,766,794, Inf)), ifany=F)]
  
  .x
})

if(F) {
  aa = acqall_dev1 %>% collect
  glm(default_next_12m ~ oltv_band + scr_band - 1, data=aa)
}

head(acqall_dev1)

# compile a kera model
build_model <- function() {
  model <- keras_model_sequential() %>%
    layer_dense(units = 2, activation = 'softmax')
  
  
  model %>% compile(
    loss = "categorical_crossentropy",
    optimizer = 'sgd'
  )
  
  model
}

# compiling the model ~ 
system.time(model <- build_model())

#ol = levels(acqall_dev1[,oltv_band, keep=c("oltv","cscore_b")])
#dl = levels(acqall_dev1[,scr_band, keep=c("oltv","cscore_b")])

ol = levels(get_chunk(acqall_dev1,1)[,oltv_band])
dl = levels(get_chunk(acqall_dev1,1)[,scr_band])


#for(i in 1:nchunks(acqall_dev1)) {
kk <- function() {
  #browser()
  j = 0
  done = F
  ii = 0
  while(!done) {
    j <- j + 1
    si = sample(nchunks(acqall_dev1), nchunks(acqall_dev1)*0.3)
    osi = setdiff(1:nchunks(acqall_dev1), si)
    #browser()
    system.time(a <- map_dfr(si, ~{
      #browser()
      ii <- ii + 1
      i = .x
      if(ii %% 20 == 0) print(glue::glue("{j}:{ii} {Sys.time()}"))
      
      a = get_chunk(acqall_dev1, i)
      #a[,.(sum(default_next_12m), .N),oltv_band]
      
      a1 = 
        cbind(
          a[,keras::to_categorical(as.integer(oltv_band) - 1)]
          #,a[,keras::to_categorical(as.integer(scr_band) - 1)]
        )
      
      at = a[,default_next_12m*1]
      Y_train = keras::to_categorical(at)
      
      hist = model %>% fit(
        a1,
        Y_train,
        epochs = 1,
        validation_split = 0.2,
        verbose = 0
      )
      
      gw = get_weights(model)
      
      gwdt = as.data.table(gw[1])
      setnames(gwdt, names(gwdt), c("non_default", "default"))
      gwdt$i =i
      gwdt$var = c(
        rep("oltv_band", length(ol))
        #, rep("scr_band", length(dl))
      )
      gwdt$band = c(ol,dl
      )
      gwdt
    }))
    
    some_chunks <- map(osi, ~{
      i = .x
      a = get_chunk(acqall_dev1, i)
      
      a1 = 
        cbind(
          a[,keras::to_categorical(as.integer(oltv_band) -1)]
          #,a[,keras::to_categorical(as.integer(scr_band)-1)]
        )
      a1
    }) %>% reduce(rbind)
    
    outcomes <- map(osi, ~{
      i = .x
      a = get_chunk(acqall_dev, i, keep=c("default_next_12m"))
      a[,default_next_12m]
    }) %>% unlist
    
    auc = auc(outcomes, predict(model, some_chunks)[,2])
    
    # scores
    p = predict(model, diag(
      length(ol)
      #+length(dl)
      ))[,2]
    a_b = log(p/(1-p))
    
    scalar = 20/log(2)
    intercept = 20*log(536870912/15)/log(2)
    
    a = mean(a_b)
    b = a_b - a
    done <- length(unique(sign(diff(b)))) == 1
    
    scrs = data.table(base_scr = round(a*scalar+intercept,0), scr = round(-b*scalar,0))
    scrs$var = c(
      rep("oltv_band", length(ol))
      #, rep("scr_band", length(dl))
    )
    scrs$band = c(ol
                  , dl
    )
    scrs = scrs[band != "bias"]
    
    scrs = scrs[,.(var, band, scr, base_scr)]
    print(glue::glue("AUC: {auc}"))
    print(scrs)
  }  
  scrs
}

pt = proc.time()
scrs = kk()
timetaken(pt)
View(scrs)

#AUC: 0.599790504924901
# var      band scr base_scr
# 1: oltv_band (-Inf,60]  10      351
# 2: oltv_band   (60,80]  10      351
# 3: oltv_band (80, Inf] -21      351
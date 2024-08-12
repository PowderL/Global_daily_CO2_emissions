## 参数优化
library(xgboost)
library(data.table)
library(lubridate)
library(readxl)
library(SHAPforxgboost)
library(doParallel)
cal_RMSE <- function(obs, pre) {
  # n = length(obs)
  sel <- complete.cases(cbind(obs, pre))
  obs <- obs[sel]
  pre <- pre[sel]
  n = length(obs)
  sqrt(sum((pre - obs)^2) / n)
}

cal_R2 <- function(obs, pre) {
  sel <- complete.cases(cbind(obs, pre))
  obs <- obs[sel]
  pre <- pre[sel]
  (cor(obs, pre))^2
}

train_model <- function(train.data, save.dir=".", if.save.model=FALSE, if.var.imp=FALSE, 
                        pred.y.names, x.names,param, model.tag="/final_pred_") {
  
  train.data <- as.data.frame(train.data)
  pred.y.len <- length(pred.y.names)
  
  model.x.names <- x.names
  model.y.name <- pred.y.names
  
  obsX <- train.data[, model.x.names]
  
  obsY <- train.data[, model.y.name]
  wY <- train.data[, "w.all"]
  dtrain <- xgb.DMatrix(as.matrix(obsX), label=obsY, weight=wY)
  model.xgb <- xgb.train(param, dtrain,reset_data = TRUE, nrounds = 500)
  charset <- c(letters, LETTERS, c(0:9))
  random_string <- paste0(sample(charset, 10, replace = TRUE), collapse = "")
  if(if.save.model) {
    xgb.save(model.xgb, paste0(save.dir, model.tag, random_string, "_", model.y.name, ".xgb"))
  }
  if(if.var.imp) {
    var.imp <- xgb.importance(model=model.xgb)
    write.csv(var.imp, file=paste0(save.dir, model.tag, random_string, "_", model.y.name, "_var_imp.csv"))
  }
  
  return(model.xgb) # model.list may stay in the GPU memory, which consumes a lot memory, may only use GPU for model training
}
cross_validation_sample <- function(model.dir, combo.data,
                                    x.names, y.names, param, 
                                    validation.fraction){
  
  print(paste0(Sys.time(), " Execuating validation!"))
  begin_time <- Sys.time()
  validation.set.size <- ceiling(validation.fraction*nrow(combo.data))
  ## use seed 
  set.seed(1024)
  data.set.index <- sample(1:nrow(combo.data), nrow(combo.data), replace = F)
  
  obs.pre.df.list <- foreach(fold_index = 1:10)%do%{
    validation.part <- (validation.set.size*(fold_index-1)+1):(min(validation.set.size*fold_index, nrow(combo.data)))
    validation.set.index <- data.set.index[validation.part]
    
    train.set.index <- data.set.index[-validation.part]
    
    validation.data <- combo.data[validation.set.index, ]
    train.data <- combo.data[train.set.index, ]
    
    model <- train_model(train.data=train.data, save.dir=model.dir, if.save.model=FALSE, 
                         if.var.imp=FALSE, pred.y.names=y.names, x.names=x.names, param=param,
                         model.tag=paste0("validation_", validation.fraction, "_"))
    
    pred.values <- predict(model, as.matrix(validation.data[, ..x.names]))
    
    obs.pre.df <- data.frame(validation.data$scale_factor, pred.values)
    
    names(obs.pre.df) <- c("x", "y")
    rm(model)
    gc()
    obs.pre.df$mdate <- validation.data$mdate
    return (obs.pre.df)
  }
  obs.pre.df.all <- as.data.table(rbindlist(obs.pre.df.list, use.names = T))
  print(paste0("Comsuming: ", Sys.time() - begin_time))
  return (obs.pre.df.all)
}

## 分国家
## 计算月均排放
for (sector_i in c("Power", "Industry", "Residential", "Transportation")) {
  
  dataset_all <- fread(paste0("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/train_and_predict_dataset_", sector_i, "_0806.csv"))
  dataset_all[, c("mmday", "mwday", "mmonth") := list(mday(mdate), wday(mdate), month(mdate))]
  train_dataset <- dataset_all[myear >= 2019, ]
  
  x.names <- c("tem", "mmonth", "mwday", "mmday", "is_holiday")
  y.names <- c("scale_factor")
  model.dir <- "/data/ltao/Qiu_paper/model"
  train_dataset$w.all <- 1
  
  country_list <- c('United States', 'China', 'India', 'Japan', 'Brazil', 
                  'Germany', 'Italy', 'United Kingdom', "France",
                  'Spain', 'Russian Federation' 'ROW', "EU")
  
  gpu_id <- 0
  for (coun in country_list) {
    train.data <- train_dataset[country == coun, ]
    validation_eta_list <- foreach (eta_i = c(0.01, 0.025, 0.05, 0.1, 0.2)) %do% {#
      validation_max_depth_list <- foreach (max_depth = c(2, 3, 4, 5)) %do% {
        validation_colsample_list <- foreach(col_s = c( 0.3, 0.33333, 0.5, 0.7, 0.9)) %do% {
          validation_subsample_list <- foreach (sub_s = c(0.3, 0.5, 0.8)) %do% {
            param <- list(
              colsample_bytree = col_s,
              eta = eta_i,
              eval_metric = "rmse",
              gpu_id = gpu_id,
              max_depth = max_depth,
              n_gpus = 1,
              nthread = 10,
              objective = "reg:linear",
              subsample = sub_s,
              tree_method = "gpu_hist"
            )
            validation_result <- cross_validation_sample(model.dir, train.data,
                                                         x.names, y.names, param, 
                                                         validation.fraction = 0.1)
            
            validation_result$CM_avg <- train.data$CM_avg
            validation_result[, c("x", "y") := list(x * CM_avg, y * CM_avg)]
            r2 <- cal_R2(validation_result$x, validation_result$y)
            rmse <- cal_RMSE(validation_result$x, validation_result$y)
            
            return (data.table(r2 = r2, rmse = rmse, subsample = sub_s))
          }
          validation_subsample_all <- rbindlist(validation_subsample_list, use.names = T)
          validation_subsample_all$colsample <- col_s
          return (validation_subsample_all)
        }
        validation_colsample_all <- rbindlist(validation_colsample_list, use.names = T)
        validation_colsample_all$max_depth <- max_depth
        return (validation_colsample_all)
      }
      validation_max_depth_all <- rbindlist(validation_max_depth_list, use.names = T)
      validation_max_depth_all$eta <- eta_i
      return (validation_max_depth_all)
    }
    validation_eta_all <- rbindlist(validation_eta_list, use.names = T)
    fwrite(validation_eta_all, paste0("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/parm_opt_0806/", tolower(sector_i), "_validation_for_", coun, ".gz"))
  }
  
}

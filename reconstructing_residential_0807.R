##
library(xgboost)
library(data.table)
library(lubridate)
library(readxl)
library(SHAPforxgboost)
library(doParallel)
library(ggpubr)
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
  model.xgb <- xgb.train(param, dtrain,reset_data = TRUE, nrounds = 2000)
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

train_and_predict_dataset <- fread("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/train_and_predict_dataset_Residential_0806.csv")

train_dataset <- train_and_predict_dataset[mdate >= as.Date("2019-01-01"), c("CM", "tem", "country", "mdate", 
                                                                             "extreme_temp", "is_holiday", "scale_factor", "co2_emission_month_avg")]
train_dataset[, c("mmday", "mwday", "mmonth", "myear") := list(mday(mdate), wday(mdate), month(mdate), myear = year(mdate))]

predict_dataset <- train_and_predict_dataset[, c("tem", "country", "mdate", "co2_emission_month_avg", 
                                                 "extreme_temp", "is_holiday")]
predict_dataset[, c("mmday", "mwday", "mmonth") := list(mday(mdate), wday(mdate), month(mdate))]

x.names <- c("tem", "mmonth", "mwday", "mmday", "is_holiday")
y.names <- c("scale_factor")

model.dir <- "/data/ltao/Qiu_paper/model"
train_dataset$w.all <- 1
predict_dataset$co2_emission_pred <- 0.0

param_table <- fread("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_residential.csv")
country_list <- c('United States', 'China', 'India', 'Japan', 'Brazil', 
                  'Germany', 'Italy', 'United Kingdom', "France",
                  'Spain', 'Russian Federation' 'ROW', "EU")

out.data.list <- foreach (country_i = country_list) %do% {
  train.data <- train_dataset[country == country_i, ]
  pred.data <- predict_dataset[country == country_i, ]
  
  param <- list(colsample_bytree = param_table[country == country_i, ]$colsample, 
                eta =param_table[country == country_i, ]$eta, 
                eval_metric = "rmse", 
                gpu_id = 2, 
                max_depth = param_table[country == country_i, ]$max_depth, 
                nthread = 8, 
                objective = "reg:linear", 
                subsample = param_table[country == country_i, ]$subsample, 
                tree_method = "gpu_hist")
  
  
  model <- train_model(train.data=train.data, save.dir=model.dir, if.save.model=TRUE, 
                       if.var.imp=TRUE, pred.y.names=y.names, x.names=x.names, param=param)
  
  pre.values <- predict(model, as.matrix(pred.data[, ..x.names]))
  
  pred.data$co2_emission_pred <- pred.data$co2_emission_month_avg * pre.values
  
  ## 模型验证
  ## 计算5%分位数回归
  quantile <- 0.16
  quantile_loss1 <- function(preds, dtrain) {
    labels <- getinfo(dtrain, "label")
    err <- ifelse(labels > preds, quantile * (labels - preds)^2, (1 - quantile) * (labels - preds)^2)
    
    grad <- ifelse(labels > preds, -quantile * (labels - preds), -(1 - quantile) * (labels - preds))
    
    hess <- ifelse(labels > preds, quantile, (1 - quantile))
    
    return(list(grad = grad, hess = hess))
  }
  
  param <- list(
    objective = quantile_loss1, 
    colsample_bytree = param_table[country == country_i, ]$colsample, 
    eta =param_table[country == country_i, ]$eta, 
    eval_metric = "rmse", 
    gpu_id = 2, 
    max_depth = param_table[country == country_i, ]$max_depth, 
    nthread = 8, 
    subsample = param_table[country == country_i, ]$subsample, 
    tree_method = "gpu_hist")
  
  model <- train_model(train.data=train.data, save.dir=model.dir, if.save.model=TRUE, 
                       if.var.imp=TRUE, pred.y.names=y.names, x.names=x.names, param=param)
  
  pre.values <- predict(model, as.matrix(pred.data[, ..x.names]))
  
  pred.data$co2_emission_pred_5p <- pred.data$co2_emission_month_avg * pre.values
  
  
  ## 计算95%分位数回归
  quantile <- 0.84
  quantile_loss2 <- function(preds, dtrain) {
    labels <- getinfo(dtrain, "label")
    err <- ifelse(labels > preds, quantile * (labels - preds)^2, (1 - quantile) * (labels - preds)^2)
    
    grad <- ifelse(labels > preds, -quantile * (labels - preds), -(1 - quantile) * (labels - preds))
    
    hess <- ifelse(labels > preds, quantile, (1 - quantile))
    
    return(list(grad = grad, hess = hess))
  }
  param <- list(
    objective = quantile_loss2, 
    colsample_bytree = param_table[country == country_i, ]$colsample, 
    eta =param_table[country == country_i, ]$eta, 
    eval_metric = "rmse", 
    gpu_id = 2, 
    max_depth = param_table[country == country_i, ]$max_depth, 
    nthread = 8, 
    subsample = param_table[country == country_i, ]$subsample, 
    tree_method = "gpu_hist")
  
  model <- train_model(train.data=train.data, save.dir=model.dir, if.save.model=TRUE, 
                       if.var.imp=TRUE, pred.y.names=y.names, x.names=x.names, param=param)
  
  pre.values <- predict(model, as.matrix(pred.data[, ..x.names]))
  
  pred.data$co2_emission_pred_95p <- pred.data$co2_emission_month_avg * pre.values
  
  ## 然后计算一下SHAP
  shap_long <- shap.prep(xgb_model = model, X_train = as.matrix(pred.data[, ..x.names]))
  fwrite(shap_long, paste0("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/model_shap_residential/", country_i, ".csv"))
  ## 重构的排放
  train.data[, CM_new := CM]
  pred.data[, CM_new := co2_emission_pred]
  pred.data[, CM_new_5p := co2_emission_pred_5p]
  pred.data[, CM_new_95p := co2_emission_pred_95p]
  
  pred.data$scale_factor <- pre.values
  out.data <- rbindlist(list(train.data[, c("mdate", "country", "tem", "CM_new", "extreme_temp", "scale_factor", "co2_emission_month_avg")], 
                             pred.data[mdate < as.Date("2019-01-01"), c("mdate", "country", "tem", "CM_new", "extreme_temp", "scale_factor", "co2_emission_month_avg", "CM_new_5p", "CM_new_95p")]), 
                        fill = TRUE)
  
  return (out.data)
  
}
out.data.all <- rbindlist(out.data.list, use.names = T)
global_data_daily <- out.data.all[, .(tem = mean(tem), 
                                      CM_new = sum(CM_new), 
                                      extreme_temp = mean(extreme_temp), 
                                      CM_new_5p = sum(CM_new_5p), 
                                      CM_new_95p = sum(CM_new_95p),
                                      scale_factor = sum(scale_factor * co2_emission_month_avg)/sum(co2_emission_month_avg)), by = .(mdate)]
global_data_daily[, country := "Global"]
out.data.all <- rbindlist(list(out.data.all, global_data_daily), use.names = T, fill = T)
fwrite(out.data.all, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/merged_all_new_residential_0807.csv")

###############################################################################
## ##
###############################################################################
library(ggplot2)
country_list <- c('China', "Japan", "Russian Federation", "Italy", "Germany", "United States")
country_name_list <- c('China', "Japan", "Russia", "Italy", "Germany", "United States")
title_list <- c("(a) ", "(b) ", "(c) ", "(d) ", "(e) ", "(f) ", "(g) ", "(h) ")
title_list <- paste0(title_list, country_name_list)

p.list <- foreach(i = 1:length(country_list)) %do% {
  SHAP_value <- fread(paste0("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/model_shap_residential/", country_list[i], ".csv"))
  shap_avg <- SHAP_value[, .(value = mean(abs(value))), by = .(variable)]
  shap_sum <- shap_avg[, .(value = sum(value))]
  shap_avg[, per := value / shap_sum$value * 100 ]
  print(shap_avg)
  used_vars <- shap_avg$variable
  used_vars <- rev(used_vars)
  
  vars_names <- c("Temperature", "Month", "Day of week", "Day of month", "Is a public holiday")
  
  color_vars <- c("tem", "mmonth", "mwday", "mmday", "is_holiday")
  
  var_dt <- data.table(vars_names, used_vars = color_vars, colors = c("#bb6d41", "#65b3be", "#f1c865", "#585858", "#04551a"))
  used_vars_dt <- data.table(used_vars)
  used_vars_dt[var_dt, c("vars_names", "colors") := list(i.vars_names, i.colors), on = .(used_vars)]
  
  p <- ggplot(data = shap_avg, aes(x = variable, y = per, fill = variable))+
    geom_col(width = 0.5)+
    theme_bw()+
    labs(x = "", y = "Scaled mean absolute SHAP value (%)", title = title_list[i])+
    scale_x_discrete(breaks = used_vars,
                     labels = used_vars_dt$vars_names,
                     limits = used_vars)+
    
    scale_fill_manual(breaks = used_vars_dt$used_vars,
                      values = used_vars_dt$colors)+
    coord_flip()+
    theme(legend.position = "none",
          plot.margin = unit(c(0, 0.2, 0, -0.3), "cm"),
          axis.title.x = element_text(hjust = 1),
          panel.grid = element_blank())
  return (p)
}
p.all <- ggarrange(p.list[[1]], p.list[[2]], p.list[[3]], p.list[[4]], p.list[[5]], p.list[[6]], nrow = 3, ncol = 2)

tiff(paste0("/data/ltao/Qiu_paper/plots/temperature_weekly_contribution_residential.tif"), width = 6000, height = 6000, res = 1000, compression = "lzw")
print(p.all)
dev.off()


#
library(data.table)
parm_opt_dir <- "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/parm_opt_0806"
file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "residential_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 28, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[1, ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
mean(params_table$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_residential.csv")

file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "power_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 22, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[1, ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
mean(params_table$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_power.csv")

file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "industry_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 25, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[1, ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
mean(params_table$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_industry.csv")

file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "transportation_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 31, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[1, ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
mean(params_table$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_transportation.csv")



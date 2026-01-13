#
library(data.table)
parm_opt_dir <- "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/parm_opt_1229_date_block_add_variables_10trees"

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
params_table[country %in% c("United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]
mean(params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation", "United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_power_1229_date_block_add_variables_10trees.csv")

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
mean(params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation", "United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_industry_1229_date_block_add_variables_10trees.csv")

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
params_table[country %in% c("United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]
mean(params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation", "United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW"), ]$r2)
fwrite(params_table, "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/param_table_transportation_1229_date_block_add_variables_10trees.csv")


EU_countries <- c('Germany', 'Italy', 'United Kingdom', 'Austria', 'Portugal', 
                  'Hungary', 'Spain', 'Poland', 'Croatia', 'Latvia', 
                  'Netherlands', 'Slovenia', 'France', 'Cyprus', 
                  'Denmark', 'Finland', 'Malta', 'Lithuania', 
                  'Sweden', 'Belgium', 'Luxembourg', 
                  'Slovakia', 'Greece',  'Bulgaria', 'Estonia', 
                  'Romania', 'Czech Republic', 'Ireland')

params_table[!country %in% EU_countries, ]
mean(params_table[!country %in% EU_countries, ]$r2)



############################################################################################
############################################################################################
parm_opt_dir <- "/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/parm_opt_1229_date_block_add_variables_10trees"

file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "power_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 22, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[nrow(validation_result), ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
params_table[country %in% c("United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]
mean(params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation", "United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]$r2)

file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "industry_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 25, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[nrow(validation_result), ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
mean(params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation", "United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]$r2)

file.name.list <- list.files(parm_opt_dir, full.names = F, pattern = "transportation_")
params_list <- foreach(i = 1:length(file.name.list)) %do% {
  file.name <- file.name.list[i]
  country <- substring(file.name, 31, nchar(file.name) - 3)
  validation_result <- fread(paste0(parm_opt_dir, "/", file.name))
  setorder(validation_result, -r2)
  validation_result$country <- country
  return (validation_result[nrow(validation_result), ])
}
params_table <- rbindlist(params_list, use.names = T)
params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation"), ]
params_table[country %in% c("United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW")]
mean(params_table[country %in% c("China", "United States", "Germany", "Italy", "Japan", "Russian Federation", "United Kingdom", "India", "Brazil", "EU", "Spain", "France", "ROW"), ]$r2)

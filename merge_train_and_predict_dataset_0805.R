## 组装训练数据和预测数据
library(data.table)
CM_data <- fread("/data/ltao/Qiu_paper/CM_website.csv")
CM_data[, mdate := as.Date(date, format = "%d/%m/%Y")]
CM_data[, c("myear", "mmonth") := list(year(mdate), month(mdate))]

CM_data[sector == "Ground Transport", sector := "Transportation"]
CM_data[sector == "Domestic Aviation", sector := "Transportation"]
CM_data[sector == "International Aviation", sector := "Transportation"]

CM_data_daily <- CM_data[(!country %in% c("WORLD", "EU27 & UK", "United Kingdom", "Spain", "France", "Germany", "Italy")) & myear <= 2023, .(CM = sum(value)), by = .(mdate, country, sector)]
CM_data_daily[country == "US", country := "United States"]
CM_data_daily[country == "Russia", country := "Russian Federation"]

CM_EU_data <- fread("/data/ltao/Qiu_paper/CM_EU_new_residential.csv")
CM_EU_data[, mdate := as.Date(date, format = "%d/%m/%Y")]
CM_EU_data[, c("myear", "mmonth") := list(year(mdate), month(mdate))]
CM_EU_data[sector == "Ground Transport", sector := "Transportation"]
CM_EU_data[sector == "Domestic Aviation", sector := "Transportation"]
CM_EU_data[sector == "International Aviation", sector := "Transportation"]

CM_EU_data_daily <- CM_EU_data[(!country %in% c("WORLD", "UK", "Norway", "Switzerland")) & myear <= 2023, .(CM = sum(value)), by = .(mdate, country, sector)]
CM_EU_data_daily[country == "EU27 & UK", country := "EU"]
CM_plus_EU_data_daily <- rbindlist(list(CM_data_daily, CM_EU_data_daily), use.names = T)

## 加载温度数据
temperature_add_czech <- fread("/data/ltao/ERA5/countries_population_weigted_mean_temperature_add_czech_new_pop.csv")
temperature_ROW <- fread("/data/ltao/ERA5/countries_population_weigted_mean_temperature_ROW_new_pop.csv")
temperature_EU <- fread("/data/ltao/ERA5/countries_population_weigted_mean_temperature_EU_new_pop.csv")
temperature_global <- rbindlist(list(temperature_add_czech, temperature_ROW, temperature_EU), use.names = T)
temperature_global[, mdate := mtime]
## 计算极端低温
extreme_low_temp <- temperature_global[, .(tem_5p = quantile(tem, 0.05)), by = .(country)]
extreme_high_temp <- temperature_global[, .(tem_95p = quantile(tem, 0.95)), by = .(country)]

temperature_global[extreme_low_temp, tem_5p := i.tem_5p, on = .(country)]
temperature_global[extreme_high_temp, tem_95p := i.tem_95p, on = .(country)]

temperature_global[, extreme_temp := 0]
temperature_global[tem <= tem_5p, extreme_temp := -1]
temperature_global[tem >= tem_95p, extreme_temp := 1]
## merged EDGAR data
edgar_emission_dataset <- read_excel("/data/ltao/Qiu_paper/IEA_EDGAR_CO2_m_1970_2022_scope_match.xlsx")

edgar_emission_dataset <- as.data.table(edgar_emission_dataset)
edgar_emission_dataset$IPCC_annex <- NULL
edgar_emission_dataset$C_group_IM24_sh <- NULL
edgar_emission_dataset$Country_code_A3 <- NULL
edgar_emission_dataset$Substance <- NULL
edgar_emission_dataset$ipcc_code_1996_for_standard_report_name <- NULL
edgar_emission_dataset$fossil_bio <- NULL

edgar_emission_dataset_dt <- melt(edgar_emission_dataset, id.vars = c("Name", "Year", "ipcc_code_1996_for_standard_report"), variable.name = "month_str", value.name = "co2_emission")

month_table <- data.table(month_str = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 
                          mmonth = c(1:12))

CM_codes <-  c("1A1a", "1A1bc", "1A2", "2A1", "1A3a", "1A3b_noRES", "1A3c", "1A3d", "1A3e", "1A4", "1A5", "1C2", "1C1")

sector_compare_table2 <- data.table(ipcc_code_1996_for_standard_report  = c("1A1a", "1A1bc", "1A2", "2A1", "1A3a", "1A3b_noRES", "1A3c", "1A3d", "1A3e", "1A4", "1A5", "1C2", "1C1"), 
                                     sector = c("Power", "Industry", "Industry", "Industry", "Transportation", "Transportation", "Transportation", "Transportation", "Transportation", "Residential", "Residential", "Transportation", "Transportation"))

edgar_emission_dataset_dt <- edgar_emission_dataset_dt[ipcc_code_1996_for_standard_report %in% CM_codes, .(co2_emission = sum(co2_emission)), by = .(Name, Year, month_str, ipcc_code_1996_for_standard_report)]
edgar_emission_dataset_dt[sector_compare_table2, sector := i.sector, on = .(ipcc_code_1996_for_standard_report)]

edgar_emission_dataset_dt[month_table, mmonth := i.mmonth, on = .(month_str)]
edgar_emission_dataset_dt[, co2_emission := co2_emission / 10^3]
edgar_emission_dataset_dt[, country := Name]
edgar_emission_dataset_dt[, myear := Year]
edgar_emission_dataset_dt[, myear := as.numeric(myear)]

edgar_emission_dataset_dt[, num_days_in_month := days_in_month(as.Date(paste0(myear, "-", mmonth, "-01")))]
edgar_emission_dataset_dt[, co2_emission_month_avg := co2_emission/num_days_in_month]
edgar_emission_dataset_dt[!Name %in% unique(temperature_global$country), country := "ROW"]

edgar_emission_dataset_dt_copy <- copy(edgar_emission_dataset_dt)
EU_countries <- c('Germany', 'Italy', 'United Kingdom', 'Austria', 'Portugal', 
                  'Hungary', 'Spain', 'Poland', 'Croatia', 'Latvia', 
                  'Netherlands', 'Slovenia', 'France', 'Cyprus', 
                  'Denmark', 'Finland', 'Malta', 'Lithuania', 
                  'Sweden', 'Belgium', 'Luxembourg', 
                  'Slovakia', 'Greece',  'Bulgaria', 'Estonia', 
                  'Romania', 'Czech Republic', 'Ireland')
edgar_emission_dataset_dt_copy[Name %in% EU_countries, country := "EU"]
edgar_emission_dataset_dt <- rbindlist(list(edgar_emission_dataset_dt, edgar_emission_dataset_dt_copy[country == "EU", ]), use.names = T)

edgar_emission_dataset_country_dt <- edgar_emission_dataset_dt[, .(co2_emission_month_avg = sum(co2_emission_month_avg)), by = .(myear, mmonth, country, sector)]

# CM_to_EDGAR_ratio_monthly <- fread("/data/ltao/Qiu_paper/CM_to_EDGAR_ratio_monthly.csv")
# edgar_emission_dataset_country_dt[CM_to_EDGAR_ratio_monthly, CM_to_EDGAR_ratio := i.ratio, on = .(mmonth, country)]
# edgar_emission_dataset_country_dt[, co2_emission_month_avg := co2_emission_month_avg * CM_to_EDGAR_ratio]
temperature_global[, c("myear", "mmonth") := list(year(mdate), month(mdate))]

for (sector_i in c("Transportation", "Residential", "Power", "Industry")) {
  temperature_global[CM_plus_EU_data_daily[sector == sector_i, ], CM := i.CM, on = .(country, mdate)]
  temperature_global[edgar_emission_dataset_country_dt[sector == sector_i, ], co2_emission_month_avg := i.co2_emission_month_avg, on = .(myear, country, mmonth)]
  
  CM_monthly_avg <- temperature_global[myear >= 2019, .(CM_avg = mean(CM)), by = .(myear, mmonth, country)]
  
  temperature_global[CM_monthly_avg, CM_avg := i.CM_avg, on = .(myear, mmonth, country)]
  temperature_global[, scale_factor := CM/CM_avg]
  
  temperature_global_before_2022 <- temperature_global[myear <= 2022, ]
  
  holidays_all <- fread("/data/ltao/Qiu_paper/holidays_all_dt.csv")
  temperature_global_before_2022[, is_holiday := 0]
  temperature_global_before_2022[holidays_all, is_holiday := 1, on = .(mdate, country)]
  ## 把农历腊月二十三至正月十五也作为public holiday
  Spring_festival_list <- foreach(y = 1970:2022) %do% {
    
    begin_date <- min(holidays_all[country == "China" & name == "Chinese New Year (Spring Festival)" & year(mdate) == y, ]$mdate)
    
    date_list <- seq(begin_date - 8, begin_date + 14, by = "days")
    return (data.table(country = "China", mdate = date_list))
  }
  
  Spring_festival_dt <- rbindlist(Spring_festival_list, use.names = T)
  temperature_global_before_2022[Spring_festival_dt, is_holiday := 1, on = .(mdate, country)]
  fwrite(temperature_global_before_2022, paste0("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/train_and_predict_dataset_", sector_i, "_0806.csv"))
}

fread("/data/ltao/Qiu_paper/data_for_reconstruction_by_sector/train_and_predict_dataset_Power_0806.csv")


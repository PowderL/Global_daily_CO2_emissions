library(data.table)
library(terra)
library(doParallel)
library(raster)
setDTthreads(10)

countries_index_dt <- fread("/data/ltao/ERA5/country_index_all_countries_dt.gz")
year_list <- c(1970, 1980, 1990, 2000, 2005, 2010, 2015, 2020)
era5_data_dir <- "/data/ltao/ERA5/temperature_daily_raster"
countries_index_dt[country == "Montenegro", country := "Serbia"]
registerDoParallel(cores = 5)
countries_tem_date_y_list <- foreach (y = year_list) %do% {
  if (y < 2000) {
    date_list <- seq(as.Date(paste0(y, "-01-01")), as.Date(paste0(y + 9, "-12-31")), by = "day")
    
    r_pop_den <- raster(paste0("/data/ltao/POP/popdynamics-global-pop-count-time-series-estimates_", y, ".tif"))
    r_pop_den <- crop(r_pop_den, extent(-180, 180, -90, 90))
    r <- raster(extent(c(-180, 180, -90.125, 90.125)), resolution = 1/120, crs = "+proj=longlat +datum=WGS84")
    values(r) <- 0
    values(r)[(15 * 43200 + 1):(21615 * 43200)] <- values(r_pop_den)
    r_pop_den <- rast(r)
    r_pop <- r_pop_den
    
    countries_index_dt$pop <- values(r_pop)[countries_index_dt$index]
    countries_index_dt[is.na(pop), pop := 0]
    countries_index_dt[, pop_scale := pop / 10^8]
    
    countries_tem_date_list <- foreach(i = 1:length(date_list)) %dopar% {
      date_i <- date_list[i]
      era5_path <- paste0(era5_data_dir, "/", date_i, ".tif")
      r_era5 <- raster(era5_path)
      countries_index_dt$tem <- values(r_era5)[countries_index_dt$index0083]
      countries_index_dt[, tem := round(tem, 2)]
      countries_index_dt[, pop_w_tem := tem * pop]
      countries_mean <- countries_index_dt[, .(pop_s = sum(pop), pop_w_tem_s = sum(pop_w_tem)), by = .(country)]
      countries_mean[, tem := round(pop_w_tem_s/pop_s, 2)]
      countries_mean$mtime <- date_i
      return (countries_mean[, c("country", "tem", "mtime")])
    }
    countries_tem_date_y <- rbindlist(countries_tem_date_list, use.names = T)
    return (countries_tem_date_y)
  }
  if (y >= 2000) {
    date_list <- seq(as.Date(paste0(y, "-01-01")), min(as.Date(paste0(y + 4, "-12-31")), as.Date("2024-12-31")), by = "day")
    
    r_pop_den <- raster(paste0("/data/ltao/POP/gpw_v4_population_density_rev11_", y, "_30_sec.tif"))
    
    r <- raster(extent(c(-180, 180, -90.125, 90.125)), resolution = 1/120, crs = "+proj=longlat +datum=WGS84")
    values(r) <- 0
    values(r)[(15 * 43200 + 1):(21615 * 43200)] <- values(r_pop_den)
    r_pop_den <- rast(r)
    r_pop_den_area <- cellSize(r_pop_den)
    r_pop <- r_pop_den * r_pop_den_area/(10^6)
    countries_index_dt$pop <- values(r_pop)[countries_index_dt$index]
    countries_index_dt[is.na(pop), pop := 0]
    countries_index_dt[, pop_scale := pop / 10^8]
    
    countries_tem_date_list <- foreach(i = 1:length(date_list)) %dopar% {
      date_i <- date_list[i]
      print(paste0(Sys.time(), " ", date_i))
      era5_path <- paste0(era5_data_dir, "/", date_i, ".tif")
      r_era5 <- raster(era5_path)
      
      countries_index_dt$tem <- values(r_era5)[countries_index_dt$index0083]
      countries_index_dt[, tem := round(tem, 2)]
      countries_index_dt[, pop_w_tem := tem * pop]
      countries_mean <- countries_index_dt[, .(pop_s = sum(pop), pop_w_tem_s = sum(pop_w_tem)), by = .(country)]
      countries_mean[, tem := round(pop_w_tem_s/pop_s, 2)]
      countries_mean$mtime <- date_i
      return (countries_mean[, c("country", "tem", "mtime")])
    }
    countries_tem_date_y <- rbindlist(countries_tem_date_list, use.names = T)
    return (countries_tem_date_y)
  }
}

countries_tem_date_y_all <- rbindlist(countries_tem_date_y_list, use.names = T)
fwrite(countries_tem_date_y_all, "/data/ltao/ERA5/countries_population_weigted_mean_temperature_all_world_countries_new_pop.csv")

# fwrite(countries_tem_date_y, "/data/ltao/ERA5/countries_population_weigted_mean_temperature_all_world_countries_new_pop_2024.csv")



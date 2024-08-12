library(raster)
library(terra)
library(doParallel)
library(data.table)
gen_raster_define <- function (value_cells, grid.extent = c(102.5, 105, 30, 31.5),
                               grid.res = 0.01) {
  value_raster <- raster(extent(grid.extent), resolution = grid.res,
                         crs = "+proj=longlat +datum=WGS84")
  row_min <- ceiling((90.125 - grid.extent[4] + grid.res/2)/grid.res)
  col_min <- ceiling((grid.extent[1] + 180 + grid.res/2)/grid.res)
  n.rows <- nrow(value_raster)
  n.cols <- ncol(value_raster)
  r.values <- rep(NA, n.rows * n.cols)
  i.loc <- (value_cells[, 1] - row_min) * n.cols + (value_cells[,
                                                                2] - col_min) + 1
  r.values[i.loc] <- value_cells[, 3]
  values(value_raster) <- r.values
  return(value_raster)
}
# 找到每个国家的网格index
r <- raster(extent(c(-180, 180, -90.125, 90.125)), resolution = 1/120, crs = "+proj=longlat +datum=WGS84")
values(r) <- 1
world_contries <- sf::st_read("/data/ltao/gis/World_Countries", layer = "World_Countries_Generalized")
country_list <- c(
                  "United States", "China", "India", "Japan", "Brazil", "Germany", "Italy", "United Kingdom",
                  "Austria", "Portugal", "Hungary", "Spain", "Poland", "Croatia", "Latvia", "Ireland",
                  "Netherlands", "Slovenia", "France", "Cyprus", "Denmark", "Finland", "Malta", "Lithuania",
                  "Sweden", "Belgium", "Russian Federation", "Luxembourg", "Slovakia", "Greece", "Bulgaria", "Estonia",
                  "Romania",
                  "Czech Republic")
all(country_list %in% world_contries$COUNTRY)
countries_index_list <- foreach(i = 1:length(country_list)) %do% {
  country_i <- world_contries[world_contries$COUNTRY %in% country_list[i],]
  r_mask <- mask(r, country_i)
  index_list <- which(!is.na(values(r_mask)))
  return (data.table(index = index_list, country = country_list[i]))
}
countries_index_dt <- rbindlist(countries_index_list, use.names = T)
countries_index_dt[, c("row001", "col001") := list((index - 1) %/% 43200 + 1, (index - 1) %% 43200 + 1)]
countries_index_dt[,  c("row0083", "col0083") := list((row001 -1) %/% 30 + 1, (col001 - 1) %/% 30 + 1)]
countries_index_dt[, index0083 := (row0083 -1) * 1440 + col0083]
fwrite(countries_index_dt, "/data/ltao/ERA5/country_index_add_czech_dt.gz")
countries_index_dt <- fread("/data/ltao/ERA5/country_index_add_czech_dt.gz")
# r_pop_den <- rast("/data3/ltao/POP/gpw_v4_population_density_rev11_2020_30_sec.tif")
# r_pop_den_area <- cellSize(r_pop_den)
# r_pop <- r_pop_den * r_pop_den_area
# pop_dt <- data.table(pop = values(r_pop)[countries_index_dt$index])
era5_data_dir <- "/data/ltao/ERA5/temperature_daily_raster"
date_list <- seq(as.Date("1970-01-01"), as.Date("2022-12-31"), by = "days")
registerDoParallel(cores = 30)
foreach(i = 1:length(date_list)) %dopar% {
  date_i <- date_list[i]
  out.path <- paste0("/data/ltao/ERA5/country_level_temperature_add_czech/", date_i, ".gz")
  era5_path <- paste0(era5_data_dir, "/", date_i, ".tif")
  r_era5 <- raster(era5_path)
  dt <- data.table(tem = values(r_era5)[countries_index_dt$index0083])
  dt[, tem := round(tem, 2)]
  fwrite(dt, out.path)
  rm(dt)
  gc()
}
## 

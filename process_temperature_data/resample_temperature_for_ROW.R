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
r <- raster(extent(c(-180, 180, -90.125, 90.125)), resolution = 1/8, crs = "+proj=longlat +datum=WGS84")
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
country_ROW <- world_contries[!(world_contries$COUNTRY %in% country_list),]
r_mask <- mask(r, country_ROW)
index_list <- which(!is.na(values(r_mask)))
countries_index_dt <- data.table(index = index_list, country = "ROW")

countries_index_dt[, c("row001", "col001") := list((index - 1) %/% 2880 + 1, (index - 1) %% 2880 + 1)]

countries_index_dt[,  c("row0125", "col0125") := list((row001 -1) %/% 2 + 1, (col001 - 1) %/% 2 + 1)]

countries_index_dt[, index0125 := (row0125 -1) * 1440 + col0125]

fwrite(countries_index_dt, "/data/ltao/ERA5/country_index_ROW_dt.gz")
countries_index_dt <- fread("/data/ltao/ERA5/country_index_ROW_dt.gz")
# r_pop_den <- rast("/data3/ltao/POP/gpw_v4_population_density_rev11_2020_30_sec.tif")
# r_pop_den_area <- cellSize(r_pop_den)
# r_pop <- r_pop_den * r_pop_den_area
# pop_dt <- data.table(pop = values(r_pop)[countries_index_dt$index])
era5_data_dir <- "/data/ltao/ERA5/temperature_daily_raster"
date_list <- seq(as.Date("1970-01-01"), as.Date("2022-12-31"), by = "days")
registerDoParallel(cores = 10)
foreach(i = 1:length(date_list)) %dopar% {
  date_i <- date_list[i]
  out.path <- paste0("/data/ltao/ERA5/country_level_temperature_ROW/", date_i, ".gz")
  era5_path <- paste0(era5_data_dir, "/", date_i, ".tif")
  r_era5 <- raster(era5_path)
  dt <- data.table(tem = values(r_era5)[countries_index_dt$index0125])
  dt[, tem := round(tem, 2)]
  fwrite(dt, out.path)
  rm(dt)
  gc()
}
## 

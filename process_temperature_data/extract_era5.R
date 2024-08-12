library(terra)
library(raster)
library(ncdf4)
library(doParallel)
era5_data_nc_dir <- "/data/ltao/ERA5/data"
registerDoParallel(cores = 15)
foreach(y = c(1970:2022)) %do% {
  era5_data_path <- paste0(era5_data_nc_dir, "/", y, ".nc")
  nc_file <- nc_open(era5_data_path)
  lat <- ncvar_get(nc_file, "latitude")
  lon <- ncvar_get(nc_file, "longitude")
  time <- ncvar_get(nc_file, "time")
  mtime <- as.POSIXct("1900-01-01 00:00:00", tz = "UTC") + time * 3600
  tem <- ncvar_get(nc_file, "t2m")
  nc_close(nc_file)
  if ((y  %% 4 == 0 & y %% 100 != 0) | y %% 400 == 0){
    num_day <- 366
  }else{
    num_day <- 365
  }
  foreach(j = 1:num_day) %dopar% {
    if (dim(tem)[3] == 2) {
      if (j <= 304) {
        tem_daily <- tem[, , 1,((j-1)*24 + 1):(j*24)]
      }else {
        tem_daily <- tem[, , 2,((j-1)*24 + 1):(j*24)]
      }
    }else {
      tem_daily <- tem[, , ((j-1)*24 + 1):(j*24)]
    }
    
    # mtime[((j-1)*24 + 1):(j*24)]
    ## 计算一个平均值
    tem_daily_avg <- apply(tem_daily, MARGIN = c(1, 2), FUN = mean)
    tem_daily_avg <- tem_daily_avg - 273.25
    r <- raster(extent(c(0, 360, -90 - 0.125, 90 + 0.125)), resolution = 0.25, crs = "+proj=longlat +datum=WGS84")
    values(r) <- as.numeric(tem_daily_avg)
    r_rotate <- rotate(r)
    writeRaster(r_rotate, paste0("/data/ltao/ERA5/temperature_daily_raster/", as.Date(paste0(y, "-01-01")) + j - 1, ".tif"), overwrite=TRUE)
  }
  rm(tem)
  gc()
}

# Synthetic deployment fixtures. Rebuild only when intentionally updating fixtures.
# R/sf are needed to regenerate these files, not to use the packaged fixture.
args <- commandArgs(TRUE)
root <- if (length(args)) args[[1]] else "tests/fixtures/deployment/data"
crs <- paste(readLines("tests/fixtures/deployment/wgs84.wkt", warn=FALSE), collapse="")
for (i in 1:2) {
  id <- c("animal-A", "animal-B")[[i]]
  n <- 12L
  x <- data.frame(
    event_id = paste0(id, "-", seq_len(n)),
    x_ = 10 + i + seq_len(n) * 0.001,
    y_ = 40 + i + sin(seq_len(n)) * 0.001,
    t_ = as.numeric(as.POSIXct("2024-01-01", tz="UTC")) + seq_len(n) * 3600,
    individual_local_identifier = id, individual_id = i, study_id = 9001,
    burst_ = 1L, is_outlier = FALSE,
    species = c("Synthetic species A", "Synthetic species B")[[i]],
    study_name = "Synthetic deployment test", stringsAsFactors = FALSE
  )
  x$timestamp <- x$t_
  csv <- data.frame(eventid=x$event_id, individual=id,
    timestamp=format(as.POSIXct(x$t_, origin="1970-01-01", tz="UTC"), "%Y-%m-%dT%H:%M:%SZ", tz="UTC"),
    longitude=x$x_, latitude=x$y_, species=x$species, check.names=FALSE)
  csv_dir <- file.path(root, "movement_raw", "acceptance")
  dir.create(csv_dir, recursive=TRUE, showWarnings=FALSE)
  if (i == 1) all_csv <- csv else all_csv <- rbind(all_csv, csv)
  x <- sf::st_as_sf(x, coords=c("x_", "y_"), remove=FALSE, crs=crs)
  class(x) <- c("move2", class(x))
  attr(x,"time_column") <- "t_"
  attr(x,"track_id_column") <- "individual_local_identifier"
  attr(x,"track_data") <- data.frame(individual_local_identifier=id)
  rds_dir <- file.path(root, "movement_rds", "acceptance")
  dir.create(rds_dir, recursive=TRUE, showWarnings=FALSE)
  saveRDS(x, file.path(rds_dir, paste0("9001_", i, ".rds")), version=3)
}
write.csv(all_csv, file.path(csv_dir, "movement.csv"), row.names=FALSE, fileEncoding="UTF-8")

c_val <- Sys.getenv("C_VAL")
dir_path <- Sys.getenv("OUTPUT_PIPELINE_DIR")
dir_path <- file.path(dir_path, "D")
# Make sure dir_path exists
if (!dir.exists(dir_path)) {
  dir.create(dir_path, recursive = TRUE)
}
filepath <- file.path(dir_path, "D_2024.csv")
cat(c_val, file = filepath, append = TRUE)
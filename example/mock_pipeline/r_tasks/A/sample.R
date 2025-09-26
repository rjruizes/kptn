# This script writes a value to a file in the scratch directory.
# The value is the value of the environment variable A_VAL, or
# "A" if the environment variable is not set.

a_val <- Sys.getenv("DATA_YEAR")
dir_path <- Sys.getenv("OUTPUT_PIPELINE_DIR")
dir_path <- file.path(dir_path, "A")
# Make sure dir_path exists
if (!dir.exists(dir_path)) {
  dir.create(dir_path, recursive = TRUE)
}
filepath <- file.path(dir_path, "A_2024.csv")
cat(a_val, file = filepath)
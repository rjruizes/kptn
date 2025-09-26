options(
  keep.source = TRUE,
  error = function(){ traceback(2,max.lines=3);if(!interactive())quit("no",status=1,runLast=FALSE) }
)

write_param <- function(output_dir, item) {
  output_dir <- fs::path(output_dir, "my_output")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  writeLines(item, fs::path(output_dir, stringr::str_glue("{item}.txt")))
}

# call write_param with environment variables
output_dir <- Sys.getenv("OUTPUT_PIPELINE_DIR")
item <- Sys.getenv("item")
write_param(output_dir, item)
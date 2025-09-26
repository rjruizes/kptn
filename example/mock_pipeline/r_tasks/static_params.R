options(
  keep.source = TRUE,
  error = function(){ traceback(2,max.lines=3);if(!interactive())quit("no",status=1,runLast=FALSE) }
)

static_params <- function(output_dir) {
  output_dir <- fs::path(output_dir, "my_output")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  writeLines("blah", fs::path(output_dir, stringr::str_glue("dummy1.txt")))
  writeLines("blah", fs::path(output_dir, stringr::str_glue("dummy2.txt")))
}

# call write_param with environment variables
output_dir <- Sys.getenv("OUTPUT_PIPELINE_DIR")
static_params(output_dir)
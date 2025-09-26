options(
  keep.source = TRUE,
  error = function(){ traceback(2,max.lines=3);if(!interactive())quit("no",status=1,runLast=FALSE) }
)

combo_process <- function(output_dir, item1, item2) {
  output_dir <- file.path(output_dir, "combo_process")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  if (item1 == "T1" && item2 == "1") {
    output_file <- file.path(output_dir, "T1.txt")
    if (file.exists(output_file)) {
      return(NULL)
    } else {
      writeLines("T1", output_file)
      stop("Purposefully failing for testing")
    }
  }
}

# call combo_process with environment variables
output_dir <- Sys.getenv("OUTPUT_PIPELINE_DIR")
item1 <- Sys.getenv("item1")
item2 <- Sys.getenv("item2")
combo_process(output_dir, item1, item2)
# Caching

We cache pipeline tasks to avoid re-computation of the same task when the pipeline is run again. A task is recomputed if any of the following are true:

1. The arguments of the task have changed
2. The code of the task has changed
3. The input files of the task have changed (inputs to the task are outputs of the tasks listed as `dependencies`)


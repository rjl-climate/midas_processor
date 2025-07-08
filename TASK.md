# Main Application Logic


You will now implement the `process` command that converts a MIDAS dataset into a parquet file.

The command will present the user with a list of available datasets to convert with a default option of `all` and get the user's selection.

The selected dataset will be scanned and converted to a parquet database. The file will be named after the dataset and the version of it as YYYYMM.

The command will accept an optional parameter to specify the location. The config system will be updated to allow this to be specified. It will default to folder `parquet` in the root of the folder configured to contain the datasets.

A progress bar will be presented to the user to display progress and ETA.

You will implement comprehensive error handling.

You will configure logging with tracing.

You will handle graceful shutdown (e.g. from CRTL-C)

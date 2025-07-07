# REWRITE CSV PARSER

The CSV parser module appears to be far too complex - 1875 lines of code including tests.

The objective of this module is to represent the weather data distributed in CSV files in a .parquet file.

The data to be parsed is numerical. Parsing should be robust enough to convert different representations of numerical data - string, float, integer, or string - into a consistent representation for parquet conversion.

Quality control consists of detecting .CSV files that cannot be parsed. It should not consist of attempts to assess whether the .CSV file conforms exactly to BADC-CSV spoecification. Quality control fields in the original data should be passed through "as-is" and responsibility left to downstream processing to interpret.

Parsing should support enrichment of each observatioon record with data obtained from header information from station capability records or the observation record, but this should be kept simple.

You will create a new csv_parser module to ensure none of the complexity from the first attempt is carried over - you may refer to the old version for specifics for parsing details if it speeds up code development but do not copy routines - think hard and create these from first principles.

1. Examine the planning document PLANNING.md to understand the objectives of the processor - to represent the weather data distriburted in CSV files in a .parquet file. You a

2. Examine the BADC-CSV format guide https://help.ceda.ac.uk/article/105-badc-csv to understand the BADC-CSV data format.

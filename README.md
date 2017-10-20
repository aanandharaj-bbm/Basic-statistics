# Basic-statistics

It provides the descriptive statistics for any dataset in parquet and csv formats.

To run the script execute the following command :

spark-submit basic_stats.py <location_of_the_data_file_name_in_gcs> all_text><output_text_file_name>

example: spark-submit basic_stats.py "gs://ds-url-catag/project_self/train.csv" all_text>csv_stats.txt


The following information is displayed in the script :

1) Data schema
2) Total and distinct records
3) Mising values in each of the fields
4) Min,Max,Mean,standard deviation of Interger fields
5) Categorical fields and their distribution

# README

Parsing is working for radiation and temperature data sets but failing for the rain data set. The issue I think is handling different columns and unions of them when the column names are varying.

This dataset works:'/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-radiation-obs-202507'
This dataset fails: '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-rain-obs-202407'

Example output after failure is:

```
: 105553249278176]
                PROJECT */15 COLUMNS
                SLICE: Positive { offset: 0, len: 365 }
          PLAN 419:
            SELECT [col("ob_date"), col("id"), col("id_type"), col("version_num"), col("met_domain_name"), col("ob_end_ctime"), col("ob_day_cnt"), col("src_id"), col("rec_st_ind"), col("prcp_amt"), col("ob_day_cnt_q"), col("prcp_amt_q"), col("meto_stmp_time"), col("midas_stmp_etime"), col("station_id"), col("station_name"), col("county"), col("latitude"), col("longitude"), col("height"), col("height_units")]
               WITH_COLUMNS:
               [dyn float: 57.339.alias("latitude"), dyn float: -4.012.alias("longitude"), "tomatin-no-2".alias("station_name"), "62214".alias("station_id"), "inverness-shire".alias("county"), dyn float: 310.alias("height"), "m".alias("height_units")]
                Csv SCAN [/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-rain-obs-202407/qcv-1/inverness-shire/62214_tomatin-no-2/midas-open_uk-daily-rain-obs_dv-202407_inverness-shire_62214_tomatin-no-2_qcv-1_2020.csv] [id: 105553249278192]
                PROJECT */15 COLUMNS
                SLICE: Positive { offset: 0, len: 213 }
          PLAN 420:
            SELECT [col("ob_date"), col("id"), col("id_type"), col("version_num"), col("met_domain_name"), col("ob_end_ctime"), col("ob_day_cnt"), col("src_id"), col("rec_st_ind"), col("prcp_amt"), col("ob_day_cnt_q"), col("prcp_amt_q"), col("meto_stmp_time"), col("midas_stmp_etime"), col("station_id"), col("station_name"), col("county"), col("latitude"), col("longitude"), col("height"), col("height_units")]
               WITH_COLUMNS:
               [dyn float: 57.339.alias("latitude"), dyn float: -4.012.alias("longitude"), "tomatin-no-2".alias("station_name"), "62214".alias("station_id"), "inverness-shire".alias("county"), dyn float: 310.alias("height"), "m".alias("height_units")]
                Csv SCAN [/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-rain-obs-202407/qcv-1/inverness-shire/62214_tomatin-no-2/midas-open_uk-daily-rain-obs_dv-202407_inverness-shire_62214_tomatin-no-2_qcv-1_2022.csv] [id: 105553249292464]
                PROJECT */15 COLUMNS
                SLICE: Positive { offset: 0, len: 335 }
          PLAN 421:
            SELECT [col("ob_date"), col("id"), col("id_type"), col("version_num"), col("met_domain_name"), col("ob_end_ctime"), col("ob_day_cnt"), col("src_id"), col("rec_st_ind"), col("prcp_amt"), col("ob_day_cnt_q"), col("prcp_amt_q"), col("meto_stmp_time"), col("midas_stmp_etime"), col("station_id"), col("station_name"), col("county"), col("latitude"), col("longitude"), col("height"), col("height_units")]
               WITH_COLUMNS:
               [dyn float: 57.339.alias("latitude"), dyn float: -4.012.alias("longitude"), "tomatin-no-2".alias("station_name"), "62214".alias("station_id"), "inverness-shire".alias("county"), dyn float: 310.alias("height"), "m".alias("height_units")]
                Csv SCAN [/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-rain-obs-202407/qcv-1/inverness-shire/62214_tomatin-no-2/midas-open_uk-daily-rain-obs_dv-202407_inverness-shire_62214_tomatin-no-2_qcv-1_2021.csv] [id: 105553249292480]
                PROJECT */15 COLUMNS
                SLICE: Positive { offset: 0, len: 365 }
        END UNION
    END UNION
END UNION
⠄ [00:00:46] Creating parquet file, this will take a few minutes...
```

Several attempts to fix it have failed using The Polar's column handling with diagonals. It's taking too long to iterate using the very large dataset.

We need to step back and make sure we understand the API and how to use it. The purpose of this is to create a test example that fails and use that to iterate on to understand the problem and how to Fix it with the API.

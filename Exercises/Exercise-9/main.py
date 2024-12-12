import polars as pl

def main():
    file_path = "data/202306-divvy-tripdata.csv"

    schema_overrides = {
        "end_station_id": pl.Utf8
    }

    lazy_df = pl.read_csv(file_path, schema_overrides = schema_overrides).lazy()

    lazy_df = lazy_df.with_columns([
        pl.col("ride_id").cast(pl.Utf8),
        pl.col("rideable_type").cast(pl.Utf8),
        pl.col("started_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        pl.col("ended_at").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        pl.col("start_station_name").cast(pl.Utf8),
        pl.col("start_station_id").cast(pl.Utf8),
        pl.col("end_station_name").cast(pl.Utf8),
        pl.col("start_lat").cast(pl.Float64),
        pl.col("start_lng").cast(pl.Float64),
        pl.col("end_lat").cast(pl.Float64),
        pl.col("end_lng").cast(pl.Float64),
        pl.col("member_casual").cast(pl.Utf8)
    ])

    lazy_df_with_date = lazy_df.with_columns([
        pl.col("started_at").dt.strftime("%Y-%m-%d").alias("date")
    ])

    rides_per_day = lazy_df_with_date.group_by("date").agg([
        pl.count("ride_id").alias("ride_count")  
    ]).collect()

    print(rides_per_day.head(10))

if __name__ == "__main__":
    main()

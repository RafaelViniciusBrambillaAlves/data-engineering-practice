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

    lazy_df_with_date_and_week = lazy_df.with_columns([
        pl.col("started_at").dt.strftime("%Y-%m-%d").alias("date"),
        pl.col("started_at").dt.week().alias("week"),
        pl.col("started_at").dt.weekday().alias("weekday")
    ])

    rides_per_day = lazy_df_with_date_and_week.group_by("date").agg([
        pl.count("ride_id").alias("ride_count")  
    ]).collect()
    
    rides_per_week = lazy_df_with_date_and_week.group_by("week").agg([
        pl.count("ride_id").alias("ride_count")
    ]).collect()

    average_rides_per_week = rides_per_week["ride_count"].mean()
    min_rides_per_week = rides_per_week["ride_count"].min()
    max_rides_per_week = rides_per_week["ride_count"].max()

    rides_per_day = lazy_df_with_date_and_week.group_by(["date", "week", "weekday"]).agg([
        pl.count("ride_id").alias("ride_count")  
    ]).collect()

    rides_previous_week = rides_per_day.with_columns(
        (pl.col("week") + 1).alias("week")  
    ).select(["week", "weekday", "ride_count"])  
    
    rides_with_diff = rides_per_day.join(
        rides_previous_week.with_columns(
            pl.col("ride_count").fill_null(0).alias("ride_count")
        ), 
        on = ["week", "weekday"],
        how = "left"
    ).with_columns([
        (pl.col("ride_count") - pl.col("ride_count_right").fill_null(0)).alias("difference_from_last_week"),
        pl.col("ride_count_right").fill_null(0).alias("previous_week_count")
    ]).select(["date", "week", "weekday", "ride_count", "previous_week_count", "difference_from_last_week"])

    print(rides_per_day.head(10))
    
    print(f"Average rides per week: {average_rides_per_week}")
    print(f"Min rides per week: {min_rides_per_week}")
    print(f"Max rides per week: {max_rides_per_week}")

    print(rides_with_diff)

if __name__ == "__main__":
    main()

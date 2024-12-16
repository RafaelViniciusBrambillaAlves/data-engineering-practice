import polars as pl

def read_data(file_path: str) -> pl.DataFrame:
    
    return pl.read_csv(file_path,
                        schema_overrides={
                            "ride_id": pl.Utf8,
                            "rideable_type": pl.Categorical,
                            "started_at": pl.Datetime,
                            "ended_at": pl.Datetime,
                            "start_station_name": pl.Utf8,
                            "start_station_id": pl.Utf8,
                            "end_station_name": pl.Utf8,
                            "end_station_id": pl.Utf8,
                            "start_lat": pl.Float32,
                            "start_lng": pl.Float32,
                            "end_lat": pl.Float32,
                            "end_lng": pl.Float32,
                            "member_casual": pl.Categorical,
                        })

def question1(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col("started_at").dt.date().alias("started_at_date")).group_by("started_at_date").count()

def question2(df: pl.DataFrame) -> pl.DataFrame:
    return (df.with_columns(pl.col("started_at").dt.week())
            .group_by("started_at")
            .agg(cnt = pl.col("started_at").count())).select(
                                                                pl.mean("cnt").alias("mean_per_week"),
                                                                pl.min("cnt").alias("min_per_week"),
                                                                pl.max("cnt").alias("max_per_week")
            )

def question3(df: pl.DataFrame) -> pl.DataFrame:
    return (
            (
                df.with_columns(pl.col("started_at").dt.date().alias("started_at_date")).group_by("started_at_date").agg(pl.count().alias("count"))
            )
            .sort("started_at_date")
            .with_columns(
                pl.col("count").shift(7).alias("count_last_week")
            ).select(
                pl.col("count"),
                pl.col("count_last_week"),
                (pl.col("count") - pl.col("count_last_week")).alias("diff")
            )
    )

def main():
    file_path = "data/202306-divvy-tripdata.csv"

    df = read_data(file_path)

    print(question1(df))

    print(question2(df))

    print(question3(df))

   
if __name__ == "__main__":
    main()

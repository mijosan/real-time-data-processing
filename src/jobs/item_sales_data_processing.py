from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, to_json, struct, from_csv, to_timestamp, concat, lit
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField

def process_item_sales_data(spark, item_sales_data_df: DataFrame, item_master_data_df: DataFrame):
    # 1. 아이템 판매 데이터(item_sales_data_df)와 아이템 마스터 데이터(item_master_data_df)를 item_code 기준으로 inner join
    joined_item_sales_data_df = item_sales_data_df.join(
        item_master_data_df,
        item_sales_data_df["item_code"] == item_master_data_df["item_code"],
        how="inner"
    ).select(
        # 필요한 컬럼만 선택
        item_sales_data_df["sales_date"],
        item_sales_data_df["sales_time"],
        item_master_data_df["item_code"],
        item_master_data_df["item_name"],
        item_sales_data_df["sales_quantity"],
        item_sales_data_df["sales_price"],
        item_sales_data_df["sales_return"],
        item_sales_data_df["is_discount"],
        item_master_data_df["category_code"],
        item_master_data_df["category_name"]
    )
    
    # 2. sales_date와 sales_time을 합쳐서 Timestamp 타입의 sales_datetime 컬럼 생성
    joined_item_sales_data_df = joined_item_sales_data_df.withColumn(
        "sales_datetime", 
        to_timestamp(
            concat(joined_item_sales_data_df["sales_date"], lit(" "), joined_item_sales_data_df["sales_time"]), 
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )
    
    # 3. 사용 완료한 sales_date, sales_time 컬럼 삭제
    joined_item_sales_data_df = joined_item_sales_data_df.drop("sales_date", "sales_time")
    
    # 4. null(결측치) 행 제거
    joined_item_sales_data_df = joined_item_sales_data_df.dropna()
    
    # 5. 환불되지 않는(sales_return='sale') 거래만 필터링
    joined_item_sales_data_df = joined_item_sales_data_df.filter(joined_item_sales_data_df["sales_return"] == "sale")
    
    # 6. sales_quantity와 sales_price를 이용해 총 판매금액(sales_amount) 컬럼 추가
    processed_sales_data_df = joined_item_sales_data_df.withColumn(
        "sales_amount",
        joined_item_sales_data_df["sales_quantity"] * joined_item_sales_data_df["sales_price"]
    )
    
    return processed_sales_data_df

def analyze_item_sales_data_statistics(processed_sales_data_df: DataFrame):
    # 7. 처리된 판매 데이터에 대해 윈도우 기반의 집계 작업 수행
    #    - 10분 윈도우로 집계, 5분 단위로 슬라이드
    #    - sales_amount, sales_quantity 합계 계산
    #    - window_start, window_end와 함께 item_code, item_name, category_code, category_name 별로 통계 산출
    sales_data_statistics_df = (
        processed_sales_data_df
        .withWatermark("sales_datetime", "15 minutes")  # 지연 허용 시간 설정: 늦게 도착한 데이터 처리
        .groupBy(
            window(processed_sales_data_df["sales_datetime"], "10 minutes", "5 minutes"),
            processed_sales_data_df["item_code"],
            processed_sales_data_df["item_name"],
            processed_sales_data_df["category_code"],
            processed_sales_data_df["category_name"]
        )
        .agg(
            spark_sum(processed_sales_data_df["sales_amount"]).alias("total_sales_amount"),
            spark_sum(processed_sales_data_df["sales_quantity"]).alias("total_sales_quantity")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("item_code"),
            col("item_name"),
            col("category_code"),
            col("category_name"),
            col("total_sales_amount"),
            col("total_sales_quantity")
        )
        .orderBy(col("window_start").asc())  # 시간순 정렬
    )
    return sales_data_statistics_df

def main():
    # 8. SparkSession 생성
    spark = (
        SparkSession.builder
        .appName("KafkaSalesProcessing")
        .getOrCreate()
    )
    
    # 9. 마스터 데이터 스키마 정의
    item_master_data_schema = StructType([
        StructField("item_code", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("category_name", StringType(), True)
    ])
    
    # 10. 판매 데이터 스키마 정의
    item_sales_data_schema = StructType([
        StructField("sales_date", StringType(), True),
        StructField("sales_time", StringType(), True),
        StructField("item_code", StringType(), True),
        StructField("sales_quantity", DoubleType(), True),
        StructField("sales_price", DoubleType(), True),
        StructField("sales_return", StringType(), True),
        StructField("is_discount", StringType(), True)
    ])
    
    # 11. PostgreSQL 연결 설정 (예: 실제 환경에서 JDBC 사용)
    jdbc_url = "jdbc:postgresql://<HOST>:<PORT>/<DATABASE>"
    jdbc_properties = {
        "user": "<USERNAME>",
        "password": "<PASSWORD>",
        "driver": "org.postgresql.Driver"
    }
    
    # 12. 아이템 마스터 데이터 PostgreSQL에서 로드
    item_master_data_df = spark.read.jdbc(
        url=jdbc_url,
        table="item_master_table",
        properties=jdbc_properties,
        schema=item_master_data_schema
    )
    
    # 13. Kafka에서 판매 데이터 스트리밍 수신
    #     - value 필드를 CSV 스키마로 파싱하여 DataFrame 생성
    item_sales_data_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "sales_topic")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_csv(col("value"), item_sales_data_schema).alias("data"))
        .select("data.*")
    )
    
    # 14. 전처리 함수 호출
    processed_item_sales_data_df = process_item_sales_data(spark, item_sales_data_df, item_master_data_df)
    
    # 15. 통계 집계 함수 호출
    sales_data_statistics_df = analyze_item_sales_data_statistics(processed_item_sales_data_df)

    # 16. 결과를 다시 Kafka로 전송
    query = (
        sales_data_statistics_df
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "sales_data_statistics_topic")
        .option("checkpointLocation", "/tmp/spark_checkpoint")  # 상태 관리 폴더
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, to_json, struct, from_csv, to_timestamp, concat, lit
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField

def process_item_sales_data(spark, item_sales_data_df: DataFrame, item_master_data_df: DataFrame):
    joined_item_sales_data_df = item_sales_data_df.join(
        item_master_data_df,  # 조인할 다른 DataFrame
        item_sales_data_df["item_code"] == item_master_data_df["item_code"],  # 조인 키
        how="inner"  # 조인 유형: inner, left, right, full 등을 선택할 수 있음)
    ).select(
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
    
    # sales_date, sales_time 칼럼을 합쳐서 sales_datetime 칼럼 추가
    joined_item_sales_data_df = joined_item_sales_data_df.withColumn(
        "sales_datetime", 
        to_timestamp(concat(joined_item_sales_data_df["sales_date"], lit(" "), joined_item_sales_data_df["sales_time"]), "yyyy-MM-dd HH:mm:ss.SSS")
    )
    
    # 기존 sales_date, sales_time 칼럼 제거
    joined_item_sales_data_df = joined_item_sales_data_df.drop("sales_date", "sales_time")
    
    # 결측치 제거
    joined_item_sales_data_df = joined_item_sales_data_df.dropna()
    
    # 환불되지 않는 거래 필터링
    joined_item_sales_data_df = joined_item_sales_data_df.filter(joined_item_sales_data_df["sales_return"] == 'sale')
    
    # 총 판매금액 계산
    processed_sales_data_df = joined_item_sales_data_df.withColumn(
        "sales_amount",
        joined_item_sales_data_df["sales_quantity"] * joined_item_sales_data_df["sales_price"]
    )
    
    return processed_sales_data_df

sales_data_statistics_df = (processed_sales_data_df
    .withWatermark("sales_datetime", "15 minutes")  # 지연 허용 시간 설정
    .groupBy(
        window(processed_sales_data_df["sales_datetime"], "10 minutes", "5 minutes"),  # 10분 윈도우, 5분 슬라이드
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
        processed_sales_data_df["window.start"].alias("window_start"),
        processed_sales_data_df["window.end"].alias("window_end"),
        processed_sales_data_df["item_code"],
        processed_sales_data_df["item_name"],
        processed_sales_data_df["category_code"],
        processed_sales_data_df["category_name"],
        processed_sales_data_df["total_sales_amount"],
        processed_sales_data_df["total_sales_quantity"]
    )
    .orderBy(processed_sales_data_df["window_start"].asc()))
    
    return sales_data_statistics_df

def main():
    # 스파크 세션 생성
    spark = (SparkSession.builder 
        .appName("KafkaSalesProcessing")
        .getOrCreate())
    
    # 마스터 데이터 스키마 정의
    item_master_data_schema = StructType([
        StructField("item_code", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("category_name", StringType(), True)
    ])
    
    # 판매 데이터 스키마 정의
    item_sales_data_schema = StructType([
        StructField("sales_date", StringType(), True),
        StructField("sales_time", StringType(), True),
        StructField("item_code", StringType(), True),
        StructField("sales_quantity", DoubleType(), True),
        StructField("sales_price", DoubleType(), True),
        StructField("sales_return", StringType(), True),
        StructField("is_discount", StringType(), True)
    ])
    
    # PostgreSQL 연결 설정 (실제 환경에서 사용)
    jdbc_url = "jdbc:postgresql://<HOST>:<PORT>/<DATABASE>"
    jdbc_properties = {
        "user": "<USERNAME>",
        "password": "<PASSWORD>",
        "driver": "org.postgresql.Driver"
    }
    
    # PostgreSQL에서 상품 마스터 데이터 로드
    item_master_data_df = spark.read.jdbc(
        url=jdbc_url,
        table="item_master_table",
        properties=jdbc_properties,
        schema=item_master_data_schema
    )
    
    # Kafka 데이터 스트리밍 소스
    item_sales_data_df = (spark
        .readStream 
        .format("kafka") 
        .option("kafka.bootstrap.servers", "localhost:9092") 
        .option("subscribe", "sales_topic") 
        .load()
        .selectExpr("CAST(value AS STRING)")  # Kafka 메시지의 value 필드를 문자열로 변환
        .select(from_csv(col("value"), item_sales_data_schema)))  # CSV 형식의 데이터를 스키마에 맞게 변환

    # 판매 데이터 전 처리
    processed_item_sales_data_df = process_item_sales_data(spark, item_sales_data_df, item_master_data_df)
    
    # 판매 데이터 통계
    sales_data_statistics_df = analyze_item_sales_data_statistics(processed_item_sales_data_df)

    # 처리된 데이터를 다시 Kafka로 전송
    query = (sales_data_statistics_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "sales_data_statistics_topic")
        .option("checkpointLocation", "/tmp/spark_checkpoint")
        .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()

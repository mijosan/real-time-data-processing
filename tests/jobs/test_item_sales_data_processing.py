import os
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField

from src.jobs.item_sales_data_processing import analyze_item_sales_data_statistics, process_item_sales_data

def test_main():
    # Given: Spark 세션을 설정하고, CSV 파일에서 판매 거래 및 아이템 마스터 데이터를 로드합니다.
    spark = (SparkSession.builder 
        .appName("Sales Data Test") 
        .master("local[*]")
        .getOrCreate())

    # CSV 파일 경로
    base_path = os.path.dirname(os.path.abspath(__file__))  # 현재 파일의 절대 경로
    item_sales_data_path = os.path.join(base_path, '../../data/input/item_sales_data.csv')
    item_master_data_path = os.path.join(base_path, '../../data/input/item_master_data.csv')

    output_path = os.path.join(base_path, '../../data/output')
    
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

    # 마스터 데이터 스키마 정의
    item_master_data_schema = StructType([
        StructField("item_code", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("category_name", StringType(), True)
    ])
    
    # CSV 파일을 읽어서 데이터프레임 생성
    item_sales_data_df = spark.read.schema(item_sales_data_schema).option("header", "true").csv(item_sales_data_path)
    item_master_data_df = spark.read.schema(item_master_data_schema).option("header", "true").csv(item_master_data_path)

    item_sales_data_df.show()
    item_master_data_df.show()

    # When: 판매 데이터를 처리하는 함수를 호출합니다.
    processed_item_sales_data_df = process_item_sales_data(spark, item_sales_data_df, item_master_data_df)
    sales_data_statistics_df = analyze_item_sales_data_statistics(processed_item_sales_data_df)
    
    # Then: 결과를 검증합니다.
    sales_data_statistics_df.explain()
    sales_data_statistics_df.show()

    (sales_data_statistics_df
        .coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(output_path))

    print(f"Results saved to {output_path}")
    # 테스트 종료 후 Spark 세션 정리
    spark.stop()

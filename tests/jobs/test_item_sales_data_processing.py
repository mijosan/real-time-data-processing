import os
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType, StringType, DoubleType, StructField

from src.jobs.item_sales_data_processing import analyze_item_sales_data_statistics, process_item_sales_data

def test_main():
    # 로컬 모드 SparkSession 생성
    spark = (
        SparkSession.builder
        .appName("Sales Data Test")
        .master("local[*]")
        .getOrCreate()
    )

    # 테스트용 CSV 경로
    base_path = os.path.dirname(os.path.abspath(__file__))
    item_sales_data_path = os.path.join(base_path, '../../data/input/item_sales_data.csv')
    item_master_data_path = os.path.join(base_path, '../../data/input/item_master_data.csv')
    output_path = os.path.join(base_path, '../../data/output')
    
    # 스키마 정의
    item_sales_data_schema = StructType([
        StructField("sales_date", StringType(), True),
        StructField("sales_time", StringType(), True),
        StructField("item_code", StringType(), True),
        StructField("sales_quantity", DoubleType(), True),
        StructField("sales_price", DoubleType(), True),
        StructField("sales_return", StringType(), True),
        StructField("is_discount", StringType(), True)
    ])

    item_master_data_schema = StructType([
        StructField("item_code", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("category_name", StringType(), True)
    ])
    
    # CSV 파일에서 DF 생성
    item_sales_data_df = (
        spark.read
        .schema(item_sales_data_schema)
        .option("header", "true")
        .csv(item_sales_data_path)
    )
    item_master_data_df = (
        spark.read
        .schema(item_master_data_schema)
        .option("header", "true")
        .csv(item_master_data_path)
    )

    # 비즈니스 로직 수행
    processed_item_sales_data_df = process_item_sales_data(spark, item_sales_data_df, item_master_data_df)
    sales_data_statistics_df = analyze_item_sales_data_statistics(processed_item_sales_data_df)

    # -------------------------
    # 검증 로직
    # -------------------------
    
    # 1. 결과 DataFrame이 비어있지 않은지 체크
    assert sales_data_statistics_df.count() > 0, "결과 DataFrame이 비어있습니다. 입력 데이터 혹은 로직을 확인하세요."
    
    # 2. 스키마 검증 - 필수 컬럼 및 타입 체크
    expected_columns = {
        "window_start": "TimestampType", 
        "window_end": "TimestampType",
        "item_code": "StringType",
        "item_name": "StringType",
        "category_code": "StringType",
        "category_name": "StringType",
        "total_sales_amount": "DoubleType",
        "total_sales_quantity": "DoubleType"
    }
    
    result_schema = {field.name: field.dataType.simpleString() for field in sales_data_statistics_df.schema.fields}
    for col_name, expected_type in expected_columns.items():
        assert col_name in result_schema, f"{col_name} 컬럼이 결과 DataFrame에 없습니다."
        assert expected_type.lower().replace("type","") in result_schema[col_name].lower(), f"{col_name} 컬럼의 타입이 {expected_type}이(가) 아닙니다. 현재: {result_schema[col_name]}"
    
    # 3. 특정 값 범위 체크 - total_sales_amount와 total_sales_quantity는 0 이상이어야 함
    invalid_rows_amount = sales_data_statistics_df.filter(col("total_sales_amount") < 0).count()
    invalid_rows_quantity = sales_data_statistics_df.filter(col("total_sales_quantity") < 0).count()
    assert invalid_rows_amount == 0, "total_sales_amount에 음수 값이 있습니다. 로직을 확인하세요."
    assert invalid_rows_quantity == 0, "total_sales_quantity에 음수 값이 있습니다. 로직을 확인하세요."
    
    # 4. item_code가 유니크하게 잘 집계되었는지 (비즈니스 로직상 문제 없는지)  
    # 예: 윈도우별, 아이템별로 집계되는 것이므로 같은 윈도우+아이템 조합이 중복 나타나지 않는지 검증
    distinct_count = sales_data_statistics_df.select("window_start", "window_end", "item_code").distinct().count()
    total_count = sales_data_statistics_df.count()
    assert distinct_count == total_count, "동일한 window_start, window_end, item_code 조합이 중복되었습니다."
    
    # 5. 결과 저장
    (
        sales_data_statistics_df
        .coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(output_path)
    )

    # 결과 출력 (디버깅용)
    sales_data_statistics_df.explain()
    sales_data_statistics_df.show()
    print(f"Results saved to {output_path}")

    spark.stop()

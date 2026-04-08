import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, sum, desc, to_date

def get_spark_session():
    """Initializes and returns a PySpark session."""
    return SparkSession.builder \
        .appName("Boticario_Sales_ETL") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def extract_data(spark, data_dir):
    """Loads all CSV files from the raw data directory into a single DataFrame."""
    print(f"Loading data from {data_dir}...")
    # The files are semicolon separated and have a header
    df = spark.read.option("header", "true") \
                   .option("sep", ";") \
                   .csv(os.path.join(data_dir, "*.csv"))
    return df

def transform_data(df):
    """Applies necessary transformations and type casting to the DataFrame."""
    print("Transforming data...")
    # Cast QTD_VENDA to integer and parse DATA_VENDA to Date format
    df_transformed = df.withColumn("QTD_VENDA", col("QTD_VENDA").cast("integer")) \
                       .withColumn("DATA_VENDA", to_date(col("DATA_VENDA"), "dd/MM/yyyy"))
    
    # Extract YEAR and MONTH for easier aggregation
    df_transformed = df_transformed.withColumn("ANO", year(col("DATA_VENDA"))) \
                                   .withColumn("MES", month(col("DATA_VENDA")))
    return df_transformed

def create_consolidated_views(df_transformed, output_dir):
    """Creates the 4 specified aggregate tables and saves them as Parquet."""
    print("Creating consolidated views...")
    
    # a. Tabela1: Consolidado de vendas por ano e mês.
    table1 = df_transformed.groupBy("ANO", "MES") \
                           .agg(sum("QTD_VENDA").alias("TOTAL_VENDAS")) \
                           .orderBy("ANO", "MES")
    table1.write.mode("overwrite").parquet(os.path.join(output_dir, "tabela1_vendas_ano_mes"))
    
    # b. Tabela2: Consolidado de vendas por marca e linha.
    table2 = df_transformed.groupBy("MARCA", "LINHA") \
                           .agg(sum("QTD_VENDA").alias("TOTAL_VENDAS")) \
                           .orderBy("MARCA", "LINHA")
    table2.write.mode("overwrite").parquet(os.path.join(output_dir, "tabela2_vendas_marca_linha"))
                           
    # c. Tabela3: Consolidado de vendas por marca, ano e mês.
    table3 = df_transformed.groupBy("MARCA", "ANO", "MES") \
                           .agg(sum("QTD_VENDA").alias("TOTAL_VENDAS")) \
                           .orderBy("MARCA", "ANO", "MES")
    table3.write.mode("overwrite").parquet(os.path.join(output_dir, "tabela3_vendas_marca_ano_mes"))
    
    # d. Tabela4: Consolidado de vendas por linha, ano e mês.
    table4 = df_transformed.groupBy("LINHA", "ANO", "MES") \
                           .agg(sum("QTD_VENDA").alias("TOTAL_VENDAS")) \
                           .orderBy("LINHA", "ANO", "MES")
    table4.write.mode("overwrite").parquet(os.path.join(output_dir, "tabela4_vendas_linha_ano_mes"))
    
    print("Views saved successfully.")
    return table4

def find_top_line_dec_2019(table4):
    """Finds the line with the most sales in month 12 of 2019 according to item 2.d."""
    print("Extracting top selling line for 12/2019...")
    top_linha_df = table4.filter((col("ANO") == 2019) & (col("MES") == 12)) \
                         .orderBy(desc("TOTAL_VENDAS")) \
                         .limit(1)
    
    top_linha_row = top_linha_df.collect()
    
    if top_linha_row:
        linha_name = top_linha_row[0]['LINHA']
        sales = top_linha_row[0]['TOTAL_VENDAS']
        print(f"--> The top selling line in 12/2019 was: {linha_name} with {sales} sales.")
        return linha_name
    else:
        print("No sales found for 12/2019!")
        return None

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    silver_data_dir = os.path.join(base_dir, "data", "silver")
    
    spark = get_spark_session()
    
    try:
        raw_df = extract_data(spark, raw_data_dir)
        clean_df = transform_data(raw_df)
        
        # Cache to speed up the multiple aggregations
        clean_df.cache()
        
        # Create and save tables
        table4 = create_consolidated_views(clean_df, silver_data_dir)
        
        # Determine the target for the Twitter scraper
        top_line = find_top_line_dec_2019(table4)
        
        # Save the top line to a text file so the twitter scraper can read it easily
        if top_line:
            with open(os.path.join(base_dir, "data", "top_line.txt"), "w") as f:
                f.write(top_line)
                
    finally:
        spark.stop()
        print("ETL Job Finished.")

if __name__ == "__main__":
    main()

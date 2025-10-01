import os
import sys
import time
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def main():
    # Chargement de l'environnement
    load_dotenv()

    # Initialisation de Spark
    spark = SparkSession.builder \
        .appName("TicketCountByType") \
        .config("spark.jars", "/var/tmp/jars/mysql-connector-j-8.3.0.jar") \
        .master("local[2]") \
        .getOrCreate()

    poll_interval = 30
    max_retries = 10
    retry_delay = 10

    while True:
        success = False
        attempt = 0

        while not success and attempt < max_retries:
            try:
                df = spark.read \
                    .format("jdbc") \
                    .option("driver","com.mysql.cj.jdbc.Driver") \
                    .option("url", f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}") \
                    .option("dbtable", f"(SELECT request_type FROM {os.getenv('MYSQL_DATABASE')}) AS tmp_tickets") \
                    .option("user", f"{os.getenv('MYSQL_USER')}") \
                    .option("password", f"{os.getenv('MYSQL_PASSWORD')}") \
                    .load()
                
                if df.count() > 0:
                    # Comptage des tickets par type
                    count_df = df.groupBy("request_type").count()
                    count_df.write.mode("overwrite").json(os.getenv('JSON_PATH'))
                success = True
            except Exception as e:
                attempt += 1
                print(f"[Erreur] Tentative {attempt}/{max_retries} : {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    print("Échec après plusieurs tentatives. Attente avant relance...")
                    time.sleep(poll_interval)

        time.sleep(poll_interval)

if __name__ == '__main__':
    sys.exit(main())
from src.bronze import Bronze
from src.silver import Silver
from src.gold import Gold
from src.sparkInit import start_spark


def main():
    spark = start_spark()
    Bronze.process(spark)
    Silver.process(spark)
    Gold.process(spark)
    spark.stop()


if __name__ == "__main__":
    main()

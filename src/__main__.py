from src.bronze import Bronze
from src.sparkInit import start_spark


def main():
    spark = start_spark()
    Bronze.process(spark)


if __name__ == "__main__":
    main()

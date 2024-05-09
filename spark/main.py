from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace, length
import sys

def count_words(file_path, word_length):
    spark = SparkSession.builder \
        .appName("WordCount") \
        .getOrCreate()
    df = spark.read.text(file_path)
    words = df.select(explode(split(regexp_replace(lower(df.value), "[^a-zA-Z0-9\\s]", ""), "\\s+")).alias("slowo")) \
              .filter(length("slowo") >= word_length)
    word_counts = words.groupBy("slowo").count()
    sorted_word_counts = word_counts.orderBy("count", ascending=False)
    sorted_word_counts.show(sorted_word_counts.count(), False)
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main.py <file_path> <min_word_length>")
        sys.exit(1)

    file_path = sys.argv[1]
    word_length = int(sys.argv[2])

    count_words(file_path, word_length)

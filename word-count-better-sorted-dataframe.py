from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///SparkCourse/book.txt")

# Passing columns as parameters

# Split using a regular expression that extracts words
# value is a default name of the column created for inputDF, "\\W+" splits text into words 
# func.explode() similar to flatmap; explodes columns into rows
# alias renames the column
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
# filter out empty words 
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results. We want to show all results not 20 (by default)
wordCountsSorted.show(wordCountsSorted.count())

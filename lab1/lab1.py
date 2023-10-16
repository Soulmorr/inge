#Імпортування бібліотек
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower,col, regexp_replace, explode, split, concat_ws

# Створення SparkSession
spark = SparkSession.builder \
    .appName("CommitAnalysis") \
    .getOrCreate()

# Завантаження вхідних даних
df = spark.read.json("10K.github.jsonl")

# Фільтрація даних для подій типу "PushEvent"
filtered_df = df.filter(df.type == "PushEvent")

# Трансформація даних
transformed_df = filtered_df.select(lower(col("actor.login")).alias("author"),
                                    explode("payload.commits").alias("commits")) \
                            .select("author", lower(regexp_replace(col("commits.message"), "[^a-zA-Z\\s]", "")).alias("message")) \
                            .withColumn("words", split(col("message"), " ")) \
                            .withColumn("ngrams", concat_ws(" ", col("words"), col("words").getItem(1), col("words").getItem(2))) \
                            .select("author", "ngrams") \
                            .limit(5)

# Збирання результатів у словник Python
results = transformed_df.collect()
output_dict = {}
for row in results:
    if row.author not in output_dict:
        output_dict[row.author] = []
    output_dict[row.author].append(row.ngrams)

# Збереження результатів у CSV-файл
with open('output.csv', 'w') as file:
    for author, ngrams in output_dict.items():
        file.write(f"(Author: {author}, Commit message: {' '.join(ngrams[0].split(' ')[:3])}) -> {{\n")
        for ngram in ngrams:
            ngram_list = ngram.split(' ')
            file.write(f"{ngram_list[0]}, {ngram_list[0]} {ngram_list[1]} {ngram_list[2]}, {ngram_list[1]} {ngram_list[2]} {ngram_list[3]}\n")
        file.write("}\n")

# Зупинити SparkSession
spark.stop()

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys

# should return a pyspark.sql.DataFrame
def cruncher(spark, datadir, a1, a2, a3, a4, d1, d2):
    person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/person*.csv*")
    interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/interest*.csv*")
    knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").load(datadir + "/knows*.csv*")

    # find p1 candidates
    p1 = interest \
        .where(col("interest") == a1) \
        .select(col("personId")) \
        .join(person, on="personId") \
        .withColumn("bday", month(col("birthday"))*100 + dayofmonth(col("birthday"))) \
        .filter((d1 <= col("bday")) & (col("bday") <= d2))

    # precompute scores of p2/p3 and filter 
    scores = interest \
        .where(col("interest").isin(a2, a3, a4)) \
        .groupBy("personId") \
        .agg(count("interest").alias("score")) \
        .filter(col("score") >= 2) \
        .join(interest.filter(col("interest") == a1), on="personId", how="left_anti") \
        .select("personId", "score")

    # to avoid confusion with person.csv's header and with p1/p2/3,
    # we rename the header of knows to pA and pB (person A and person B)
    same_city_knows = knows \
        .select(col("personId").alias("pA"), col("friendId").alias("pB")) \
        .join(person, on=(col("pA") == col("personId"))) \
        .select("pA", "pB", col("locatedIn").alias("pAcity")) \
        .join(person, on=(col("pB") == col("personId"))) \
        .select("pA", "pB", "pAcity", col("locatedIn").alias("pBcity")) \
        .filter(col("pAcity") == col("pBcity"))

    # find p1-p2 candidates
    p1p2 = p1 \
        .join(same_city_knows, col("personId") == col("pA")) \
        .select(col("pA").alias("p1"), col("pB").alias("p2")) \
        .join(scores, col("p2") == col("personId")) \
        .select("p1", "p2", col("score").alias("p2score"))

    # find p1-p2-p3 triangles
    p1p2p3 = p1p2 \
        .join(same_city_knows, (col("p2") == col("pA"))) \
        .select("p1", "p2", "p2score", col("pB").alias("p3")) \
        .filter(col("p2") < col("p3")) \
        .join(scores, col("p3") == col("personId")) \
        .select("p1", "p2", "p3", "p2score", col("score").alias("p3score")) \
        .join(same_city_knows, (col("p1") == col("pA")) & (col("p3") == col("pB")))

    # sort to produce final results
    result = p1p2p3 \
        .selectExpr("p2score + p3score AS score", "p1", "p2", "p3") \
        .orderBy(desc("score"), asc("p1"), asc("p2"), asc("p3"))

    return result


def main():
    if len(sys.argv) < 4:
        print("Usage: cruncher.py [datadir] [query file] [results file]")
        sys.exit()

    datadir = sys.argv[1]
    query_file_path = sys.argv[2]
    results_file_path = sys.argv[3]

    spark = SparkSession.builder.getOrCreate()

    query_file = open(query_file_path, 'r')
    results_file = open(results_file_path, 'w')
        
    for line in query_file.readlines():
        q = line.strip().split("|")

        # parse rows like: 1|1989|1990|5183|1749|2015-04-09|2015-05-09
        qid = int(q[0])
        a1 = int(q[1])
        a2 = int(q[2])
        a3 = int(q[3])
        a4 = int(q[4])
        d1 = 100*(int(q[5][5:7])) + int(q[5][8:10])
        d2 = 100*(int(q[6][5:7])) + int(q[6][8:10])
        
        print(f"Processing Q{qid}")
        result = cruncher(spark, datadir, a1, a2, a3, a4, d1, d2)
        
        # write rows like: Query.id|TotalScore|P1|P2|P3
        for row in result.collect():
            results_file.write(f"{qid}|{row[0]}|{row[1]}|{row[2]}|{row[3]}\n")

    query_file.close()
    results_file.close()


if __name__ == "__main__":
    main()

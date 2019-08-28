from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf, coalesce, lit, col, count
from pyspark.sql.functions import *
from pyspark.ml.feature import CountVectorizer

import re
import string
import argparse
import sys
import json
import functools
from itertools import tee

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from sanitize import sanitize


def main(context):
    """Main function takes a Spark SQL context."""

    opinions = sqlContext.read.json("training_data.json")
    labels = sqlContext.read.json("combined_labels.json")
    opinions.write.parquet("data.parq")
    labels.write.parquet("labels.parq")
    #submissions.write.parquet("submissions.parq")
    #labeled_data = sqlContext.read.csv("labeled_data.csv")
    #labeled_data.write.parquet("labeled_data.parq")
    return
    #comments = sqlContext.read.parquet("comments.parq")
    #submissions = sqlContext.read.parquet("submissions.parq")
    #labeled_data = sqlContext.read.parquet("labeled_data.parq")
    
    df = labeled_data.join(comments, labeled_data._c0==comments.id, 'inner').select("_c3", "body")
    # df.show()

    clean_udf = udf(lambda z: sanitize(z), ArrayType(StringType()))
    df_cleaned = df.select("_c3", clean_udf("body").alias("features"))
    # df_cleaned.show()

    cv = CountVectorizer(inputCol="features", outputCol="vectors", minDF=10, binary=True)
    model = cv.fit(df_cleaned)
    df_transformed = model.transform(df_cleaned)

    """
    df_transformed.show()
    
    pos_col = coalesce((col("_c3") == 1).cast("int"), lit(0))
    neg_col = coalesce((col("_c3") == -1).cast("int"), lit(0))
    df_new_columns = df_transformed.withColumn("pos", pos_col).withColumn("neg", neg_col)
    #df_new_columns.show()
    
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()#rawPredictionCol
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
        estimator=poslr,
        evaluator=posEvaluator,
        estimatorParamMaps=posParamGrid,
        numFolds=5)
    negCrossval = CrossValidator(
        estimator=neglr,
        evaluator=negEvaluator,
        estimatorParamMaps=negParamGrid,
        numFolds=5)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    pos = df_new_columns.select("vectors", "pos").selectExpr("vectors as features", "pos as label")
    neg = df_new_columns.select("vectors", "neg").selectExpr("vectors as features", "neg as label")
    posTrain, posTest = pos.randomSplit([0.5, 0.5])
    negTrain, negTest = neg.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)

    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.save("pos.model")
    negModel.save("neg.model")
    """

    posModel = CrossValidatorModel.load("pos.model")
    negModel = CrossValidatorModel.load("neg.model")

    comments = comments.sample(withReplacement=False, fraction=.1, seed=3)

    udf1 = udf(lambda x: x[3:], StringType())
    df_1 = comments.select("id", "body", "created_utc", "author_flair_text", "score",
                           udf1("link_id").alias("link_id"))
    df_int = df_1.select('*').where(~ col('body').like("%/s%") & ~ col('body').like("%&gt:%"))
    df_t = submissions.select("title", "id", "score").selectExpr("title as submissions_title",
                                                                 "id as submissions_id",
                                                                 "score as submission_score")
    df_2 = df_int.join(df_t, df_int.link_id == df_t.submissions_id, 'inner').select("id", "link_id",
                                                                                    clean_udf("body").alias("features"),
                                                                                    "created_utc",
                                                                                    "author_flair_text",
                                                                                    "submissions_title",
                                                                                    "score",
                                                                                    "submission_score")

    df_final = model.transform(df_2)
    df_final2 = df_final.withColumnRenamed("features", "words").withColumnRenamed("vectors", "features")

    posResult = posModel.transform(df_final2)
    posResult = posResult.select("id", "created_utc", "author_flair_text", "link_id",
                                 "submissions_title", col("probability").alias("pos_probs"), "score",
                                 "submission_score", "features")
    allResults = negModel.transform(posResult)

    all_final = allResults.select("id", "created_utc", "author_flair_text", "link_id",
                                  "submissions_title", "pos_probs", col("probability").alias("neg_probs"), "score",
                                  "submission_score")

    udf_pos = udf(lambda x: 1 if x[1] > .2 else 0, IntegerType())
    penult = all_final.withColumn("my_pos_label", udf_pos(all_final["pos_probs"]))
    udf_neg = udf(lambda x: 1 if x[1] > .25 else 0, IntegerType())
    ult = penult.withColumn("my_neg_label", udf_neg(penult["neg_probs"]))

    all_results = ult.select("id", "created_utc", "author_flair_text", "link_id",
                             "submissions_title", "my_pos_label", "my_neg_label",
                             "score", "submission_score")
    print(all_results.dtypes)

    # posResult = posModel.transform(df_final2)
    # negResult = negModel.transform(df_final2)
    # # posResult.show()
    # # negResult.show()
    #
    # udf_pos = udf(lambda x: 1 if x[1] > .2 else 0, IntegerType())
    # pos_penult = posResult.withColumn("my_pos_label", udf_pos(posResult["probability"]))
    # # pos_result_col = coalesce((col("prob_ind_1") > .2).cast("int"), lit(0))
    # # pos_final = pos_penult.withColumn("my_pos_label", pos_result_col)
    # pos_show = pos_penult.select("id", "created_utc", "author_flair_text",
    #                              "submissions_title", "my_pos_label", "score",
    #                              "submission_score")
    #
    # udf_neg = udf(lambda x: 1 if x[1] > .25 else 0, IntegerType())
    # neg_penult = negResult.withColumn("my_neg_label", udf_neg(negResult["probability"]))
    # # neg_result_col = coalesce((col("prob_ind_1") > .25).cast("int"), lit(0))
    # # neg_final = neg_penult.withColumn("my_neg_label", neg_result_col)
    # neg_show = neg_penult.select("id", "my_neg_label").selectExpr("id as neg_id", "my_neg_label as my_neg_label")
    #
    # all_results = pos_show.join(neg_show, pos_show.id == neg_show.neg_id, 'inner')
    # # all_results.show()
    #
    # all_results = all_results.select("id", "created_utc", "author_flair_text",
    #                                  "submissions_title", "my_pos_label", "my_neg_label",
    #                                  "score", "submission_score")
    # print(all_results.dtypes)

    # task 10
    all_results.createOrReplaceTempView("all_results")

    all_results.show()

    # answer_1 = context.sql("""SELECT submissions_title as title, SUM(my_pos_label) / COUNT(*) as percent_pos,
    # SUM(my_neg_label) / COUNT(*) as percent_neg
    # FROM all_results
    # GROUP BY submissions_title
    # """)
    #answer_1 = all_results.groupBy("link_id", "submissions_title").agg(sum("my_pos_label") / count("my_pos_label"),
    #                                                                   sum("my_neg_label") / count("my_neg_label"))
    answer_1 = context.sql("select sum(my_pos_label) / count(my_pos_label) from all_results GROUP BY link_id, submissions_title")
    answer_1.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(
        "answer_1.csv")

    answer_2 = context.sql("""SELECT FROM_UNIXTIME(created_utc, '%D %M %Y') as date, SUM(my_pos_label) / COUNT(*) as percent_pos,
        SUM(my_neg_label) / COUNT(*) as percent_neg
    FROM all_results
    GROUP BY FROM_UNIXTIME(created_utc, '%D %M %Y')""")
    answer_2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(
        "answer_2.csv")

    answer_3 = context.sql("""SELECT author_flair_text as state, SUM(my_pos_label) / COUNT(*) as percent_pos,
        SUM(my_neg_label) / COUNT(*) as percent_neg
    FROM all_results
    GROUP BY author_flair_text""")
    answer_3.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(
        "answer_3.csv")

    # NOTE : apparently I need to be careful of quotes and use a certain flag...
    answer_4_pt1 = context.sql("""SELECT score as comment_score, SUM(my_pos_label) / COUNT(*) as percent_pos,
        SUM(my_neg_label) / COUNT(*) as percent_neg
    FROM all_results
    GROUP BY score""")
    answer_4_pt1.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(
        "answer_4_pt1.csv")

    answer_4_pt2 = context.sql("""SELECT submission_score as story_score, SUM(my_pos_label) / COUNT(*) as percent_pos,
        SUM(my_neg_label) / COUNT(*) as percent_neg
    FROM all_results
    GROUP BY submission_score""")
    answer_4_pt2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(
        "answer_4_pt2.csv")


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import logging
from pyspark.sql import Window
from datetime import datetime

#import configparser
#import os


def spark_session():
    """
    It creates and returns default spark session.
    """
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.ui.enabled", 'true') \
        .config("spark.ui.port", "4444") \
        .getOrCreate()
    return spark


def create_df(spark, input_path, file_name):
    """
    This function reads data from a given path and returns a spark dataframe.

    :param spark: spark session
    :param input_path: source file path
    :param file_name: source file name
    """

    full_path = input_path + '/' + file_name
    df = spark.read.json(full_path)
    return df


def write_as_files(df, target_path, target_file_name, partition_column=None):
    """
    This function writes a df's data partitioning to a given location.

    :param df: spark dataframe
    :param target_path: target file path
    :param target_file_name: target file name
    :param partition_column: partition columns array
    """
    full_path = target_path + '/' + target_file_name
    print("our full path {}".format(full_path))
    if partition_column:
        df.write.partitionBy(*partition_column).json(full_path, mode='overwrite')
    else:
        df.write.json(full_path, mode='overwrite')


def convert_json(s):
    """
    This function takes a json string and turns it an actual json object.
    :param s: json string
    """
    if s and s != 'None':
        return eval(s)
    else:
        return None


# datetime object containing current date and time
currentYear = datetime.now().year

input_path = sys.argv[1]
output_path = sys.argv[2]


# input_path = "/home/gizem/Desktop/kod/__DATA__/input_data/yelp"
# output_path = "/home/gizem/Desktop/kod/__DATA__/output_data/yelp_datalake"

# config = configparser.ConfigParser()
# config.read('/tmp/aws.cfg')
#
# os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def cleaning_business(business_df):
    # convert a python function to udf with a specific schema
    json_udf = udf(lambda y: convert_json(y), StructType(
        [StructField("romantic", BooleanType()),
         StructField("intimate", BooleanType()),
         StructField("classy", BooleanType()),
         StructField("hipster", BooleanType()),
         StructField("divey", BooleanType()),
         StructField("touristy", BooleanType()),
         StructField("trendy", BooleanType()),
         StructField("upscale", BooleanType()),
         StructField("casual", BooleanType())]))

    # convert regular python function to spark udf.
    convert_str = udf(lambda z: convert_json(z), StringType())

    # cleaning business data and dropping unnecessary columns
    b_df = business_df \
        .withColumn("alcohol",
                    F.when(convert_str(business_df.attributes["Alcohol"]) == 'none', None).
                    otherwise(convert_str(business_df.attributes["Alcohol"]))) \
        .withColumn("ambience",
                    F.when(json_udf(business_df.attributes["Ambience"])["romantic"] == True, "romantic")
                    .when(json_udf(business_df.attributes["Ambience"])["classy"] == True, "classy")
                    .when(json_udf(business_df.attributes["Ambience"])["hipster"] == True, "hipster")
                    .when(json_udf(business_df.attributes["Ambience"])["touristy"] == True, "touristy")
                    .when(json_udf(business_df.attributes["Ambience"])["casual"] == True, "casual")
                    .when(json_udf(business_df.attributes["Ambience"])["intimate"] == True, "intimate")
                    .when(json_udf(business_df.attributes["Ambience"])["divey"] == True, "divey")
                    .when(json_udf(business_df.attributes["Ambience"])["trendy"] == True, "trendy")
                    .when(json_udf(business_df.attributes["Ambience"])["upscale"] == True, "upscale")
                    .otherwise("N/A")) \
        .withColumn("take_credit_cards", business_df.attributes["BusinessAcceptsCreditCards"]) \
        .withColumn("good_for_kids", business_df.attributes["GoodForKids"]) \
        .withColumn("happy_hour", business_df.attributes["HappyHour"]) \
        .withColumn("restaurants_takeout", business_df.attributes["RestaurantsTakeOut"]) \
        .withColumn("smoking", business_df.attributes["Smoking"]) \
        .withColumn("wifi", convert_str(business_df.attributes["WiFi"])) \
        .withColumn("restaurant_category", F.when(business_df["categories"].like('%American%'), "American").
                    when(business_df["categories"].like('%Mexican%'), "Mexican").
                    when(business_df["categories"].like('%Italian%'), "Italian").
                    when(business_df["categories"].like('%Canadian%'), "Canadian").
                    when(business_df["categories"].like('%Japanese%'), "Japanese").
                    when(business_df["categories"].like('%Indian%'), "Indian").
                    when(business_df["categories"].like('%Chinese%'), "Chinese").
                    when(business_df["categories"].like('%Middle Eastern%'), "Middle Eastern")
                    .otherwise(None)) \
        .withColumnRenamed("stars", "business_stars") \
        .drop("attributes", "hours", "categories", "latitude", "longitude", "postal_code",
              "address", "name", "city")
    return b_df


def state_dimension(business_df):
    # state dimension
    states = business_df.select(business_df.state).distinct()
    state_dim = states.withColumn("state_id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    return state_dim


def category_dimension(b_df):
    # category dimension
    categories = b_df.select(b_df.restaurant_category).filter(b_df.restaurant_category.isNotNull()).distinct()
    category_dim = categories.withColumn("category_id",
                                         F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    return category_dim


def ambience_dimension(b_df):
    # ambience dimension
    ambience = b_df.select(b_df.ambience).distinct().filter(b_df.ambience != 'N/A')
    ambience_dim = ambience.withColumn("ambience_id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

    return ambience_dim


def user_dimension(user_df):
    # cleaning user data and droping unnecessary columns

    user_dim = user_df.withColumn("user_friend_count", F.size(F.split(user_df.friends, ','))) \
        .withColumn('year', F.year(user_df.yelping_since)) \
        .withColumn('month', F.month(user_df.yelping_since)) \
        .withColumnRenamed("fans", "user_fans") \
        .withColumnRenamed("review_count", "user_review_count") \
        .withColumnRenamed("useful", "useful_vote_cnt") \
        .withColumnRenamed("funny", "funny_vote_cnt") \
        .withColumnRenamed("cool", "cool_vote_cnt") \
        .withColumnRenamed("useful", "useful_vote_cnt") \
        .withColumnRenamed("average_stars", "user_average_stars") \
        .withColumn("user_elite_year_cnt", F.when(F.length(user_df.elite) == 0, 0)
                    .otherwise(F.size(F.split(user_df.elite, ',')))) \
        .withColumn("is_user_elite", F.when(F.substring_index(user_df.elite, ',', -1) == currentYear - 1, True)
                    .otherwise(False)) \
        .select("user_id", "name", "user_review_count", "yelping_since", "user_friend_count",
                "useful_vote_cnt", "funny_vote_cnt", "cool_vote_cnt",
                "user_average_stars", "user_elite_year_cnt", "is_user_elite",
                "user_fans", "year", "month")
    return user_dim


def business_reviews(category_dim, ambience_dim, state_dim, reviews_df, b_df):
    ####  preparing fact data

    # finding ids for attributes in business data
    business_w_ids = b_df.join(category_dim, b_df.restaurant_category == category_dim.restaurant_category,
                               how='left') \
        .join(ambience_dim, b_df.ambience == ambience_dim.ambience, how='left') \
        .join(state_dim, state_dim.state == b_df.state) \
        .drop("restaurant_category", "ambience", "state")

    # cleaning review data
    reviews = reviews_df.withColumn('year', F.year(reviews_df.date)) \
        .withColumn('month', F.month(reviews_df.date)) \
        .withColumnRenamed("stars", "review_stars") \
        .withColumnRenamed("cool", "review_cool") \
        .withColumnRenamed("funny", "review_funny") \
        .withColumnRenamed("useful", "review_useful") \
        .drop("text", "date")

    # combining cleaned review and business data
    business_reviews_fact = business_w_ids.join(reviews, reviews.business_id == business_w_ids.business_id) \
        .drop(reviews.business_id)
    return business_reviews_fact


def main(task_name):
    # creating spark session
    spark = spark_session()

    business_df = create_df(spark, input_path, 'business.json').repartition(13)

    b_df = cleaning_business(business_df)

    if task_name == "state_dim":
        state_dim = state_dimension(business_df)
        write_as_files(state_dim, output_path, "state.json")

    elif task_name == "category_dim":
        category_dim = category_dimension(b_df)
        write_as_files(category_dim, output_path, "category.json")

    elif task_name == "ambience_dim":
        new_df= b_df.filter(b_df.ambience !="N/A")
        ambience_dim = ambience_dimension(new_df)

        write_as_files(ambience_dim, output_path, "ambience.json")

    elif task_name == "user_dim":
        # reading user data
        user_df = create_df(spark, input_path, 'user.json').repartition(7)
        user_dim = user_dimension(user_df)
        user_dim = user_dim.withColumn('yearp',user_dim.year).withColumn('monthp',user_dim.month)
        write_as_files(user_dim, output_path, 'user.json', ["yearp"])

    elif task_name == "business_reviews_fact":
        category_dim = category_dimension(b_df)
        ambience_dim = ambience_dimension(b_df)
        state_dim = state_dimension(business_df)

        reviews_df = create_df(spark, input_path, 'review.json').repartition(7)

        business_reviews_fact = business_reviews(category_dim, ambience_dim, state_dim, reviews_df, b_df)
        business_reviews_fact= business_reviews_fact.withColumn('yearp',business_reviews_fact.year)
        write_as_files(business_reviews_fact, output_path, 'business_reviews.json', ["yearp"])


if __name__ == "__main__":
    print("helloooooo")
    main(sys.argv[3])

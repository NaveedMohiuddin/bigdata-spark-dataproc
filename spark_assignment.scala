
// Import Spark libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Initialize SparkSession
val spark = SparkSession.builder.appName("FoodRatings").getOrCreate()

// Define Schema for foodratings
val foodratingsSchema = StructType(Array(
    StructField("name", StringType, true),
    StructField("food1", IntegerType, true),
    StructField("food2", IntegerType, true),
    StructField("food3", IntegerType, true),
    StructField("food4", IntegerType, true),
    StructField("placeid", IntegerType, true)
))

// Load foodratings CSV file
val foodratings = {
    spark.read
        .option("header", "false")
        .schema(foodratingsSchema)
        .csv("hdfs:///user/hadoop/foodratings105319.csv")
}

// Define Schema for foodplaces
val foodplacesSchema = StructType(Array(
    StructField("placeid", IntegerType, true),
    StructField("placename", StringType, true)
))

// Load foodplaces CSV file
val foodplaces = {
    spark.read
        .option("header", "false")
        .schema(foodplacesSchema)
        .csv("hdfs:///user/hadoop/foodplaces105319.csv")
}

// Register as SQL Tables
foodratings.createOrReplaceTempView("foodratingsT")
foodplaces.createOrReplaceTempView("foodplacesT")

// Query foodratings where food2 < 25 and food4 > 40
val foodratings_ex3a = spark.sql("SELECT * FROM foodratingsT WHERE (food2 < 25) AND (food4 > 40)")
foodratings_ex3a.printSchema()
foodratings_ex3a.show(5)

// Query foodplaces where placeid > 3
val foodplaces_ex3b = spark.sql("SELECT * FROM foodplacesT WHERE placeid > 3")
foodplaces_ex3b.printSchema()
foodplaces_ex3b.show(5)

// Filter foodratings where name = "Mel" and food3 < 25
val foodratings_ex4 = foodratings.filter(col("name") === "Mel" && col("food3") < 25)
foodratings_ex4.printSchema()
foodratings_ex4.show(5)

// Select only 'name' and 'placeid' columns
val foodratings_ex5 = foodratings.select("name", "placeid")
foodratings_ex5.printSchema()
foodratings_ex5.show(5)

// Perform inner join on placeid
val ex6 = foodratings.join(foodplaces, "placeid")
ex6.printSchema()
ex6.show(5)

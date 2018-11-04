import org.apache.spark.sql.SparkSession

val spar = SparkSession.builder().getOrCreate()

val df = spar.read.option("header","true").option("inferSchema","true")csv("GitiGroup2006_2008.csv")//crisis financiera

//First 5 rows
df.head(5)

for(row <- df.head(5)){
  println(row)
}

//columns
df.columns
df.describe().show()

//select a columns
df.select("Volume").show()

//sekect multiple columns
df.select($"Date", $"Close").show()

//create new columns
val df2 = df.withColumn("HighPlusLow", df("High")+df("Low"))

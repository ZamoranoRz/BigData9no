import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true")csv("CitiGroup2006_2008.csv")

df.printSchema()
// devuelve un array con las dos primeras filas
df.head(2)
// devuelve un dataframe con las dos primeras filas
df.show(2)
//Extrae el mes como un entero de una fecha / indicación de tiempo / cadena determinada.
df.select(month(df("Date"))).show()
//Extrae el año como un número entero de una fecha
df.select(year(df("Date"))).show()
df.filter($"Date" === 2006).show()

val df2 = df.withColumn("Year", year(df("Date")))

val dfavgs = df2.groupBy("Year").mean()

dfavgs.select($"Year", $"avg(Close)").show()

val dfmins = df2.groupBy("Year").min()

dfmins.select($"Year", $"min(Close)").show()

val df4 = df2.withColumn("Current Date", current_date).show()

from_unixtime(unix_timestamp())

import org.apache.spark.sql.SparkSession
val spar = SparkSession.builder().getOrCreate()
val df = spar.read.option("header","true").option("inferSchema","true")csv("Netflix_2011_2016.csv")

//Cuales son los nombres de las columnas
df.columns
//Como es el esquema?
df.printSchema
//Imprime las primeras 5 columnas
df.select($"Date",$"Open", $"High", $"Low", $"Close").show()
//Usar describe para aprender sobre el DataFrame
df.describe().show()
//
val df2 = df.withColumn("HV Ratio", df("Volume")/df("High"))
df2.select($"HV Ratio").show()
//Que dia tuvo Peak High en Price?
df.filter("High > 716").show()
//Cual es el significado de la columna Close
// Es el precio de cierre.
//Cual es el maximo y minimo de la columna volumen
df.select(max("Volume"),min("Volume")).show()

//Con sintaxis scala/spark $ conteste los siguiente:
//Cuantos dias fue el cierre inferior a 600
val a = df.filter($"Close" < 600).count()
//Que procentaje del tiempo fue el alto mayor de 500
val b: Double = df.filter($"High" > 500).count()*100.0/df.count()
val c: Double = df.select(df("High")).count()
println(b/c*100)
//Correlacion de pearson
df.select(corr("High", "Volume")).show()
//Maximo alto por ano
val y = df.withColumn("Year",year(df("Date")))
val y2 = y.groupBy("Year").max()
y2.select($"Year", $"max(High)").show()

//Cual es el promedio de cierre para cada mes del calendario.
val m = df.withColumn("Month",month(df("Date")))
val m2 = m.groupBy("Month").mean()
m2.select($"Month",$"avg(Close)").orderBy("Month").show()

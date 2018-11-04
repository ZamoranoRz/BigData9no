// primero que nada.
import org.apache.spark.sql.SparkSession
val spar = SparkSession.builder().getOrCreate()
val df = spar.read.option("header","true").option("inferSchema","true")csv("FL_insurance_sample.csv")
//despues de nada

// obtener filas sin header
df.head(5)

//Schema
df.printSchema

//imprimir filas con header
df.show(1)

//Seleccion de columna de huracan
df.select("hu_site_deductible").show()

// se filtra el que paga mas de determinada cantidad de deducible en huracanes. wuju!!
df.filter("hu_site_deductible > 7000000").show()

//esta columna es de huracan con alias de HSD
df("hu_site_deductible").as("HSD")

//creo una nueva columnas que contien
val df2 = df.withColumn("Total_deducible", df("hu_site_deductible")+df("eq_site_deductible")+df("fl_site_deductible")+df("fr_site_deductible"))

df2.select(df2("Total_deducible").as("TD")).show()
df.filter("fl_site_deductible > 1").show()

//describe de df
df.describe().show()
// describe de determinadas columnas.
df.describe("hu_site_limit", "hu_site_deductible").show()
// Buscar solo lo que quieres establecer

df.select([mean('hu_site_deductible'), min('hu_site_deductible'), max('hu_site_deductible')]).show()

//Todas las casas que estan construidas de madera.
df.filter($"construction".equalTo("Wood")).show()
//  casas construidas de madera.
df.filter($"contruction" === "Wood").show()
// correlacion entre el deducible de huracanes y el limite
df.select(corr("hu_site_limit", "hu_site_deductible")).show()
df.select(corr(df("hu_site_limit"), df("hu_site_deductible"))).show()
//obtenemos las primeras 5 filas, se representa de una mejor manera que poniendo "df.head(5)", pero da el
//mismo resultado.
for(row <- df.head(5)){
    println(row)
}
//te muestra un array con los nombres de las columnas.
df.columns
// recolecta los datoss que queremos filtrar en una matriz en lugar de en un dataframe
val a = df.filter($"hu_site_limit" < 10000 && $"hu_site_deductible" < 10000).collect()
df.filter("hu_site_deductible > 7000000").show()
// Regresa el numero de renglones que son filtrados.
val a = df.filter($"hu_site_limit" < 10000 && $"hu_site_deductible" < 10000).count()

// agrupar datos por point_latitude, asi miramos cuantas cosas coinciden con el mismo punto.
// RE: 30.406355|    8|
df.groupBy("point_latitude").count().show()

//Registre el DataFrame como una vista temporal de SQL
df.createOrReplaceTempView("FL_insurance_sample.csv")
val sqlDF = spark.sql("SELECT contruction FROM FL_insurance_sample")
sqlDF.show()
//devuelve el promedio de los valores en un grupo.
df.select(avg("hu_site_limit")).show()
//devuelve el número aproximado de elementos distintos en un grupo.
df.select(approx_count_distinct("construction")).show()
//devuelve una lista con objetos duplicados "funciona con String"
df.select(collect_list("statecode")).show()
//devuelve el número de elementos distintos en un grupo.
df.select(countDistinct("county")).show()
//Una expresión que devuelve la representación de cadena del valor binario de la columna larga dada.
df.select(bin("hu_site_limit")).show()

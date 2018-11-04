//a - importar linear regesion
import org.apache.spark.ml.regression.LinearRegression
//b- utilizar el siguiente codigo para el informe de errores
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

//c. comenzar session spark
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

//d. utilizar spark para leer el archivo csv clientes ecommerce
val df = spark.read.option("header","true").option("inferSchema","true").format("csv").load("EcommerceCustomers.csv")

//e. imprimir el esquema de dataframe
df.printSchema

//f. imprime una fila de ejemplo
df.show(1)

//g. limpieza de datos en dos columnas label y futures

//h. importar vectores ensambladores
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

//i  cambie el nombre de la columna Yearly amount Spend como label, tome solo las columnas numericas de los datos
//establecer todo en un nuevo dataframe
//cambiar nombre de una colimna con .as y despues crear un data frame con columnas especificas
val df2 = (df.select(df("Yearly Amount Spent").as("label"),$"Avg Session Length",$"Time on App",$"Time on Website",$"Length of Membership"))
df2.printSchema
df2.show()
//j.

//k. use VectorAssembler para convertir las columnas de entrada de df en una sola columna de salida de una matriz llamada features
val assembler = (new VectorAssembler().setInputCols(Array("Avg Session Length","Time on App","Time on Website","Length of Membership")).setOutputCol("features"))

//l.utilice ensablardor para transformas nuestro data frame en las dos columnas label y features
val output = assembler.transform(df2).select($"label",$"features")
output.printSchema
output.describe().show()


//m. Crear un objeto de modelo de regresion lineal
val lr = new LinearRegression().setMaxIter(450).setRegParam(0.3).setElasticNetParam(0.8)

//.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

//n. Ajuste el modelo a los datos y llame este modelo lrModel

val lrModel = lr.fit(output)

//o. imprimir los coeficioentes e interceptar para la regresion lineal
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

//p. Resume el modelo sobre el conjunto de entrenamiento e imprime algunas metricas, utilice el metodo .summary
//para crear un modelo trainingsumary.
val trainingSummary = lrModel.summary

//q. muestra los residuos el RMSE, el MSE, y los valores R^2
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"MSE: ${trainingSummary.meanSquaredError}")
println(s"r2: ${trainingSummary.r2}")

//importar un sesion spar (SparkSession)
import org.apache.spark.sql.SparkSession
//Utilice las lineas de codigo para reportar errores reducidos
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
//Crea una instancia de la sesion Spark
val spark = SparkSession.builder().getOrCreate()
//Importar la libreria de Kmeans para el algoritmo de agrupamiento.
import org.apache.spark.ml.clustering.KMeans
//Carga el dataset de wholesale customers data
val dataset = spark.read.option("header","true").option("inferSchema","true").csv("Wholesale customers data.csv")
dataset.show()
dataset.printSchema
//Seleccionar la siguientes columnas para conjunto de entrenamiento:
//Fresh,Milk,Grocery,Frozen,Detergents_Paper,Delicassen 
val features_data = dataset.select($"Fresh",$"Milk",$"Grocery",$"Frozen",$"Detergents_Paper",$"Delicassen")
//importar vectorAssembler y Vector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
//Crea un nuevo objeto VectorAssembler para las columnas de caracteristicas como un conjunto de entrada, recordando que no hay etiquetas (labels)
val assembler = new VectorAssembler().setInputCols(Array("Fresh","Milk","Grocery","Frozen","Detergents_Paper","Delicassen")).setOutputCol("features")
//Utilice el objeto assembler para transformar feature_data
val training = assembler.transform(features_data).select("features")
//Crea un modelo kmeans con k=3
val kmeans = new KMeans().setK(8)
//Evaluar los grupos utilizando WSSSE (Withn set sum of squared errors)
val model = kmeans.fit(training)
//Mostrar los resultados
val WSSSE = model.computeCost(training)

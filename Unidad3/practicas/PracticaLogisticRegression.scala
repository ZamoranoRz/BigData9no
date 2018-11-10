//////////////////////////////////////////////
// Proyecto de regresion logistica //////////////
////////////////////////////////////////////

//  In this project we will be working with a fake advertising data set, indicating whether or not a particular internet user clicked on an Advertisement. We will try to create a model that will predict whether or not they will click on an ad based off the features of that user.
//  This data set contains the following features:
//    'Daily Time Spent on Site': consumer time on site in minutes
//    'Age': cutomer age in years
//    'Area Income': Avg. Income of geographical area of consumer
//    'Daily Internet Usage': Avg. minutes a day consumer is on the internet
//    'Ad Topic Line': Headline of the advertisement
//    'City': City of consumer
//    'Male': Whether or not consumer was male
//    'Country': Country of consumer
//    'Timestamp': Time at which consumer clicked on Ad or closed window
//    'Clicked on Ad': 0 or 1 indicated clicking on Ad

//////////////////////////////////////////////////////////
// Complete las siguientes tareas que estan comentas ////
/////////////////////////////////////////////////////////



////////////////////////
/// Tome los datos //////
//////////////////////

// Importe una  SparkSession con la libreria Logistic Regression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
// Optional: Utilizar el codigo de  Error reporting
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)
// Cree un sesion Spark
val spark = SparkSession.builder().getOrCreate()
// Utilice Spark para leer el archivo csv Advertising.
val data  = spark.read.option("header","true").option("inferSchema", "true").format("csv").load("advertising.csv")

// Imprima el Schema del DataFrame
data.printSchema()


///////////////////////
/// Despliegue los datos /////
/////////////////////

// Imprima un renglon de ejemplo
data.show(1)

////////////////////////////////////////////////////
//// Preparar el DataFrame para Machine Learning ////
//////////////////////////////////////////////////

//   Hacer lo siguiente:
//    - Renombre la columna "Clicked on Ad" a "label"
//    - Tome la siguientes columnas como features "Daily Time Spent on Site","Age","Area Income","Daily Internet Usage","Timestamp","Male"
//    - Cree una nueva clolumna llamada "Hour" del Timestamp conteniendo la  "Hour of the click"
val data2 = (data.select(data("Clicked on Ad").as("label"), $"Daily Time Spent on Site",
$"Age", $"Area Income", $"Daily Internet Usage", $"Ad Topic Line", $"City", $"Male", $"Country", $"Timestamp"))

val df = data2.withColumn("Hour", data2("Timestamp"))

// Importe VectorAssembler y Vectors
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
// Cree un nuevo objecto VectorAssembler llamado assembler para los feature

val assembler = (new VectorAssembler()
                  .setInputCols(Array("Daily Time Spent on Site","Age","Area Income","Daily Internet Usage","Timestamp","Male"))
                  .setOutputCol("features"))

// Utilice randomSplit para crear datos de train y test divididos en 70/30


///////////////////////////////
// Configure un Pipeline ///////
/////////////////////////////

// Importe  Pipeline
// Cree un nuevo objeto de  LogisticRegression llamado lr

// Cree un nuevo  pipeline con los elementos: assembler, lr

// Ajuste (fit) el pipeline para el conjunto de training.


// Tome los Resultados en el conjuto Test con transform

////////////////////////////////////
//// Evaluacion del modelo /////////////
//////////////////////////////////

// Para Metrics y Evaluation importe MulticlassMetrics

// Convierta los resutalos de prueba (test) en RDD utilizando .as y .rdd

// Inicialice un objeto MulticlassMetrics

// Imprima la  Confusion matrix

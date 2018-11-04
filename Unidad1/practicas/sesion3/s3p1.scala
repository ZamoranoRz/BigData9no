//Crea una lista llamad "lista" con los elementos "rojo", "blanco", "negro"
val lista = List("rojo","blanco","negro")

// 2. Añadir 5 elementos mas a "lista" "verde" ,"amarillo", "azul", "naranja", "perla"
val newlist = "verde" :: "amarillo" :: "azul" :: "naranja" :: "perla" :: lista

// 3. Traer los elementos de "lista" "verde", "amarillo", "azul"
newlist slice (0,3)

// 4. Crea un arreglo de numero en rango del 1-1000 en pasos de 5 en 5

val arreglo = Array.range(0,1000, 5)

// 5. Cuales son los elementos unicos de la lista Lista(1,3,3,4,6,7,3,7) utilice conversion a conjuntos
val list = List(1,3,3,4,6,7,3,7)
list.toSet

// 6. Crea una mapa mutable llamado nombres que contenga los siguiente
//     "Jose", 20, "Luis", 24, "Ana", 23, "Susana", "27"

val mapa = collection.mutable.Map(("Jose", 20),("Luis", 24),("Ana", 23),("Susana", 27))
mapa("Jose")


// 6 a . Imprime todas la llaves del mapa
mapa.keys

// 7 b . Agrega el siguiente valor al mapa("Miguel", 23)
mapa += ("Miguel" -> 23)

//Desarrollar un algoritmo en scala que me diga si un numero es primo

var a: Double = 223;
var con = 2;
var r: Double = 0;
if(a%con==0){
  println("Es par, no primo.")
}else{
  while(a/con > con){
    con = con + 1;
    if(a%con ==0){
      println("El número no es primo")
    }else{
      if(r<con){
        println("El número es primo ya que solo se puede dividir entre 1 y el mismo.")
        println("comprobación: ")
      }
      r = a/con;
      println(a +"/"+ con + "=" + r );
    }

  }
}

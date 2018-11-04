def primo(num:Int): String = {
    for(n <- Range(2, num)){
        if(num%n == 0){
            return "no es primo"
        }
    }
    return "es primo"
}

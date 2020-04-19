

object evenInAList extends App {
  val x = List (1,3,5,7,9)
  val y = List (1,2,3)

  def check(x:List[Int]):Boolean = {
    for (y<-x) if (y % 2 == 0) return true
    return false
  }

  println(check(x))
  println(check(y))

}

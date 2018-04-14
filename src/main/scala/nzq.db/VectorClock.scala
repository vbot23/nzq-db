package nzq.db

class VectorClock(numOfMachine: Int, localIdx: Int, neighborsIdx: List[Int], allKeys: List[List[String]]) {

  private var vc = Array.ofDim[Int](numOfMachine, numOfMachine)
  vc = vc.map(_ => Array.fill(numOfMachine)(-1))
  for (j <- neighborsIdx) {
    vc(j)(localIdx) = 0
    vc(localIdx)(j) = 0
  }


  def advance(machines: List[Int]): Unit = {
    machines.foreach(
      machine => vc(localIdx)(machine) += 1
    )
  }

  def merge(other: VectorClock): Unit = {
    for (i <- 0 until numOfMachine) {
      for (j <- 0 until numOfMachine) {
        if (vc(i)(j) != -1) {
          vc(i)(j) = math.max(vc(i)(j), other.getElement(i, j))
        }
      }
    }
  }

  def getElement(r: Int, c: Int): Int = vc(r)(c)

  def getLocalIdx: Int = localIdx

  def larger(other: VectorClock): Boolean = {
    val k = other.getLocalIdx
    if (vc(k)(localIdx) != other.getElement(k, localIdx) - 1) return false
    for (j <- 0 until numOfMachine if j != k) {
      if (vc(j)(localIdx) < other.getElement(j, localIdx)) return false
    }
    true
  }
}

package nzq.db

class VectorClock(numOfMachine: Int, localIdx: Int) extends Serializable {

  private var vc = Array.ofDim[Int](numOfMachine, numOfMachine)
  vc = vc.map(_ => Array.fill(numOfMachine)(-1))

  def init(machines: List[Int]): Unit = {
      for (j <- machines) {
        vc(j)(localIdx) = 0
        vc(localIdx)(j) = 0
      }
  }

  // for test
  def initWithArray(array: Array[Array[Int]]): Unit = {
    vc = array
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

  /**
    * for testing purpose only!!
    */

  def getLocalIdx: Int = localIdx

  /**
    * check if other is causal past of this vc
    */
  def inCausalPast(other: VectorClock): Boolean = {
    val k = other.getLocalIdx
    if (vc(k)(localIdx) != other.getElement(k, localIdx) - 1) return false
    for (j <- 0 until numOfMachine if j != k) {
      if (vc(j)(localIdx) < other.getElement(j, localIdx)) return false
    }
    true
  }

  override def toString = s"VectorClock($vc, $getLocalIdx)"

  def showVc(): Unit = {
    for (i <- vc.indices) {
      for (j <- vc(i)) {
        print(j + " ")
      }
      println()
    }
  }

  def equals(other: VectorClock): Boolean = {
    if (vc.length != other.vc.length) false
    for (i <- vc.indices) {
      if (!vc(i).sameElements(other.vc(i))) false
    }
    true
  }

}

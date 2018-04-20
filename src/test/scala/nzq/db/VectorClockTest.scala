package nzq.db

import org.scalatest._
class VectorClockTest extends FlatSpec with BeforeAndAfter {


  val numOfMachine = 4
  var arr = Array.ofDim[Int](numOfMachine, numOfMachine)
  arr = arr.map(_ => Array.fill(numOfMachine)(-1))
  "advance test" should "increase specified idx by 1" in {
    val tarr = arr
    val base = new VectorClock(numOfMachine, 0)
    tarr(0)(1) = 1
    tarr(0)(2) = 1
    tarr(0)(3) = 1
    tarr(1)(0) = 0
    tarr(2)(0) = 0
    tarr(3)(0) = 0
    base.initWithArray(tarr)

    val test = new VectorClock(numOfMachine, 0)
    val machines = List(1, 2, 3)
    test.init(machines)
    test.advance(machines)
    assert(base.equals(test))
  }

  "merge test" should "test merging 2 vcs" in {
    val tarr = arr
    val base = new VectorClock(numOfMachine, 0)
    tarr(0)(1) = 1
    tarr(0)(2) = 1
    tarr(0)(3) = 1
    tarr(2)(0) = 1
    tarr(2)(1) = 1
    tarr(2)(3) = 0
    tarr(1)(0) = 0
    tarr(2)(0) = 0
    tarr(3)(0) = 0
    tarr(0)(2) = 0
    tarr(1)(2) = 0
    tarr(3)(2) = 0
    base.initWithArray(tarr)

    val test = new VectorClock(numOfMachine, 0)
    val machines = List(1, 2, 3)
    test.init(machines)
    test.advance(machines)

    val other = new VectorClock(numOfMachine, 2)
    val otherMachines = List(0, 3)
    other.init(otherMachines)
    other.advance(otherMachines)

    test.merge(other)
    assert(base.equals(test))
  }

  "is causal past test" should "test ki part" in {
    val tarr = arr
    val base = new VectorClock(numOfMachine, 0)
    val test = new VectorClock(numOfMachine, 3)

    base.init(List(1, 2, 3))
    base.advance(List(2))
    val machines = List(0, 1, 2)
    test.init(machines)
    test.advance(machines)


    // ki == 1
    assert(base.inCausalPast(test))

    // ki == 2
    test.advance(List(0))
    assert(!base.inCausalPast(test))
  }

  "is causal past test" should "ji part" in {
    val tarr = arr
    val base = new VectorClock(numOfMachine, 0)

    tarr(0)(1) = 0
    tarr(0)(2) = 0
    tarr(0)(3) = 0
    tarr(2)(3) = 1
    tarr(1)(0) = 0
    tarr(2)(0) = 0
    tarr(3)(0) = 0
    base.initWithArray(tarr)

    val test = new VectorClock(numOfMachine, 3)

    val machines = List(0, 1, 2)
    test.init(machines)
    test.advance(machines)

    assert(!test.inCausalPast(base))
    assert(base.inCausalPast(test))
  }
}

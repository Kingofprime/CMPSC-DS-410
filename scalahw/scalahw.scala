// Worked with Vishnu Varthan

case class Neumaier(sum: Double, c: Double)

object HW {

    def q1(n: Int): List[Int] = {
        List.tabulate(n)(i => (i + 1) * (i + 1))
    }

    def q2(n: Int): Vector[Double] = {
        Vector.tabulate(n)(i => math.sqrt(i + 1))
    }

    def q3(x: Seq[Double]): Double = {
        x.foldLeft(0.0)(_+_)
    }

    def q4(x: Seq[Double]): Double = {
      x.foldLeft(1.0)(_ * _)
    }

    def q5(x: Seq[Double]): Double = {
        x.foldLeft(0.0)((a, b) => a + math.log(b))
    }
    
    def q6(x: Seq[(Double, Double)]): (Double, Double) = {
        x.foldLeft(0.0,0.0)((a,b) => (a._1 + b._1, a._2 +b._2)) 
    }

    def q7(x: Seq[(Double, Double)]): (Double, Double) = {
        x.foldLeft((0.0, Double.NegativeInfinity))((a, b) => (a._1 + b._1, math.max(a._2, b._2)))
    }

    def q8(n: Int): (Int, Int) = {
        (1 to n).foldLeft((0, 1))((a, b) => (a._1 + b, a._2 * b))
    }

    def q9(x: Seq[Int]): Int = {
        x.filter(_ % 2 == 0).map(x => x * x).reduce(_ + _)
    }

    def q10(x: Seq[Double]): Double = {
        x.foldLeft((0.0, 1)) { case ((sum, index), currentValue) =>
           (sum + currentValue * index, index + 1)
         }._1
    }

    def q11(x: Seq[Double]): Double = {
        val initial = Neumaier(0.0, 0.0)
        val neumaierSum = x.foldLeft(initial) { (a, value) =>
          val t = a.sum + value
          val c = if (math.abs(a.sum) >= math.abs(value)) {
            a.c + ((a.sum - t) + value)
          } else {
            a.c + ((value - t) + a.sum)
          }
          Neumaier(t, if (t == Double.PositiveInfinity || t == Double.NegativeInfinity) a.c else c)
         }
        neumaierSum.sum + neumaierSum.c
    }

}

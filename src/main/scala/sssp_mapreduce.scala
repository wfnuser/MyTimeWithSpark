import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wfnuser on 16/10/13.
  */
object sssp_mapreduce {
  def main(args: Array[String]) {
    //@transient
    val conf = new SparkConf().setMaster("local").setAppName("sssp_md")
    //@transient
    val sc = new SparkContext(conf)

    // get edges list
    val graph = sc.textFile("/Users/wfnuser/Downloads/temp/facebook/graph.edges").map { line =>
      val fields = line.split(" ")
      ((fields(0),fields(1)),fields(2))
    }

    graph.foreach(println)

    // source vertex
    val src = 0.toString()

    // initialize adjacency list
    val adjacency = graph.map( line => (line._1._1, (line._1._2, line._2)) )
      .groupByKey()
      .mapValues(_.toList)
      .map{ line =>
        if (src == line._1) {
          (line._1,(0,line._2))
        } else {
          (line._1,(Int.MaxValue,line._2))
        }
      }

    adjacency.foreach(println)

    // flatMap
    val distanceFlat = adjacency.flatMap{ line =>
      val arr = line._2._2.map{ x =>
        if (line._2._1 >= Int.MaxValue) {
          (x._1, Int.MaxValue)
        } else {
          (x._1, x._2.toInt + line._2._1)
        }
      }
      arr ++ Array((line._1,line._2._1.toInt))
    }
    distanceFlat.foreach(println)

    // reduceByKey
    val distance = distanceFlat.reduceByKey { (x, y) =>
      if (x<y) {
        val res = x
        res
      } else {
        val res = y
        res
      }
    }
    distance.foreach(println)



  }

}

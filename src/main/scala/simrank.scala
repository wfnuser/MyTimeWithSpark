import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

/**
  * Created by wfnuser on 16/10/13.
  */
object simrank {
  def main(args: Array[String]): Unit = {
    //@transient
    //val conf = new SparkConf().setAppName("simrank")
    val conf = new SparkConf().setMaster("local").setAppName("simrank")
    //@transient
    val sc = new SparkContext(conf)

    def hexToLong(s:String): Long = {
      s.toList.map("0123456789abcdef".indexOf(_)).reduceLeft(_*16 + _)
    }

    val graph = sc.textFile("/Users/wfnuser/Downloads/temp/facebook/414.edges").map { line =>
      val fields = line.split(" ")

      (fields(0),fields(1))
      //((hexToLong(fields(0)),hexToLong(fields(1))),(fields(0),fields(1)))
    }

    graph.foreach(println)

    val reversegraph = graph.map(x => (x._2,1))


    val refernum    = reversegraph.reduceByKey((x,y) => x+y).collect().toMap
    val citers      = graph.groupByKey().collect().map(x=> (x._1,x._2.toArray)).toMap
    val verts       = graph.flatMap(x =>Array(x._1,x._2)).distinct()
    println("vertices:")
    verts.foreach(println)
    val vertsarr    = verts.collect()

    val numIter = 5

    var similarity  = verts.flatMap(x =>
      for( i <- vertsarr)
        yield ((x,i),if(x==i) 1.0 else 0.0)
    )

    for (t <- 0 to numIter){
      val maprdd = similarity.flatMap { x => {
        if(citers.contains(x._1._1) && citers.contains(x._1._2)){
          val simarr = new Array[((String,String),Double)]( {if((refernum.contains(x._1._1)==false)||(refernum.contains(x._1._2)==false)) 1 else 0} + citers(x._1._1).length * citers(x._1._2).length)
          var index = 0
          for (i <- citers(x._1._1)) {
            for (j <- citers(x._1._2)) {
              simarr(index) = ((i,j),x._2)
              index = index + 1
            }
          }
          if((refernum.contains(x._1._1)==false)||(refernum.contains(x._1._2)==false))
            simarr(index) = ((x._1._1,x._1._2),0)
          simarr
        }else{
          if((refernum.contains(x._1._1)==false)||(refernum.contains(x._1._2)==false)){
            val simarr = new Array[((String,String),Double)](1)
            simarr(0) = ((x._1._1,x._1._2),0)
            simarr
          }else{
            val simarr = new Array[((String,String),Double)](0)
            simarr
          }
        }
      }
      }

      val reducerdd = maprdd.reduceByKey((x,y) => x+y)

      similarity = reducerdd.map{
        x => {
          if(x._1._1 == x._1._2)
            ((x._1._1,x._1._2),1)
          else {
            if(refernum.contains(x._1._1)&&refernum.contains(x._1._2))
              ((x._1._1,x._1._2),0.95*x._2/((refernum(x._1._1))*refernum(x._1._2)))
            else
              ((x._1._1,x._1._2),0)
          }
        }
      }
    }

    similarity.filter{case (key,value) => value > 0.1}.sortBy(_._2,false).foreach(println)
    //similarity.saveAsTextFile("hdfs://lab307-spark:9000/input/qinghao/simrank/10000node/")

    //    val graph = sc.textFile("/Users/wfnuser/Documents/STUDY/SJTU_LAB/iiot/spark/datasets/cite2.txt").map { line =>
    //      val fields = line.split("\t")
    //      (fields(0).toLong,fields(1).toLong)
    //    }
    //
    //    val verts = graph.flatMap(x => Array(x._1,x._2)).distinct()
    //    val vertsarr = verts.collect()
    //
    //    val map = new mutable.HashMap[(Long,Long),Long]()
    //    val testmap = new mutable.HashMap[Long,Long]()
    //
    //    for( i <- 0 to vertsarr.length - 1){
    //        for(j <- i+1 to vertsarr.length - 1){
    //          map.put((vertsarr(i),vertsarr(j)),0)
    //        }
    //    }
    //
    //    var simgraph = sc.parallelize(map.toSeq)
    //    val basegraph = simgraph
    //    simgraph.foreach(x => println(x._1))
    //
    //    val neibs = graph.groupByKey()
    //    val neibmap = new mutable.HashMap[Long,Array[Long]]()
    //    neibs.foreach(
    //      x =>  println(x._1)
    //        neibmap.put(x._1,x._2.toArray)
    //    )
    //neibmap.foreach(println)
    //neibmap
    //print(neibmap(9907233))


    //    for( num <- 0 to 10) {
    //      simgraph = simgraph.flatMap(x => {
    //        val srcset = neibmap(x._1._1)
    //        val dstset = neibmap(x._1._2)
    //        val newset = srcset.union(dstset).distinct
    //        val my_length = (newset.length + srcset.length + dstset.length).toInt
    //        val result = new Array[(Long,Long)](5)
    //        /*
    //        for (i <- 0 to (newset.length - 1)) {
    //          for (j <- i + 1 to (newset.length - 1)) {
    //            result ++ Array(((newset(i), newset(j)), (x._2,1)))
    //          }
    //        }
    //        for (i <- 0 to (srcset.length - 1)) {
    //          for (j <- i + 1 to (srcset.length - 1)) {
    //            result ++ Array(((srcset(i), srcset(j)), (1,1)))
    //          }
    //        }
    //        for (i <- 0 to (dstset.length - 1)) {
    //          for (j <- i + 1 to (dstset.length - 1)) {
    //            result ++ Array(((dstset(i), dstset(j)), (1,1)))
    //          }
    //        }
    //        */
    //        result
    //      }).take(2).foreach(println)
    //reduceByKey( (x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count }

    //simgraph.
    //    }
  }

}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wfnuser on 16/10/13.
  */
object pagerank {
  def main(args: Array[String]) {
    //@transient
    val conf = new SparkConf().setMaster("local").setAppName("sssp")
    //@transient
    val sc = new SparkContext(conf)

    // Initialize a random graph with 10 vertices
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges( e => ((new scala.util.Random).nextInt(10000))/10000.0 ).mapEdges(e => e.attr.toDouble)
    graph.edges.foreach(println)
    //val sourceId: VertexId = 0 // The ultimate source

    val tmp = graph.outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
    val edgetmp = tmp.mapTriplets( e => 1.0/e.srcAttr )

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = edgetmp.mapVertices( (id, attr) => (1.0,1.0) )

    //initialGraph.vertices.foreach(println)


    val resetProb = 0.15
    val initialMessage = 10.0
    val numIter = Int.MaxValue
    val tol = 0.01


    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      println(msgSum)
      val newPR = resetProb + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }
    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]): Iterator[(VertexId, Double)] = {
      if(edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))

      }
      else {
        //print(edge.srcAttr)
        Iterator.empty
        //Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
      }
    }
    def messageCombiner(a: Double, b: Double): Double = a + b


    val pageRankGraph = Pregel(initialGraph,initialMessage,numIter)(vertexProgram, sendMessage, messageCombiner)

    println(pageRankGraph.vertices.collect.mkString("\n"))
  }
}

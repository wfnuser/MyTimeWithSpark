import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wfnuser on 16/10/13.
  */
object sssp {
  def main(args: Array[String]) {
    //@transient
    val conf = new SparkConf().setMaster("local").setAppName("sssp")
    //@transient
    val sc = new SparkContext(conf)

    // Initialize a random graph with 10 vertices
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges( e => (new scala.util.Random).nextInt(100) ).mapEdges(e => e.attr.toDouble)
    graph.edges.foreach(println)
    val sourceId: VertexId = 0 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      //Merge Message
      (a, b) => math.min(a, b)
    )

    println(sssp.vertices.collect.mkString("\n"))
  }
}

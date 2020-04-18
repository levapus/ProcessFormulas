import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes._


object CalculateDepth extends App {
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("HelloGraphFrames")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext
  sparkContext.setCheckpointDir("/tmp/graphx-checkpoint")
  sparkContext.setLogLevel("ERROR")

  val sqlContext = sparkSession.sqlContext

  //Building the graph
  //create a Vertex DataFrame with unique ID column "id"
  val verticesDF = sqlContext.createDataFrame(List(
    ("r00t", "root", "", 0),
    ("b", "formula B", "1", 0),
    ("c", "formula C", "item(q) + 1", 0),
    ("d", "formula D", "3", 0),
    ("e", "formula E", "item(b) + 1", 0),
    ("f", "formula F", "item(b) + 1", 0),
    ("g", "formula G", "item(b) + 1", 0),
    ("h", "formula H", "item(g) + 1", 0),
    ("i", "formula I", "item(c) + item(d)", 0),
    ("j", "formula J", "item(f) + item(i)", 0),
    ("k", "formula K", "item(i) + 1", 0),
    ("l", "formula L", "item(i) + item(c)", 0),
    ("m", "formula M", "item(d) + 1", 0),
    ("n", "formula N", "item(d) + 1", 0),
    ("o", "formula O", "item(m) + item(n) + item(d)", 0),
    ("p", "formula P", "item(m) + 1", 0),
    ("q", "formula Q", "item(e) + 1", 0),
    ("r", "formula R", "item(k) + 1", 0)
  )).toDF("id", "name", "formula", "value")
    //processing columns
    .withColumn("level", lit(null))
    .withColumn("root", lit(null))
    .withColumn("path", lit(null))
    .withColumn("iscyclic", lit(null))
    .withColumn("isleaf", lit(null))

  //create an Edge DataFrame with "src" and "dst" columns
  val edgesDF = sqlContext.createDataFrame(List(
    ("r00t", "b"),
//    ("r00t", "c"),
    ("r00t", "d"),
    ("b", "e"),
    ("b", "f"),
    ("b", "g"),
    ("c", "f"),
    ("c", "i"),
    ("c", "l"),
    ("d", "i"),
    ("d", "m"),
    ("d", "n"),
    ("d", "o"),
    ("e", "q"),
    ("q", "c"),
    ("f", "h"),
    ("f", "j"),
    ("g", "h"),
    ("i", "j"),
    ("i", "k"),
    ("i", "l"),
    ("m", "o"),
    ("m", "p"),
    ("n", "o"),
    ("k", "r"),
    //    ("r", "d"), //creates a cycle
    //    ("h", "h"), //creates a self-cycle
    //    ("g", "g"), //creates a self-cycle
    //    //("j", "b"), //creates a big cycle
    //    ("o", "m"), //creates a big cycle
  )).toDF("src", "dst")

  //create a graph
  val g = GraphFrame(verticesDF, edgesDF)

  //display the vertex and edge DataFrames
  println("=== Built graph =========================================================")
  println("Graph (vertices, edges):");
  g.vertices.show(100)
  g.edges.show(100)

  //Pregel
  println(s"=== Pregel =============================================================")
  val gx = g.toGraphX
  println("========================================================")
  gx.vertices.foreach(x => println(x))

  //create a graph with (id, formula, level, value)
  val initialGx = gx.mapVertices((id, row) =>
    if(row.getString(0) == "r00t")
      (row.getString(0), row.getString(2), 0D)
    else
      (row.getString(0), row.getString(2), Double.NegativeInfinity))
  initialGx.vertices.foreach(x => println(x))

  //call Pregel to process the graph
  val bfsGx = initialGx.pregel(("", Double.NegativeInfinity), Int.MaxValue, EdgeDirection.Out)(
    //vprog - vertex Program
    (id, vAttr, msg) => {
      println("vprog: " + (vAttr._1, vAttr._2, math.max(vAttr._3, msg._2)))
      (vAttr._1, vAttr._2, math.max(vAttr._3, msg._2))
    },
    //sendMsg - send Message
    triplet => {
      if (triplet.srcAttr._3 != Double.NegativeInfinity) {
        println(s"sendMsg: " + (triplet.dstId, (triplet.srcAttr._1, triplet.srcAttr._3 + 1)))
        Iterator((triplet.dstId, (triplet.srcAttr._1, triplet.srcAttr._3 + 1)))
      } else {
        Iterator.empty
      }
    },
    //mergeMsg - merge message
    (msg1, msg2) => {
      println(s"mergeMsg: $msg1, $msg2")
      (msg1._1, math.max(msg1._2, msg2._2))
    }
  )
  println("========================================================")
  println(bfsGx.vertices.collect.mkString("\n"))


  import org.apache.spark.graphx.{Graph, VertexId}
  import org.apache.spark.graphx.util.GraphGenerators

  // A graph with edge attributes containing distances
  val graph: Graph[Long, Double] =
    GraphGenerators.logNormalGraph(sparkContext, numVertices = 100).mapEdges(e => e.attr.toDouble)
  val sourceId: VertexId = 42 // The ultimate source
  // Initialize the graph such that all vertices except the root have distance infinity.
  val initialGraph = graph.mapVertices((id, _) =>
    if (id == sourceId) 0.0 else Double.PositiveInfinity)

  println("========================================================")
  graph.vertices.foreach(x => println(x))
  println("========================================================")
  initialGraph.vertices.foreach(x => println(x))

  val sssp = initialGraph.pregel(Double.PositiveInfinity)(
    (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
    triplet => {  // Send Message
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    },
    (a, b) => math.min(a, b) // Merge Message
  )

  println("========================================================")
  println(sssp.vertices.collect.mkString("\n"))

}

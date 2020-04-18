import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes._


object ProcessGraph extends App {
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
    ("b", "formula B", "", 1),
    ("c", "formula C", "", 2),
    ("d", "formula D", "", 3),
    ("e", "formula E", "", 4),
    ("f", "formula F", "", 5),
    ("g", "formula G", "", 6),
    ("h", "formula H", "", 7),
    ("i", "formula I", "", 8),
    ("j", "formula J", "", 9),
    ("k", "formula K", "", 10),
    ("l", "formula L", "", 11),
    ("m", "formula M", "", 12),
    ("n", "formula N", "", 13),
    ("o", "formula O", "", 14),
    ("p", "formula P", "", 15),
    ("q", "formula Q", "", 16),
    ("r", "formula R", "", 17)
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
    ("r00t", "c"),
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
    ("r", "d"), //creates a cycle
    ("h", "h"), //creates a self-cycle
    ("g", "g"), //creates a self-cycle
    //("j", "b"), //creates a big cycle
    ("o", "m"), //creates a big cycle
  )).toDF("src", "dst")

  //create a graph
  val g = GraphFrame(verticesDF, edgesDF)

  //display the vertex and edge DataFrames
  println("=== Built graph =========================================================")
  println("Graph (vertices, edges):");
  g.vertices.show(100)
  g.edges.show(100)

  //Select subgraph for a given root
  val newRoot = "d"
  //val newRoot = "r00t"
  println(s"=== Select subgraph with root ($newRoot) ===============================")

  /**
   * Calculate subgraph based on new root vertex.
   *
   * @param graph
   * @param newRoot
   * @return
   */
  def subgraph(graph: GraphFrame, newRoot: String) = {
    val vertices = g.vertices.select("id").collect.map(r => r.getString(0)).toList
    val shortestPathsDF = graph.shortestPaths.landmarks(vertices).run()
      .select("id", "distances")
      .where(s"id = '$newRoot'")

    //debug info
    //shortestPathsDF.show(false)

    val newVertices = shortestPathsDF.first().getMap(1).keySet

    g.filterVertices(s"id in (${newVertices.mkString("'", "', '", "'")})").dropIsolatedVertices()
  }

  val subgr = subgraph(g, newRoot)
  println("Subgraph (vertices, edges): ")
  subgr.vertices.show(100)
  subgr.edges.show(100)

  //Cycle detection
  println(s"=== Cycle detection ====================================================")

  /**
   * Detect cycles graphs.
   *
   * @param graph
   * @return
   */
  def detectCycles(graph: GraphFrame) = {
    val vertices = graph.vertices.select("id").collect.map(r => r.getString(0)).toList
    val shortestPathsDF = graph.shortestPaths.landmarks(vertices).run()
      .select("id", "distances")

    //debug info
    //shortestPathsDF.show(false)

    //map of nodes with distances: {fromNode -> map(toNode -> distance)}
    val shortestPathsMap = shortestPathsDF.collect().map(r => (r.getString(0), r.getMap[String, Int](1))).toMap
    //println(shortestPathsMap)

    import scala.collection.mutable
    def nextCycle(fromNode: String) = {
      val cycleNodes: mutable.Set[String] = mutable.Set[String]()
      if(shortestPathsMap.isDefinedAt(fromNode)) {
        val toNodes = shortestPathsMap(fromNode).filter(e => e._2 > 0).keySet
        toNodes.foreach(toNode => {
          if (shortestPathsMap(toNode).filter(e => e._2 > 0).keySet.contains(fromNode)) cycleNodes += (fromNode, toNode)
        })
        Some(cycleNodes.toSet)
      } else None
    }

    val cycles: mutable.Set[Set[String]] = mutable.Set[Set[String]]()
    shortestPathsMap.keySet.foreach(node => {
      val cyclicNodes = nextCycle(node).get
      if(cyclicNodes.nonEmpty) {
        cycles.add(cyclicNodes)
      }
    })
    //debug info
    //println("cycles: " + cycles)

    //last piece: detect self-cycles from a node to itself (above algorithm is based on shortest paths and as a result distance from a node to itself is 0,
    // regardless of the fact if it has a cycle to itself or not)
    val selfCycles: Set[String] = graph.find("(v)-[]->(v)").select("v").select("v.id").collect().map(r => r.getString(0)).toSet
    //debug info
    //println("self-cycles: " + selfCycles)

    val allCycles = cycles.toSet ++ selfCycles.map(node => Set(node))

    val cyclesGraphs = allCycles.map(cyclicNodes =>
      graph.filterVertices(s"id in (${cyclicNodes.mkString("'", "', '", "'")})").dropIsolatedVertices()
    )

    cyclesGraphs
  }

  val cyclesGraphs = detectCycles(g)
  cyclesGraphs.foreach(cycle => {
    println("Cycle graph (vertices, edges):");
    cycle.vertices.show(false); cycle.edges.show(false)
    val firstNode = cycle.vertices.first().getString(0)
    val sg = subgraph(g, firstNode)
    println("Affected subgraph (vertices, edges): ")
    sg.vertices.show(100)
    sg.edges.show(100)
 })


  //Pregel
  println(s"=== Pregel =============================================================")
  val gx = g.toGraphX

//  val numVertices = g.vertices.count()
//  val alpha = 0.15
//  val ranks = g.pregel
//    .withVertexColumn("rank", lit(1.0 / numVertices),
//      coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
//    .sendMsgToDst(Pregel.src("rank") / Pregel.src("value"))
//    .aggMsgs(sum(Pregel.msg))
//    .run()
//
//  ranks.show()

//  val processedRDD = gx.pregel(initialMsg,
//    Int.MaxValue,
//    EdgeDirection.Out)(
//    setMsg,
//    sendMsg,
//    mergeMsg)
//
//
//  // initialize id,level,root,path,iscyclic, isleaf
//  val initialMsg = (0L,0,0.asInstanceOf[Any],List("dummy"),0,1)
//
//  //mutate the value of the vertices
////  def setMsg(vertexId: VertexId, value: (Long,Int,Any,List[String], Int,String,Int,Any), message: (Long,Int, Any,List[String],Int,Int)): (Long,Int, Any,List[String],Int,String,Int,Any) = {
//  def setMsg(vertexId: VertexId, value: Row, message: (Long,Int, Any,List[String],Int,Int)): Row = {
//    if (message._2 < 1) { //superstep 0 - initialize
//      Row(value.get(0), value.get(1),value.get(2),value.get(3),value.get(4),value.get(5),value.get(6),value.get(7))
//    } else if ( message._5 == 1) { // set isCyclic
//      (value._1, value._2, value._3, value._4, message._5, value._6, value._7,value._8)
//    } else if ( message._6 == 0 ) { // set isleaf
//      (value._1, value._2, value._3, value._4, value._5, value._6, message._6,value._8)
//    } else { // set new values
//      ( message._1,value._2+1, message._3, value._6 :: message._4 , value._5,value._6,value._7,value._8)
//    }
//  }
//
//  // send the value to vertices
//  def sendMsg(triplet: EdgeTriplet[(Long,Int,Any,List[String],Int,String,Int,Any), _]): Iterator[(VertexId, (Long,Int,Any,List[String],Int,Int))] = {
//    val sourceVertex = triplet.srcAttr
//    val destinationVertex = triplet.dstAttr
//    // check for icyclic
//    if (sourceVertex._1 == triplet.dstId || sourceVertex._1 == destinationVertex._1)
//      if (destinationVertex._5==0) { //set iscyclic
//        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3,sourceVertex._4, 1,sourceVertex._7)))
//      } else {
//        Iterator.empty
//      }
//    else {
//      if (sourceVertex._7==1) //is NOT leaf
//        {
//          Iterator((triplet.srcId, (sourceVertex._1,sourceVertex._2,sourceVertex._3, sourceVertex._4 ,0, 0 )))
//        }
//        else { // set new values
//        Iterator((triplet.dstId, (sourceVertex._1, sourceVertex._2, sourceVertex._3, sourceVertex._4, 0, 1)))
//      }
//    }
//  }
//
//  // receive the values from all connected vertices
//  def mergeMsg(msg1: (Long,Int,Any,List[String],Int,Int), msg2: (Long,Int, Any,List[String],Int,Int)): (Long,Int,Any,List[String],Int,Int) = {
//    // dummy logic not applicable to the data in this use case
//    msg2
//  }

//  def testPregel = {
//    import org.apache.spark.graphx.{Graph, VertexId}
//    import org.apache.spark.graphx.util.GraphGenerators
//
//    // A graph with edge attributes containing distances
//    val graph: Graph[Long, Double] =
//      GraphGenerators.logNormalGraph(sparkContext, numVertices = 100).mapEdges(e => e.attr.toDouble)
//    val sourceId: VertexId = 42 // The ultimate source
//    // Initialize the graph such that all vertices except the root have distance infinity.
//    val initialGraph = graph.mapVertices((id, _) =>
//      if (id == sourceId) 0.0 else Double.PositiveInfinity)
//    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
//      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
//      triplet => {  // Send Message
//        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//        } else {
//          Iterator.empty
//        }
//      },
//      (a, b) => math.min(a, b) // Merge Message
//    )
//    println(sssp.vertices.collect.mkString("\n"))
//  }
//
//  testPregel
//
//  val vertices = g.vertices.select("id").collect.map(r => r.getString(0)).toList
//  val shortestPathsDF = g.shortestPaths.landmarks(vertices).run()
//  //debug info
//  shortestPathsDF.show(false)


}

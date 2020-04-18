import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes._


object ProcessFormulas extends App {
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
    ("d", "formula D", "1", 0),
    ("e", "formula E", "item(b) + 1", 0),
    ("f", "formula F", "item(b) + item(c)", 0),
    ("g", "formula G", "item(b) + 1", 0),
    ("h", "formula H", "item(g) + item(f)", 0),
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
//  val back = GraphFrame.fromGraphX(gx)
  gx.vertices.foreach(x => println(x))
  println("========================================================")

  //create a graph with:
  // vertex (vId, vAttr) = (VertexId, (formula id, formula, depth, value)),
  // edge Edge(src_vId, dst_vId, edgeAttr) = Edge(srcVertexId, dstVertexId, (formula src, formula dst))
  val preprocessedGx = gx.mapVertices((id, row) =>
    if(row.getString(0) == "r00t")
      (row.getString(0), row.getString(2), 0D, 0D, Map[String, Double]())
    else
      (row.getString(0), row.getString(2), Double.NegativeInfinity, Double.PositiveInfinity, Map[String, Double]()))
      .mapEdges(ed => (ed.attr.getString(0), ed.attr.getString(1)))

  preprocessedGx.vertices.foreach(x => println(x))
  preprocessedGx.edges.foreach(x => println(x))

  println("=== calling Pregel ======================================================")

  //call Pregel to process the graph. Used message form: (depth, map of parent values)
  val initialMsg = (Double.NegativeInfinity, Map[String, Double]())
  val processedGx = preprocessedGx.pregel(
    //initial message
    initialMsg,
    Int.MaxValue,
    EdgeDirection.Out)(
    //vprog - vertex Program - mutate the value of the vertices
    (id, vAttr, msg) => {
      println(s"vprog: $vAttr with msg: $msg")
      if(msg._1 == Double.NegativeInfinity) {
        //superstep 0 - initialize
        val init_vAttr = (vAttr._1, vAttr._2, math.max(vAttr._3, msg._1), vAttr._4, vAttr._5)
        println(s"  superstep 0 - initialize: $init_vAttr")
        init_vAttr
      } else {
        //set new values
        var formula = vAttr._2
        println(s"${vAttr._1} formula: " + formula + ", args: " + msg._2)
        msg._2.keySet.foreach(parentId => {
          formula = formula.replaceAll(s"item\\($parentId\\)", msg._2(parentId).toInt.toString)
        })
        println(s"${vAttr._1} formula: $formula")
        val newFormulaValue = if (!formula.contains("item"))
          ArithmeticParser.readExpression(formula).get() else vAttr._4

        val new_vAttr = (vAttr._1, formula, math.max(vAttr._3, msg._1), newFormulaValue, vAttr._5 ++ msg._2)

        println(s"  set new values: $new_vAttr")
        new_vAttr
      }
    },

    //sendMsg - send Message - send the value to vertices
    triplet => {
      if (triplet.srcAttr._3 != Double.NegativeInfinity) {
        val parentValues = if(triplet.srcAttr._4 != Double.PositiveInfinity)
          Map(triplet.srcAttr._1 -> triplet.srcAttr._4) else Map[String, Double]()

        println(s"sendMsg from ${triplet.srcAttr._1} to ${triplet.dstAttr._1}: " + (triplet.dstId, (triplet.srcAttr._3 + 1, parentValues)))
        Iterator((triplet.dstId, (triplet.srcAttr._3 + 1, parentValues)))
      } else {
        Iterator.empty
      }
    },

    //mergeMsg - merge message - receive the values from all connected vertices
    (msg1, msg2) => {
      println(s"mergeMsg: ${msg1._1}, ${msg2._1}")
      (math.max(msg1._1, msg2._1), msg1._2 ++ msg2._2)
    }
  )
  println("========================================================")
  println(processedGx.vertices.collect.mkString("\n"))

  val processedG = GraphFrame.fromGraphX(processedGx)
  println("Graph (vertices, edges):");
  processedG.vertices.show(100, false)
  processedG.edges.show(100, false)
}

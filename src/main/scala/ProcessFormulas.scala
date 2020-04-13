import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes._
import org.graphframes.lib.AggregateMessages

object ProcessFormulas extends App {
  val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("HelloGraphFrames")
      .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val sqlContext = sparkSession.sqlContext


  val g: GraphFrame = examples.Graphs.friends

  //Display the vertex and edge DataFrames
  g.vertices.show()
  g.edges.show()

  // Get a DataFrame with columns "id" and "inDeg" (in-degree)
  val vertexInDegrees: DataFrame = g.inDegrees

  // Find the youngest user's age in the graph.
  // This queries the vertex DataFrame.
  g.vertices.groupBy().min("age").show()

  // Count the number of "follows" in the graph.
  // This queries the edge DataFrame.
  val numFollows = g.edges.filter("relationship = 'follow'").count()

  println("Number of followers: " + numFollows)

  // Search for pairs of vertices with edges in both directions between them.
  val motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
  motifs.show()

  // More complex queries can be expressed by applying filters.
  motifs.filter("b.age > 30").show()

  // Find chains of 4 vertices.
  val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

  // Query on sequence, with state (cnt)
  //  (a) Define method for updating state given the next element of the motif.
  def sumFriends(cnt: Column, relationship: Column): Column = {
    when(relationship === "friend", cnt + 1).otherwise(cnt)
  }
  //  (b) Use sequence operation to apply method to sequence of elements in motif.
  //      In this case, the elements are the 3 edges.
  val condition = { Seq("ab", "bc", "cd")
    .foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship"))) }
  //  (c) Apply filter to DataFrame.
  val chainWith2Friends2 = chain4.where(condition >= 2)
  chainWith2Friends2.show()

  println("BFS ------------------------------------------")
  // Search from "Esther" for users of age < 32.
  val paths = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").run()
  paths.show()

  // Specify edge filters or max path lengths.
  {
//    g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
    val res = g.bfs.fromExpr("name = 'Alice'").toExpr("age > 0")
//    .edgeFilter("relationship != 'friend'")
//    .maxPathLength(3)
    .run()

    res.show()
  }

  println("Message passing via AggregateMessages -------------------------------------------------------")

  // We will use AggregateMessages utilities later, so name it "AM" for short.
  val AM = AggregateMessages

  // For each user, sum the ages of the adjacent users.
  val msgToSrc = AM.dst("age")
  val msgToDst = AM.src("age")
  val agg = { g.aggregateMessages
    .sendToSrc(msgToSrc)  // send destination user's age to source
    .sendToDst(msgToDst)  // send source user's age to destination
    .agg(sum(AM.msg).as("summedAges")) } // sum up ages, stored in AM.msg column
  agg.show()



}

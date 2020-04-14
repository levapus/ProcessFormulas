import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes._
import org.graphframes.lib.{AggregateMessages, Pregel}

object ProcessFormulas extends App {
  val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("HelloGraphFrames")
      .getOrCreate()
  sparkSession.sparkContext.setCheckpointDir("/tmp/graphx-checkpoint")
  sparkSession.sparkContext.setLogLevel("ERROR")

  val sqlContext = sparkSession.sqlContext

  //building the graph
  // Create a Vertex DataFrame with unique ID column "id"
  val v = sqlContext.createDataFrame(List(
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

  // Create an Edge DataFrame with "src" and "dst" columns
  val e = sqlContext.createDataFrame(List(
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
    ("d", "i"),
    ("d", "m"),
    ("d", "n"),
    ("d", "o"),
    ("e", "q"),
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
    ("r", "d"),
  )).toDF("src", "dst")

  // Create a GraphFrame
  val g = GraphFrame(v, e)

  //Display the vertex and edge DataFrames
  g.vertices.show()
  g.edges.show()


  // Search for pairs of vertices with edges in both directions between them.
  val motifs = g.find("(a)-[e]->(b)")
  motifs.show()

  println("BFS ------------------------------------------")
  // Search from "Esther" for users of age < 32.
  val paths = g.bfs.fromExpr("id = 'r00t'").toExpr("id = 'r'").run()
  paths.show()

  println("Message passing via AggregateMessages -------------------------------------------------------")

  // We will use AggregateMessages utilities later, so name it "AM" for short.
  val AM = AggregateMessages

  // For each user, sum the ages of the adjacent users.
//  val msgToSrc = AM.dst("age")
  val msgToDst = AM.src("value")
  val agg = { g.aggregateMessages
//    .sendToSrc(msgToSrc)  // send destination user's age to source
    .sendToDst(msgToDst)  // send source user's age to destination
    .agg(sum(AM.msg).as("summedValues")) } // sum up ages, stored in AM.msg column
  agg.show()


  val numVertices = g.vertices.count()
  val alpha = 0.15
  val ranks = g.pregel
    .withVertexColumn("rank", lit(1.0 / numVertices),
      coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
    .sendMsgToDst(Pregel.src("rank") / Pregel.src("value"))
    .aggMsgs(sum(Pregel.msg))
    .run()

  ranks.show()

}

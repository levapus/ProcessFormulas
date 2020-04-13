import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes._

object HelloDataFrames extends App {
  val greetings = "Hello, GraphFrames!"
  println(greetings)

  val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("HelloGraphFrames")
      .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  val sqlContext = sparkSession.sqlContext

  // Create a Vertex DataFrame with unique ID column "id"
  val v = sqlContext.createDataFrame(List(
    ("a", "Alice", 34, 234, "Apples"),
    ("b", "Bob", 36, 23232323, "Bananas"),
    ("c", "Charlie", 30, 2123, "Grapefruit"),
    ("d", "David", 29, 2321111, "Bananas"),
    ("e", "Esther", 32, 1, "Watermelon"),
    ("f", "Fanny", 36, 333, "Apples" ),
    ("g", "Gabby", 60, 23433, "Oranges")
  )).toDF("id", "name", "age", "cash", "fruit")

  // Create an Edge DataFrame with "src" and "dst" columns
  val e = sqlContext.createDataFrame(List(
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
  )).toDF("src", "dst", "relationship")
  
  // Create a GraphFrame
  import org.graphframes.GraphFrame
  val g = GraphFrame(v, e)

  // Query: Get in-degree of each vertex.
  g.inDegrees.show()

  // Query: Count the number of "follow" connections in the graph.
  g.edges.filter("relationship = 'follow'").count()

  // Run PageRank algorithm, and show results.
  val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
  results.vertices.select("id", "pagerank").show()

  //run BFS
  val bfsDF =  g.bfs.fromExpr("name='Alice'").toExpr("name='Charlie'").maxPathLength(3).run()
  bfsDF.show()

  val foundDF = g.find("(a)-[e]->(b)")
  foundDF.show()

  val foundDF2 = g.find("(a)-[]->(b)")
  foundDF2.show()

  val foundDF3 = g.find("(a)-[*3]->(b)")
  foundDF3.show()


}

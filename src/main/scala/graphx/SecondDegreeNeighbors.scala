package graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//
// Create a graph of second degree neighbors, as in
// http://stackoverflow.com/questions/25147768/spark-graphx-how-to-travers-a-graph-to-create-a-graph-of-second-degree-neighbor.
//
object SecondDegreeNeighbors {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SecondDegreeNeighbors").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //
    // set up the example graph
    //

    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(Array((1L,""), (2L,""), (4L,""), (6L,"")))

    val edges: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 2L, ""), Edge(1L, 4L, ""), Edge(1L, 6L, "")))

    val inputGraph = Graph(vertices, edges)

    //
    // Create an alternate set of vertices each annotated with
    // the set of their successors
    //

    val verticesWithSuccessors: VertexRDD[Array[VertexId]] =
      inputGraph.ops.collectNeighborIds(EdgeDirection.Out)

    //
    // Create a new graph using these vertices and the original edges
    //

    val successorSetGraph = Graph(verticesWithSuccessors, edges)

    //
    // Push these sets along each edge, creating yet another set of vertices,
    // this time all annotated with their neighbors. Combine the sets at the
    // destinations vertex, so using Scala Set removes duplicates. Remove each
    // vertex from the set of its neighbors, hence the extra map tacked on the end.
    //

    val ngVertices: VertexRDD[Set[VertexId]] =
      successorSetGraph.aggregateMessages[Set[VertexId]] (
        triplet => triplet.sendToDst(triplet.srcAttr.toSet),
        (s1, s2) => s1 ++ s2
    ).mapValues[Set[VertexId]](
        (id: VertexId, neighbors: Set[VertexId]) => neighbors - id
    )

    //
    // Create an edge for each neighbor relationship
    //

    val ngEdges = ngVertices.flatMap[Edge[String]](
      {
        case (source: VertexId, allDests: Set[VertexId]) => {
          allDests.map((dest: VertexId) => Edge(source, dest, ""))
        }
      }
    )

    //
    // Now put it all together
    //

    val neighborGraph = Graph(vertices, ngEdges)

    println("*** vertices")
    neighborGraph.vertices.foreach(println)
    println("*** edges")
    neighborGraph.edges.foreach(println)

  }
}

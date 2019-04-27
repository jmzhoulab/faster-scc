package mu.atlas.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by zhoujiamu on 2019/4/27.
  */
object TestSCC {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TestSCC").setMaster("local")

    val sc = new SparkContext(conf)

    val relative = Source.fromFile("data/graph_edges").getLines().toArray
      .map(line => {
        val list = line.split(" ")
        list.head.toLong -> list.last.toLong
      })

    val rdd = sc.makeRDD(relative)
    val edge = rdd.map{case(x, y) => Edge(x, y, 0)}
    val graph = Graph.fromEdges(edge, 0)

    println("show all edges in graph: ")
    graph.edges.collect().foreach(println)

    val scc = StronglyConnectedComponents.run(graph, Int.MaxValue)

    println("show all result: ")
    scc.vertices.collect().sortBy(_._1).foreach(println)

    println("show result only at scc vertex")
    scc.vertices.map{case(vid, gid) => gid -> Set(vid)}
      .reduceByKey(_++_).filter(_._2.size > 1)
      .collect().sortBy(_._1)
      .map(_._2).zipWithIndex
      .foreach{case(set, i) => {
        println(s"scc_${i+1} member: ")
        set.foreach(println)
      }}

  }

}

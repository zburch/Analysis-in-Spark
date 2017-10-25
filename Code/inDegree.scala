import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.graphx._
import scala.util.MurmurHash
import org.apache.spark.rdd.RDD


object inDegree {
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: inDegree <input file>  <output file>")
        System.exit(1);  }
    
     //Creating a spark context
    val conf = new SparkConf().setAppName("citationDistrib").setMaster("yarn")
    val sc = new SparkContext(conf)
   
    /*Create pair RDD from text file. Create sorted key. Reduce and sum counts. 
     * Print where the count = 2 (indicating symmetric following).
     */

    val file=sc.textFile(args(0)).map(line=>line.split(",")).filter(line => line(0) !=null && line(1) != null).map(line=>(line(0).drop(1),line(1).dropRight(1)))
    val edgesRDD: RDD[(VertexId,VertexId)] = file.map{case(index,citation) => (MurmurHash.stringHash(index.toString),MurmurHash.stringHash(citation.toString))}         
    val graph =Graph.fromEdgeTuples(edgesRDD,null)
    var vert = graph.numVertices
    val inDegree = graph.inDegrees
    val out = inDegree.map{case(vertex,indegree)=>(indegree,1)}.
      reduceByKey((countA,countB)=>(countA+countB)).
      map{case(indegree,count)=>(indegree,(count.toFloat/vert))}
    
    out.saveAsTextFile(args(1))
    sc.stop()
  }
}
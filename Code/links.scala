import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import scala.util.matching.Regex

object links {
    def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: Twitter <input file>  <output file>")
        System.exit(1);  }
    
    //Creating a spark session
    val spark = SparkSession.builder().master("yarn").appName("Spark Links").config("spark.sql.warehouse.dir","hdfs://CSC570T-HM02:9000/user/hive/warehouse").enableHiveSupport().getOrCreate()
    val sc= spark.sparkContext;
    
    @transient val hadoopConf=new Configuration
    hadoopConf.set("textinputformat.record.delimiter","#*")

		val inputrdd=sc.newAPIHadoopFile(args(0),classOf[TextInputFormat],classOf[LongWritable],classOf[Text],hadoopConf).map{case(key,value)=>value.toString}.filter(value=>value.length!=0)
		
		val links=inputrdd.filter(line=>line.contains("#index")).filter(line=>line.contains("#%")).map(line=>(getIndex(line),getCitations(line))).flatMapValues(x=>x)

		links.saveAsTextFile(args(1));
		
    sc.stop()
  }
    
    def getIndex(line:String):String={
      val pattern = new Regex("(?<=#index).*")
      return pattern.findFirstIn(line).getOrElse("Null")
    }
    
    def getCitations(line:String):Array[String]={
    val pattern = "(?<=#%).*".r
    return pattern.findAllIn(line).toArray
    }
    
}
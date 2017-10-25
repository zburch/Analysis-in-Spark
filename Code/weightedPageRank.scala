import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import scala.util.matching.Regex

object weightedPageRank {
    def main(args: Array[String]): Unit = {
    if (args.length < 1){
        System.err.println("Usage: Twitter <input file>  <output file>")
        System.exit(1);  }
    
    //Creating a spark session
    val spark = SparkSession.builder().master("yarn").appName("Weighted Page Rank").config("spark.sql.warehouse.dir","hdfs://CSC570T-HM02:9000/user/hive/warehouse").enableHiveSupport().getOrCreate()
    val sc= spark.sparkContext;
    
    @transient val hadoopConf=new Configuration
    hadoopConf.set("textinputformat.record.delimiter","#*")

		val inputrdd=sc.newAPIHadoopFile(args(0),classOf[TextInputFormat],classOf[LongWritable],classOf[Text],hadoopConf).map{case(key,value)=>value.toString}.filter(value=>value.length!=0)
		
		//Get list of citations - one line per each citation in the form (paper,citation)
		val links=inputrdd.filter(line=>line.contains("#index")).filter(line=>line.contains("#%")).map(line=>(getIndex(line),getCitations(line))).flatMapValues(x=>x)
		//Get list of all papers and titles
		val titles = inputrdd.filter(line=>line.contains("#index")).map(line=>(getIndex(line),line.split("\n")(0))).persist()
		
		//define weighted page rank algorithm constants
		val d = 0.85
		val n = titles.count()
		val constant = (1-d)/n
		
		//Reorganize the links RDD to get wIn counts for citations
		val citPap = links.map{case (paper,citation)=>(citation,paper)}
		
		//Get number of in citations for each paper cited
		val wInNumerator = links.map{case (paper,citation)=>(citation,1)}
		    .reduceByKey((countA,countB)=>countA+countB).persist()
		    
		//Join back into original form on citation
		val inLinksTemp = citPap.join(wInNumerator)
        .map{case(citation,(paper,count))=>(paper,(citation,count))}
		
		//Get number of in citations of each citation per page
		val wInDenominator = inLinksTemp.map{case(paper,(citation,count))=>(paper,count)}
		    .reduceByKey((countA,countB)=>countA+countB)
		    
		//Get wIn for each paper/citation
		val inLinks = inLinksTemp.join(wInDenominator)
		    .map{case(paper,((citation,numerator),denominator))=>((paper,citation),(1.0*numerator/denominator))}
		
		//Get number of citations for each paper
		val wOutNumerator = links.map{case(paper,citation)=>(paper,1)}
		    .reduceByKey((countA,countB)=>countA+countB)
		 
		//Join back in on citation
		val outLinksTemp = citPap.join(wOutNumerator)
		    .map{case(citation,(paper,count))=>(paper,(citation,count))}
		    
		//Get sum of citations of citations for each paper
		val wOutDenominator = outLinksTemp.map{case(paper,(citation,count))=>(paper,count)}
		    .reduceByKey((countA,countB)=>countA+countB)
		
		//Get wOut for each paper/citation
		val outLinks = outLinksTemp.join(wOutDenominator)
		    .map{case(paper,((citation,numerator),denominator))=>((paper,citation),(1.0*numerator/denominator))}
		
		//Put in form for the algorithm along with starting value
		var linksForm = inLinks.join(outLinks)
		    .map{case((paper,citation),(win,wout))=>((paper,citation),(win,wout,1.0/n))}
		
		//set checkpoint directory to use in algorithm
		sc.setCheckpointDir(args(1))
		
		for (x <-1 to 10){	
		  
		  //Calculate new rank
		  val calc = linksForm.map{case((paper,citation),(win,wout,pagerank))=> ((citation),(win*wout*pagerank))}
		    .reduceByKey((pageRankA,pageRankB)=>pageRankA+pageRankB)
		    .map{case(citation,pageRank)=>(citation,constant+(d*pageRank))}

		  //Join new value back into the original rdd
		  linksForm = linksForm.map{case((paper,citation),(win,wout,pagerank))=>(paper,(citation,win,wout,pagerank))}
		    .join(calc)
		    .map{case(paper,((citation,win,wout,prA),prB))=>((paper,citation),(win,wout,prB))}
		  
		  //Take a checkpoint every 2nd iteration
		  if (x % 2 == 0){
		    linksForm.checkpoint()
		    linksForm.count()
		  }
		}
		
		//Order descending based on page rank value, take the top 10
		val topten = linksForm.map{case((paper,citation),(win,wout,pagerank))=>(paper,pagerank)}
		    .reduceByKey((pagerankA,pagerankB)=>pagerankA)
		    .sortBy(_._2, false)
		    .zipWithIndex()
		    .filter{case((page,rank),index)=>index<10}
		    .map{case((page,rank),index)=>(page,rank)}
		    
		//Join in title and wIn based on index
		val topTenTitles = topten.join(wInNumerator)
		    .map{case(page,(rank,wIn))=>(page,(rank,wIn))}
		    .join(titles)
		    .map{case(page,((rank,wIn),title))=>(title,(rank,wIn))}
		    .sortBy(_._2,false)
		    .map{case(title,(rank,wIn))=>(title,rank,wIn)}
		
		//Save output
		topTenTitles.saveAsTextFile(args(2));
    sc.stop()
    }
    
    //Find the index of the paper in each citation block
    def getIndex(line:String):String={
      val pattern = new Regex("(?<=#index).*")
      return pattern.findFirstIn(line).getOrElse("Null")
    }
    
    //Find all citations a paper makes, return in an array
    def getCitations(line:String):Array[String]={
    val pattern = "(?<=#%).*".r
    return pattern.findAllIn(line).toArray
    }
}
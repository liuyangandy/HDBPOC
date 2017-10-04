import org.apache.spark._
import org.apache.spark.SparkContext._


object WordCount {
  def main(args: Array[String]) {
    val master = args.length match {
          case x: Int if x > 0 => args(0)
          case _ => "local"
        }

    val sc = new SparkContext(master, "WordCount", System.getenv("SPARK_HOME"))
    val input = args.length match {
      case x: Int if x > 1 => sc.textFile(args(1))
      case _ => sc.parallelize(List("pandas", "i like pandas"))
    }

    val words = input.flatMap(line => line.split(" "))
    args.length match {
      case x: Int if x > 2 => {
        val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
        counts.saveAsTextFile(args(2))
      }
      case _ => {
        val wc = words.countByValue()
        println(wc.mkString(","))
      }
    }

//    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val textFile = sc.textFile("D:/Projects/HDB/wordcount.txt")
//    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
//    wordCount.foreach(println)
  }
}

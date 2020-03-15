package sparkwindowfun


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark._

object sparkwindowfun {
  
  case class schema(date:String,Ticker :String,open: Double,high:Double,low:Double,close:Double,volume_of_the_day:String)
  
  def main(args:Array[String]):Unit={
   
    println("Hello world of Spark !!!")
    
    
    val conf=new SparkConf().setAppName("KING").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data =sc.textFile("file:///C://stocks.txt")
    val data1=data.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2).toDouble,x(3).toDouble,x(4).toDouble,x(5).toDouble,x(6)))
    
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val df=data1.toDF()
    
    df.show()
    
   
  
    
  }
  
   
  
}
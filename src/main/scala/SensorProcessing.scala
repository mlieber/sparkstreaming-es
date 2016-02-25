/*
  Sensor processing code
 */

// scalastyle:off println
//package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
//import com.github.nscala_time.time.Imports._
import java.util.Date
import org.elasticsearch.spark.rdd.EsSpark
//import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder
import org.apache.commons.io.FileUtils
import java.nio.file.Files
//import org.apache.lucene.util.Version
import org.apache.spark.streaming._

/**
 * Parses file sent across the wire
 * ES run w/ export  JAVA_OPTS="-Xms256m -Xmx512m -XX:-UseSuperWord"
 */

object SensorProcessing {
  
   var count = 0;
                    
   
   case class TrialRecord(variance: Int, percentage: Float, trialId: String, patientId: String, visitName: String, value: Int, device: String, startDate: Date, startTime: Int, endDate: Date, endTime: Int)        

      case class SummaryRecord2(trialId: String, private var _patientId: String, private var _visitName:String)
   {

     def visitName = _visitName 
     def patientId = _patientId 
     
     
   }
   
   case class SummaryRecord(trialId: String, private var _patientId: String, private var _visitName:String, private var _max:Int, private var _min:Int,  var count:Int, var sum:Int)
   {

     def max=_max 
     def max_=(value:Int):Unit=_max=value  
     def min=_min 
     def min_=(value:Int):Unit=_min=value  

     def visitName = _visitName 
     def patientId = _patientId 
     val avg = sum / scala.math.max(count, 1)

     def +(count: Int, sum: Int) = SummaryRecord (trialId, _patientId, _visitName, _max, _min, 
      this.count + count,
      this.sum + sum
     )
  
   }
   
     /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      //logInfo("Setting log level to [WARN] for streaming example." +
      //   " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.ERROR)

    }
  }

	def groupSummaryRecords2(batchTime: Time, key: String, value1: Option[TrialRecord],  optionSummary:State[SummaryRecord]):Option[(String, SummaryRecord)] = {
	   // var count=0;
		var min=Integer.MAX_VALUE;
		var max=0;
		var total=0;
        val sum = value1.map(_.value )  
        val sum2: Option[Int] = optionSummary.getOption.map(_.sum) 
        // just summing 2 option traits
        val sum3: Option[Int] = (sum :: sum2 :: Nil).flatten.reduceLeftOption(_ + _) 
     
        if (optionSummary.exists)
        {
          if (value1.get.value > optionSummary.get.max)
            max = value1.get.value
          if (value1.get.value < optionSummary.get.min)
            min = value1.get.value            
         val intermediaryOutput = new SummaryRecord(value1.get.trialId,
                      value1.get.patientId ,
                      value1.get.visitName,
                      max,
                      min, 
                      optionSummary.getOption.get.count+1, 
                      sum3.getOrElse(0).asInstanceOf[Int])
        val output = (key, intermediaryOutput)
        
        optionSummary.update(intermediaryOutput)
        println("updated output: " + key.toString() + " : " + intermediaryOutput.patientId.toString() + intermediaryOutput.sum)
     Some(output)
        }
        else 
        {
           val intermediaryOutput = new SummaryRecord("",
                      "" ,
                      "",
                      0,
                      1000, 
                      0, 
                      0)
        val output = (key, intermediaryOutput)
        
     optionSummary.update(intermediaryOutput)
             println("initial output: " + key.toString() + " : " + intermediaryOutput.patientId.toString() + intermediaryOutput.sum)
               Some(output)
        }
       
}
		

  def summary(trialRecordDStream: DStream[(String, TrialRecord)]):DStream[(String,SummaryRecord)] =
  {
   
    
    trialRecordDStream.foreachRDD( rdd =>
    {
      if (rdd.count > 0)
                count += rdd.count.toInt

    }
    
    )
    println("count so far: " + count)
    
    var stateSpec = StateSpec.function(groupSummaryRecords2 _)

    var summaryRecordUpdatedDStream =     trialRecordDStream.mapWithState(stateSpec)
    summaryRecordUpdatedDStream.print()
   
	return summaryRecordUpdatedDStream
  }
  


  def main(args: Array[String]) {
  

   setStreamingLogLevels()

   // Parameters
    // Create the context with a 1 second batch size
    val batchSize = 10;
    val filter1 =  "HR DAILY"
    val filter2 = "MEASUREMENT"
    val filter3 = "SUMMARY (MEAN) HEART RATE"
    val HRbaseValue = 50
    //val alertThreashold = 10
    val format = new java.text.SimpleDateFormat("ddMMMyyyy")
    
    val sparkConf = new SparkConf().setAppName("SensorProcessing")
    sparkConf.set("es.index.auto.create", "true")
    val sc = new SparkContext(sparkConf)
    sc.addJar("/Users/mlieber/app/spark-streaming_2.11-1.6.0.jar")
    sc.addJar("/Users/mlieber/app/elasticsearch-1.7.2/lib/elasticsearch-1.7.2.jar")
    val ssc = new StreamingContext(sc, Seconds(batchSize))


    ssc.checkpoint("./checkpoint")
    // define a case class

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // alteratively feed the file:
    //val lines = ssc.textFileStream("file:///Users/mlieber/projects/spark/test/data/")
    var max, min, avg =0;
    val filteredLines = lines.filter(line => line.contains(filter1)&&line.contains(filter2)&&line.contains(filter3))
 
    
    println("Trial record processing:" + filteredLines )
    val trialRecordDStream  =
          filteredLines.map ( lin =>
          {  
            val columns = lin.split('|')
            val variance = (columns(12).toInt - HRbaseValue )
            val percentage:Float = (variance*100)/HRbaseValue 
        //    println("Percentage here: " + percentage)
            // CV205-005|000100074|05FEB1945|M|S01|13JUL2015|145433|21JUL2015|030935|MEASUREMENT|HR DAILY|SUMMARY (MEAN) HEART RATE|59||BEATS/MIN|1|N11150612006CDA|||||||17JUL2015|000000|18JUL2015|000000
 //      case class TrialRecord(variance: Int, percentage: Float, trialId: String, patientId: String, visitName: String, value: Int, device: String, startDate: Date, startTime: Int, endDate: Date, endTime: Int)
            // variance/percentage/trialId/patientId/visitName/result/device/startDate/startTime/endDate/endTime
            val varianceRecord = TrialRecord(variance, percentage, columns(0), columns(1), columns(4), columns(12).toInt, columns(16), 
        	                format.parse(columns(23)), columns(24).toInt , format.parse(columns(25)), columns(26).toInt )
            // return tuple KV
//            (columns(0) + '_' + columns(1) + '_' + columns(4)+'_'+columns(16),
            (columns(0) + '_' + columns(1) + '_' + columns(4),
        	  varianceRecord)
          }
          )
        
             trialRecordDStream.foreachRDD(lineRDD =>   
   {
     print("value " + lineRDD)
     EsSpark.saveToEsWithMeta(lineRDD, "sensors/trialdata") 
   }
   )
 
     val summaryUpdatedDStream  = summary(trialRecordDStream)
 //    val summaryUpdatedDStream2  = summary(trialRecordDStream)
 
     summaryUpdatedDStream.foreachRDD(lineRDD =>   
     {
       lineRDD.collect().foreach(println)
       EsSpark.saveToEsWithMeta(lineRDD, "sensors/summarydata") 
   }
   )

    // Start
    ssc.start()    
    ssc.awaitTermination()
    

    }
  }

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

object Medtronic {
  
   var count = 0;
  // var stateSpec = StateSpec.function(groupSummaryRecords2 _)
                         
   
   case class TrialRecord(variance: Int, percentage: Float, trialId: String, patientId: String, visitName: String, value: Int, device: String, startDate: Date, startTime: Int, endDate: Date, endTime: Int)        

      case class SummaryRecord2(trialId: String, private var _patientId: String, private var _visitName:String)
   {
        // trialId: String, patientId: String, visitName:String, max:Int, min:Int, mean:Int)
     //private var _max = 0
   
     def visitName = _visitName 
     def patientId = _patientId 
     
     
   }
   
   case class SummaryRecord(trialId: String, private var _patientId: String, private var _visitName:String, private var _max:Int, private var _min:Int,  var count:Int, var sum:Int)
   {
        // trialId: String, patientId: String, visitName:String, max:Int, min:Int, mean:Int)
     //private var _max = 0
   //  def sum_=(value:Int):Unit =_sum=value
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
  
  
 
     /*
     def this(trialId: String, patientId: String, visitName: String, result: Int)
     {
       this(trialId, patientId, visitName, result, result, result, result)
     }
     
     def this()
     {
       this("","","",0,0,0,0)
     }
     */
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

  /*
	def groupSummaryRecords(it:Iterator[Option[SummaryRecord]],  optionSummary:Option[SummaryRecord]):Option[SummaryRecord] = {
	    var count=0;
		var min=Integer.MAX_VALUE;
		var max=0;
		var total=0;
	    var mean = 0;
	    var summaryRecord1 : Option[SummaryRecord] = None 
	    // new SummaryRecord()
		var i=0
		
		if(optionSummary.isDefined) { //nonEmpty
            println("update max min for " + optionSummary.toString())
		//	 Logger.getRootLogger.info("Found the old state for summary patientId:"+optionSummary.get.patientId+" visitName:"+optionSummary.get.visitName)
			println("max:" + optionSummary.get.max + " min:"+ optionSummary.get.min + " mean:" + optionSummary.get.mean)
			if( optionSummary.get.max>max) {
				println("option summary for " + optionSummary.toString() + " and max was: " + max + " now setting to:" + optionSummary.get.max )
				summaryRecord1.foreach(_.max =optionSummary.get.max);
			}
            
			if(optionSummary.get.min<min) {
			  println("option summary for " + optionSummary.toString() + " and min was: " + min + " now setting to:" + optionSummary.get.min )

				summaryRecord1.foreach(_.min=optionSummary.get.min);
			}
			
			  println("option summary for " + optionSummary.toString() + " and mean was: " + mean + " now setting to:" + optionSummary.get.mean )

			  println("Mean optional " + optionSummary.get.mean)
			  println("mean before optional " + mean)
			  
				mean=(optionSummary.get.mean+mean)/2;
			
		}
	    
		while(it.hasNext) {
			var summaryRecord:Option[SummaryRecord] = it.next();
			if(i==0) {
			  println("i =0 ")
				summaryRecord1 = summaryRecord;
			}
			if(summaryRecord.get.mean !=null.asInstanceOf[Int   ]){
				total = total+ summaryRecord.get.mean;
				count+= 1;
				println("calculating mean for summaryrecord:" + summaryRecord.toString() + " total mean:" + total)

			}
			if(summaryRecord.get.max!=null.asInstanceOf[Int   ] && summaryRecord.get.max>max) {
				 println("calculating max  for summaryrecord:" + summaryRecord.toString() + " greater than "  + max  )

				max = summaryRecord.get.max;

			}
			if(summaryRecord.get.min!=null.asInstanceOf[Int   ] && summaryRecord.get.min<min) {

				min = summaryRecord.get.min;
			 println("calculating min for summaryrecord:" + summaryRecord.toString() + " min:"+ min)

			}
			i+= 1;
		}
		if(summaryRecord1==null && optionSummary.isDefined) {
          println("returning some sr !!!!!!!!!!!!!!!!!!!!!!!")
          return optionSummary
		}
		if(summaryRecord1==null) {
		  println("null")
			return None
			 //null;
		}
		// You want to access the contained SummaryRecord (if any) and call the method on that, which can be done using foreach:
		summaryRecord1.foreach(_.max=max)
	    summaryRecord1.foreach(_.min=min)
		if(count!=0){
			mean = total/count;
			println("mean:  " + mean + " with count: "  + count)
		}

		summaryRecord1.foreach(_.mean=mean)
		println("result end of call : " + summaryRecord1.toString())
	    return summaryRecord1
		
	}
	*/
   
	/**
	 * Method to group summary records
	 * @param it
	 * @param optionSummary
	 * @return
	 */
	//   def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
/*
 * 
 * 
def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
  val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
  val output = (key, sum)
  state.update(sum)
  Some(output)
}
 */
	// def groupSummaryRecords2( it:Iterator[Option[SummaryRecord]],  optionSummary:Option[SummaryRecord]):Option[SummaryRecord] = {
	def groupSummaryRecords2(batchTime: Time, key: String, value1: Option[TrialRecord],  optionSummary:State[SummaryRecord]):Option[(String, SummaryRecord)] = {
	   // var count=0;
		var min=Integer.MAX_VALUE;
		var max=0;
		var total=0;
        val sum = value1.map(_.value )  
        val sum2: Option[Int] = optionSummary.getOption.map(_.sum) 
        // just summing 2 option traits
        val sum3: Option[Int] = (sum :: sum2 :: Nil).flatten.reduceLeftOption(_ + _) 
        // println("RESULT SUM: " + sum3)
        //        this(this.trialId, patiendId, visitName, max, min, count1, sum)
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
        
                      //                      optionSummary.getOption.map(_.min), 
     optionSummary.update(intermediaryOutput)
             println("initial output: " + key.toString() + " : " + intermediaryOutput.patientId.toString() + intermediaryOutput.sum)
               Some(output)
        }
       
}
		
	    
	    
	

  def summary(trialRecordDStream: DStream[(String, TrialRecord)]):DStream[(String,SummaryRecord)] =
  {
    /*
    var summaryRecordDStream: DStream[(String, Option[SummaryRecord])] = trialRecordDStream.map( trialRecordTuple =>
    {
      //Generate the summary records pair from study trial records dstream
      val trialRecord = trialRecordTuple._2;   
      val summaryRecord = new SummaryRecord(trialRecord.trialId, trialRecord.patientId, trialRecord.visitName, 0,0,0,0  );
      val key:String = trialRecord.trialId + "_"+ trialRecord.patientId + "_" + trialRecord.visitName + "_" //+ trialRecord.device 
      println("Summary Key: " + key)
	  (key, Some(summaryRecord));
		
    }
      )
      * 
      */
   
    
    trialRecordDStream.foreachRDD( rdd =>
    {
      if (rdd.count > 0)
                count += rdd.count.toInt

    }
    
    )
    println("count so far: " + count)
    
      //Group the summary dstream by key for aggregation
  //  val summaryGroupedDStream: DStream[(String,Iterable[Option[SummaryRecord]])] = summaryRecordDStream.groupByKey();
  //  val trialGroupedDStream = trialRecordDStream.reduceByKey((a, b) => a + b) 
    //.asInstanceOf[String, Option[SummaryRecord]]
    
    
    //Map the grouped dstream to generate summary records min max and avg
    /*
	 summaryRecordDStream  = summaryGroupedDStream.map( t => 
	  {

		val summaryRecord1 = groupSummaryRecords2(t._2.iterator, None: Option[SummaryRecord]);
		(t._1, summaryRecord1);
			}
		);
		
		*/
    var stateSpec = StateSpec.function(groupSummaryRecords2 _)
	println("TEST1");
    
    var summaryRecordUpdatedDStream =     trialRecordDStream.mapWithState(stateSpec)
	println("TEST2");
    summaryRecordUpdatedDStream.print()
    
    /*
    //Calling updateStateByKey to maintain the state of the summary object  - only from the stream
    var summaryRecordUpdatedDStream  = summaryRecordDStream.updateStateByKey(

        (v1:Seq[Option[SummaryRecord]], v2:Option[SummaryRecord]) =>
        {
		  val summaryRecord1:Option[SummaryRecord] = groupSummaryRecords(v1.iterator, v2)
		  println("result is: " + summaryRecord1.toString())
		  summaryRecord1
        }

        );
		*/

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
    
    val sparkConf = new SparkConf().setAppName("Medtronic")
    sparkConf.set("es.index.auto.create", "true")
    val sc = new SparkContext(sparkConf)
    sc.addJar("/Users/mlieber/app/spark-streaming_2.11-1.6.0.jar")
    sc.addJar("/Users/mlieber/app/elasticsearch-1.7.2/lib/elasticsearch-1.7.2.jar")
    val ssc = new StreamingContext(sc, Seconds(batchSize))


    ssc.checkpoint("./checkpoint")
    // define a case class
  // case class StudyTrialRecord(trialId: String, patientId: String, visitName: String, value: Int, device: String, startDate: Date, startTime: Int, endDate: Date, endTime: Int)              

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    //val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = ssc.textFileStream("file:///Users/mlieber/projects/spark/test/data/")
    var max, min, avg =0;
    val filteredLines = lines.filter(line => line.contains(filter1)&&line.contains(filter2)&&line.contains(filter3))
    //       val summaryRecord = new SummaryRecord(trialRecord.trialId, trialRecord.patientId, trialRecord.visitName, trialRecord.value  );

   // var stateSpec = StateSpec.function(groupSummaryRecords2 _)
                    
    
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
     EsSpark.saveToEsWithMeta(lineRDD, "medtronicsmatt/trialdata") 
   }
   )
 
     val summaryUpdatedDStream  = summary(trialRecordDStream)
 //    val summaryUpdatedDStream2  = summary(trialRecordDStream)
 
     summaryUpdatedDStream.foreachRDD(lineRDD =>   
     {
       lineRDD.collect().foreach(println)
       EsSpark.saveToEsWithMeta(lineRDD, "medtronicsmatt/summarydata") 
   }
   )
   
   
    
  //  val documents = sc.esRDD("medtronics/alerts", "?q=variable:62")
    
    // Start
    ssc.start()    
    ssc.awaitTermination()
    

    }
  }

package edu.ucr.cs.cs226.czhan187;

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



object SparkRDD {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRDD").setMaster("local")
    val sContext = new SparkContext(conf)

    val filePath = args(0)

    val data = sContext.textFile(filePath).cache()

    //average number of bytes for lines of each response code.
    val pwTask1 = new PrintWriter(new File("task1.txt"))
    //<key, value> = <code, (byte,1)>

    val covertCode = data.map(s => (s.split("\t")(5), (s.split("\t")(6).toDouble, 1)))
    //<key, value> = <code, <sum of byte, sum of count>>
    val avgByte =covertCode.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(s => (s._1, s._2._1 / s._2._2))
      .map(x => "Code " + x._1.toString + ", average number of bytes = " + x._2.toString).collect()
    avgByte.foreach( value => {
      pwTask1.write(value + "\n")
    })

    // self join
    /*
        a. t1.host = t2.host
        b. t1.url = t2.url
        c. |t1.timestamp – t2.timestamp| <= 3600
        d. t1 != t2
    */
    //<key, value> = <(url, host), s>
    val pwTask2 = new PrintWriter(new File("task2.txt"))
    val joinData = data.map(s => ((s.split("\t")(0), s.split("\t")(4)), s))
    val joinData_joined = joinData.join(joinData)
    val joinData_filter = joinData_joined.filter(s => s._2._1 != s._2._2)
      .filter(s => math.abs(s._2._1.split("\t")(2).toLong - s._2._2.split("\t")(2).toLong) <= 3600 )
      .filter( s => s._2._1.split("\t")(1) == s._2._2.split("\t")(1))
      .filter(s =>  s._2._1.split("\t")(4) == s._2._2.split("\t")(4))
    val output = joinData_filter.map( s => s._2._1 + "\t" + s._2._2).collect()
    output.foreach( value => {
      pwTask2.write(value + "\n")
    })

    pwTask1.close()
    pwTask2.close()

  }
}

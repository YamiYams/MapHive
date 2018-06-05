package com.infy

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.hive.jdbc.HiveDriver
import java.sql.DriverManager

object MapHive {

  case class Employee(empId: Int, empName: String, salary: Int, DateOfJoining: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-DataFrame-Joins").setMaster("local[1]")
    val sc = new SparkContext(conf)

    // println(sc.parallelize(List(1,23,3), 2).count())

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val emp = sc.parallelize(Seq(
      Employee(1, "Revanth", 10000, "22-02-2012"),
      Employee(2, "Shyam", 15000, "10-06-2013"),
      Employee(3, "Ravi", 10500, "01-02-2017"),
      Employee(4, "Ganesh", 20000, "06-02-2011"),
      Employee(5, "Revanth Reddy", 23000, "14-08-2009"),
      Employee(6, "Hari", 12000, "20-05-2012"),
      Employee(7, "Hari Prasad", 20000, "19-11-2000")), 4)

    val empDF = emp.map(x => Employee(x.empId, x.empName, x.salary, x.DateOfJoining)).toDF()
    
    empDF.show()

    val mapped = empDF.mapPartitions(
      iter => {
        val hiveurl = "jdbc:hive2://bbsr02cloud09.ad.infosys.com:2181,bbsr02cloud10.ad.infosys.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" 
        val conn = DriverManager.getConnection(hiveurl,"praneethraj_n","g5jRhbJn")
        
        val newItr = iter.map(record => conn.createStatement().execute("INSERT INTO TABLE default.mytable VALUES (" + record + ")"))
        conn.commit()
        conn.close()
        newItr.toIterator
      })
      mapped.collect()

  }

}
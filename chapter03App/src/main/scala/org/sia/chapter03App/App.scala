package org.sia.chapter03App

import scala.io.Source.fromFile

import org.apache.spark.sql.SparkSession

/**
 * @author satya
 */
object App {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    println("running the application")
    val homeDir = System.getenv("HOME");
    val inputPath = homeDir + "/sia/github-archive/2015-03-01-0.json"
    val ghLog = spark.read.json(inputPath)
    val pushes = ghLog.filter("type == 'PushEvent'") // identify key value
    
   // pushes.printSchema
    println("all events: " + ghLog.count)
    println("only pushes: " + pushes.count)
    //pushes.show(5)
    //grouping all the users with push events
    val grouped = pushes.groupBy("actor.login").count
    //ordering in descending to know who had max pushes
    val ordered = grouped.orderBy(grouped("count").desc)
   
    //creating a set with all the employees to remove the non employees from ordered
    val empPath = homeDir +"/spark-in-action/first-edition/ch03/ghEmployees.txt"
    val employees = Set() ++ {    //2 sets can be added(duplicates will be removed though)
    
      for{
        line <- fromFile(empPath).getLines
      } yield line.trim  //creating a SET collection behind the scenes like a buffer, to return after iteration is done.
    }
    
    import spark.implicits._
    val bcEmployees = sc.broadcast(employees)
    val isEmp = user => bcEmployees.value.contains(user) //user is like a parameter for the isEmp user defined function
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
    

  }

}

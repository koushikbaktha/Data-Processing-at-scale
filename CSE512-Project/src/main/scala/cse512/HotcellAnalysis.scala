package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  //pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))

  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("pickup")
  //pickupInfo.show()

  // Register function to return correct neighbor count
  spark.udf.register("getNeighborCount",(xStr:String, yStr:String, zStr:String)=>(HotcellUtils.getNeighborCount(xStr, yStr, zStr)))

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // Compute total number of points
  val numPoints = spark.sql("select count(*) from pickup")
  val numPointsVal = numPoints.collect()(0).mkString("").toInt

  // Compute average points per cell and square of the average for Big S computation
  val xBar = numPointsVal / numCells
  val xBarSquared = xBar * xBar

  // Find point counts for each cell
  val cellCounts = spark.sql("select x,y,z, count(*) as count from pickup group by x,y,z").persist()
  //cellCounts.show()
  cellCounts.createOrReplaceTempView("cells")

  // Compute the sum of the points squared for use in the Big S computation
  val sumSquared = spark.sql("select sum(count * count) as sumSquared from cells")
  val sumSquaredValue = sumSquared.collect()(0).mkString("").toInt

  // Compute Big S
  val bigS = math.sqrt(sumSquaredValue / numCells - xBarSquared)

  // Perform cross join on cells to get sum of all neighbor counts for each cell
  val neighborCells = spark.sql("select cells1.x, cells1.y, cells1.z, getNeighborCount(cells1.x, cells1.y, cells1.z) as neighborcount, " +
    "sum(cells2.count) as thesum " +
    "from cells as cells1 cross join cells as cells2 " +
    "where (abs(cells2.x - cells1.x) <= 1  and abs(cells2.y - cells1.y) <= 1 and abs(cells2.z - cells1.z) <= 1)" +
    "group by cells1.x,cells1.y,cells1.z,neighborcount").persist()
  neighborCells.createOrReplaceTempView("neighborcells")
  //neighborCells.show(50, false)

  // Compute G Score
  val gscore = spark.sql("select x,y,z, thesum, ((thesum - " + xBar + " * neighborcount) / (" + bigS + " * sqrt((" + numCells + " * neighborcount - neighborcount * neighborcount) / (" + numCells + " - 1)))) as gscore from neighborcells")
  //gscore.show(50, false)
  gscore.createOrReplaceTempView("gScore")

  // Create table of point values ordered by gscore
  val finished = spark.sql("select  x,y,z from gScore order by gscore desc, x desc, y asc, z desc").persist()
  finished.show(50, false)

  println(minX)
  println(maxX)
  println(minY)
  println(maxY)
  println(numCells)

  // YOU NEED TO CHANGE THIS PART

  return finished // YOU NEED TO CHANGE THIS PART
}
}

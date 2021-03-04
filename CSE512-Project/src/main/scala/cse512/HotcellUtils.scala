package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def getNeighborCount(xStr:String, yStr:String, zStr:String): String = {

    val x = xStr.toInt
    val y = yStr.toInt
    val z = zStr.toInt

      if ((x == minX  || x == maxX ) && (y == minY || y == maxY) && (z == minZ || z == maxZ))
      {
          return "8"
      }

    if (((x == maxX || x == minX) && y != maxY && y != minY && (z == maxZ || z == minZ)) || ((y == maxY || y == minY) && x != maxX && x != minX && (z == maxZ || z == minZ)) || ((x == minX || x == maxX) && (y == minY || y == maxY) && (z != minZ && z != maxZ)))
      {
          return "12"
      }

    if (((x == maxX || x == minX) && y !=  maxY && y != minY && (z != maxZ && z != minZ)) || ((y == maxY || y == minY) && x != maxX && x != minX && (z != maxZ && z != minZ)) || (y != maxY && y != minY && x != maxX && x != minX && (z == maxZ || z == minZ)))
      {
          return "18"
      }

    return "27"
  }

}

package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val xPoint = pointString.substring(0, pointString.indexOf(',')).toDouble
    val yPoint = pointString.substring(pointString.indexOf(',') + 1).toDouble

    val rectXPoint1 = queryRectangle.substring(0, queryRectangle.indexOf(',')).toDouble
    var newQueryRectangle = queryRectangle.substring(queryRectangle.indexOf(',') + 1)
    val rectYPoint1 = newQueryRectangle.substring(0, newQueryRectangle.indexOf(',')).toDouble
    newQueryRectangle = newQueryRectangle.substring(newQueryRectangle.indexOf(',') + 1)
    val rectXPoint2 = newQueryRectangle.substring(0, newQueryRectangle.indexOf(',')).toDouble
    val rectYPoint2 = newQueryRectangle.substring(newQueryRectangle.indexOf(',') + 1).toDouble

    var leftX: Double = 0;
    var topY: Double = 0;
    var rightX: Double = 0;
    var bottomY: Double = 0;

    if (rectXPoint1 <= rectXPoint2)
    {
      leftX = rectXPoint1
      rightX = rectXPoint2
    }
    else
    {
      leftX = rectXPoint2
      rightX = rectXPoint1
    }

    if (rectYPoint1 <= rectYPoint2)
    {
      bottomY = rectYPoint1
      topY = rectYPoint2
    }
    else
    {
      topY = rectYPoint1
      bottomY = rectYPoint2
    }

    if (xPoint < leftX || xPoint > rightX || yPoint < bottomY || yPoint > topY)
      return false
    else
      return true
  }

}

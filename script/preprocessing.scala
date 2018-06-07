import scala.math.acos
import scala.math.cos
import scala.math.sin
import scala.math.{toRadians => radians}

import scala.io.Source

def getDistance(gps: List[Double], origin: List[Double]): Double = { 
  var distance = (6371*acos(cos(radians(gps(0)))*cos(radians(origin(0)))*cos(radians(origin(1))-radians(gps(1)))+sin(radians(gps(0)))*sin(radians(origin(0)))))
  return distance
}

def isInCircle (gps: List[Double], origin: List[Double]): Boolean = { 
 var distance = getDistance(gps, origin)

 if (distance <= 0.110) {
   return true
 }
 else {
   return false
 }

}


// val inputData = sc.parallelize(List("37.535242, 127.134268", "37.535339, 127.133792", "37.535428, 127.134251", "36.745938, 128.021290")) 
val sourceFileName = "frequencyAccident.csv"
val resultDir = "result/"
val sourceDir = "source/"
val data = sc.textFile(sourceDir+"DTG/*.gz", '\n') 

val now = System.currentTimeMillis();

val DTG_GPS = data.map(line => line.split("\\|")).map(arr => (Array(arr(2), arr(3), arr(4)).mkString("_"), List(arr(13).toInt/1000000.0, arr(12).toInt/1000000.0), arr(21))) 


try {
  for (line <- Source.fromFile(sourceDir + sourceFileName).getLines()) {
    var partial = line.split(",")
    var location = List(partial(13).toDouble, partial(14).toDouble)
    var result = DTG_GPS.filter(gps => isInCircle(location, gps._2))
    result.saveAsTextFile(resultDir + now + "/" + location.mkString("_"))
  }
} catch {
  case ex: Exception => println(ex)
}



// test loop data
// when in real it should be made by reading file
// val loopArr = Array(
//   List(37.535242, 127.134268),
//   List(37.535339, 127.133792), 
//   List(36.745939, 128.021290) 
//   );
//
//
//
// for (location <- loopArr) {
//   var result = DTG_GPS.filter(gps => isInCircle(location, gps._2))
//   result.saveAsTextFile("./result/" + location.mkString("_"))
// }
//

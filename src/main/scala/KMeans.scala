import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Math._;

object KMeans {
  type point = (Double, Double);
  var centroids: Array[point] = Array[point]()
  var points : Array[point] = Array[point]()
  var array : Array[(point, point)] = Array[(point, point)]()


  def centroidLocate(array : Array[point], p : point) : point = 
  {
    var minDistance : Double = 999999;
    var euclidean : Double = 999999;
    var index : Int = 999999;
    var count : Int = 0;

    for(x <- array) {
      euclidean = Math.sqrt(((x._1-p._1)*(x._1 - p._1)) + ((x._2-p._2)*(x._2-p._2)));
      if(euclidean < minDistance) {
        minDistance = euclidean; 
        index = count;
      }
      count += 1
    }
    
    return (array(index)._1, array(index)._2)
  }

  
def reduce(centroid : point , points : Seq[point]) : point = 
  {
    
    var count : Int = 0;
    var sValue : Double = 0;
    var yValue : Double = 0;

    for(i <- points){
      count += 1 ;  
      sValue += i._1 ;
      yValue += i._2 ;
    }
    return (sValue/count, yValue/count);


  }

  def main(args: Array[ String ]) {
    /* ... */
    val conf = new SparkConf().setAppName("Kmeans");
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf);
    
    
    points  = sc.textFile(args(0)).collect.map( x => {val a = x.split(",") 
                                              (a(0).toDouble, a(1).toDouble)});
    centroids = sc.textFile(args(1)).collect.map( line => { val a = line.split(",")
                                                    (a(0).toDouble,a(1).toDouble)})
    
                                                    
    for ( i <- 1 to 5 ){
    array = points.map(p => {((centroidLocate(centroids, p), (p._1,p._2)))});
    var d = sc.parallelize(array)
    var map = d.mapValues(value => (value,1))
    var newCentroids = map.reduceByKey{ case ((x1, x2), (y1, y2)) => (((x1._1 + y1._1),(x1._2 + y1._2)), (x2 + y2)) }             
                 .mapValues{
                  case (x, y) => (x._1/y, x._2/y) }
                 .collect
    centroids = newCentroids.map(a => (a._2))
    }
    
    centroids.foreach(println)
  }
}
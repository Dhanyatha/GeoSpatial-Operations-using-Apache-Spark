package DdsProject.GeospatialOperations;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate; 

class Point implements Comparable<Point>,Serializable 
{
	/****/
	private static final long serialVersionUID = 1L;
	public final double x, y;
	
	public Point(double x,double y)
	{
		this.x=x;
		this.y=y;
	}

	public int compareTo(Point p) {
		if (this.x == p.x) {
			return (int) (this.y - p.y);
		} else {
			return (int)(this.x - p.x);
		}
	} 
	public String toString() {
		return "("+x + "," + y+")";
	}
	@Override
	public boolean equals(Object o){
		  if(o instanceof Point){
			  Point toCompare = (Point) o;
		    return ((this.x==toCompare.x))&&((this.y==toCompare.y));
		  }
		  return false;
		}
	}

class Pair implements Serializable
{
  private static final long serialVersionUID = 1L;
  public Point point1 = null;
  public Point point2 = null;
  public double distance = 0.0;

  public Pair()
  {  }

  public Pair(Point point1, Point point2)
  {
    this.point1 = point1;
    this.point2 = point2;
    calcDistance();
  }

  public void update(Point point1, Point point2, double distance)
  {
    this.point1 = point1;
    this.point2 = point2;
    this.distance = distance;
  }

  public void calcDistance()
  {  
	  double xdist = point2.x - point1.x;
	  double ydist = point2.y - point1.y;
	  this.distance =   Math.hypot(xdist, ydist); 
  }

  public String toString()
  {  return point1 + "-" + point2 + " : " + distance;  }
}

public class closestPair 
{
	public static void sortByX(List<? extends Point> points)
	  {
	    Collections.sort(points, new Comparator<Point>() {
	        public int compare(Point point1, Point point2)
	        {
	          if (point1.x < point2.x)
	            return -1;
	          if (point1.x > point2.x)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	 
	  public static void sortByY(List<? extends Point> points)
	  {
	    Collections.sort(points, new Comparator<Point>() {
	        public int compare(Point point1, Point point2)
	        {
	          if (point1.y < point2.y)
	            return -1;
	          if (point1.y > point2.y)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	 
	  public static double distance(Point p1, Point p2)
	  {
	    double xdist = p2.x - p1.x;
	    double ydist = p2.y - p1.y;
	    return Math.hypot(xdist, ydist);
	  }
	 
	  public static Pair divideAndConquer(List<? extends Point> points)
	  {
	    List<Point> pointsSortedByX = new ArrayList<Point>(points);
	    sortByX(pointsSortedByX);
	    List<Point> pointsSortedByY = new ArrayList<Point>(points);
	    sortByY(pointsSortedByY);
	    return divideAndConquer(pointsSortedByX, pointsSortedByY);
	  }
	  public static Pair bruteForce(List<? extends Point> points)
	  {
	    int numPoints = points.size();
	    if (numPoints < 2)
	      return null;
	    Pair pair = new Pair(points.get(0), points.get(1));
	    if (numPoints > 2)
	    {
	      for (int i = 0; i < numPoints - 1; i++)
	      {
	        Point point1 = points.get(i);
	        for (int j = i + 1; j < numPoints; j++)
	        {
	          Point point2 = points.get(j);
	          double distance = distance(point1, point2);
	          if (distance < pair.distance)
	            pair.update(point1, point2, distance);
	        }
	      }
	    }
	    return pair;
	  }
	 
	  private static Pair divideAndConquer(List<? extends Point> pointsSortedByX, List<? extends Point> pointsSortedByY)
	  {
	    int numPoints = pointsSortedByX.size();
	    if (numPoints <= 3)
	      return bruteForce(pointsSortedByX);
	 
	    int dividingIndex = numPoints >>> 1;
	    List<? extends Point> leftOfCenter = pointsSortedByX.subList(0, dividingIndex);
	    List<? extends Point> rightOfCenter = pointsSortedByX.subList(dividingIndex, numPoints);
	 
	    List<Point> tempList = new ArrayList<Point>(leftOfCenter);
	    sortByY(tempList);
	    Pair closestPair = divideAndConquer(leftOfCenter, tempList);
	 
	    tempList.clear();
	    tempList.addAll(rightOfCenter);
	    sortByY(tempList);
	    Pair closestPairRight = divideAndConquer(rightOfCenter, tempList);
	 
	    if (closestPairRight.distance < closestPair.distance)
	      closestPair = closestPairRight;
	 
	    tempList.clear();
	    double shortestDistance =closestPair.distance;
	    double centerX = rightOfCenter.get(0).x;
	    for (Point point : pointsSortedByY)
	      if (Math.abs(centerX - point.x) < shortestDistance)
	        tempList.add(point);
	 
	    for (int i = 0; i < tempList.size() - 1; i++)
	    {
	      Point point1 = tempList.get(i);
	      for (int j = i + 1; j < tempList.size(); j++)
	      {
	        Point point2 = tempList.get(j);
	        if ((point2.y - point1.y) >= shortestDistance)
	          break;
	        double distance = distance(point1, point2);
	        if (distance < closestPair.distance)
	        {
	          closestPair.update(point1, point2, distance);
	          shortestDistance = distance;
	        }
	      }
	    }
	    return closestPair;
	  }
	 
    public static void main( String[] args )
    {
        System.out.println( "Starting the main method!" );
        
        SparkConf conf = new SparkConf().setAppName("App").setMaster("spark://10.0.0.4:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);		
		JavaRDD<String> lines = sc.textFile("hdfs://master:54310/content/FarthestPairandClosestPairTestData.csv");
		System.out.println("RDD created from external file...calling local function..............");
		JavaRDD<Point> linelengths = lines.mapPartitions(new FlatMapFunction<Iterator<String>,Point>(){
		private static final long serialVersionUID = 1L;
		
		public Iterable<Point> call(Iterator<String> s) 
			{
				
				List<Point> points=new ArrayList<Point>();
				List<Coordinate> ActiveCoords = new ArrayList<Coordinate>();
				List<Point> finalPairAndCOnvexHull=new ArrayList<Point>();
				GeometryFactory geom = new GeometryFactory();
				while(s.hasNext())
				{
					String strTemp = s.next();
					String[] fields=strTemp.split(",");
					Point x=new Point(Double.parseDouble(fields[0]),Double.parseDouble(fields[1]));
					Coordinate coord = new Coordinate(Double.parseDouble(fields[0]),Double.parseDouble(fields[1]));
					ActiveCoords.add(coord);
					points.add(x);
				}
				// Find Convex Hull
				ConvexHull ch = new ConvexHull(ActiveCoords.toArray(new Coordinate[ActiveCoords.size()]), geom);
				Geometry g=ch.getConvexHull();
				List<Coordinate> localConvexHull =  Arrays.asList(g.getCoordinates());
				//Find local closest pairs
				System.out.println("Sending partition to divide and conquer method");
				Pair localClosestPair=divideAndConquer(points);
				System.out.println("The closest Pair in this partition is..");
				System.out.println(" point 1 "+localClosestPair.point1.x+ " "+localClosestPair.point1.y);
				System.out.println(" point 2 "+localClosestPair.point2.x+ " "+localClosestPair.point2.y);
				
				System.out.println(localConvexHull.contains(new Coordinate(localClosestPair.point1.x,localClosestPair.point1.y)));
				System.out.println(localConvexHull.contains(new Coordinate(localClosestPair.point2.x,localClosestPair.point2.y)));
				
				if(!localConvexHull.contains(new Coordinate(localClosestPair.point1.x,localClosestPair.point1.y)))
					finalPairAndCOnvexHull.add(localClosestPair.point1);
				if(!localConvexHull.contains(new Coordinate(localClosestPair.point2.x,localClosestPair.point2.y)))	
					finalPairAndCOnvexHull.add(localClosestPair.point2);
				int siz=localConvexHull.size();
				for(int i=0;i<siz-1;i++)
				{
					Coordinate c =localConvexHull.get(i);
					finalPairAndCOnvexHull.add(new Point(c.x,c.y));
				}
				return finalPairAndCOnvexHull;
			}
		});
		
		System.out.println("Local Closest Pair Done....................................");
		linelengths.saveAsTextFile("hdfs://master:54310/content/ClosestPairPartial");
		JavaRDD<Point> ReduceList = linelengths.repartition(1);
		JavaRDD<Point> FinalList = ReduceList.mapPartitions(new FlatMapFunction<Iterator<Point>, Point>()
		{
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<Point> call(Iterator<Point> givListIter)
			{
				List<Point> points=new ArrayList<Point>();
				while(givListIter.hasNext())
				{
					Point p = givListIter.next();
					points.add(p);
				}
				Pair globalClosestPair=divideAndConquer(points);
				List<Point> finalPoints=new ArrayList<Point>();
				finalPoints.add(globalClosestPair.point1);
				finalPoints.add(globalClosestPair.point2);
				return finalPoints;
			}
		});
		FinalList.saveAsTextFile("hdfs://master:54310/content/ClosestPairResults");
		sc.close();
    }
}
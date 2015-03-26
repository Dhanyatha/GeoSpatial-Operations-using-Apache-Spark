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
	public final double xco, yco;
	
	public Point(double x,double y)
	{
		this.xco=x;
		this.yco=y;
	}
	public int compareTo(Point p) {
		if (this.xco == p.xco) {
			return (int) (this.yco - p.yco);
		} else {
			return (int)(this.xco - p.xco);
		}
	} 
	public String toString() 
	{
		return "("+xco + "," + yco+")";
	}
	@Override
	public boolean equals(Object o)
	{
		  if(o instanceof Point)
		  {
			  Point toCompare = (Point) o;
		    return ((this.xco==toCompare.xco))&&((this.yco==toCompare.yco));
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
    calculateDistance();
  }

  public void update(Point point1, Point point2, double distance)
  {
    this.point1 = point1;
    this.point2 = point2;
    this.distance = distance;
  }

  public void calculateDistance()
  {  
	  double xdistance = point2.xco - point1.xco;
	  double ydistance = point2.yco - point1.yco;
	  this.distance =   Math.hypot(xdistance, ydistance); 
  }

  public String toString()
  {  return point1 + "-" + point2 + " : " + distance;  }
}

public class closestPair 
{
	public static void sortByXcoordinate(List<? extends Point> points)
	  {
	    Collections.sort(points, new Comparator<Point>() {
	        public int compare(Point point1, Point point2)
	        {
	          if (point1.xco < point2.xco)
	            return -1;
	          if (point1.xco > point2.xco)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	 
	  public static void sortByYcoordinate(List<? extends Point> points)
	  {
	    Collections.sort(points, new Comparator<Point>() {
	        public int compare(Point point1, Point point2)
	        {
	          if (point1.yco < point2.yco)
	            return -1;
	          if (point1.yco > point2.yco)
	            return 1;
	          return 0;
	        }
	      }
	    );
	  }
	 
	  public static double pairdistance(Point p1, Point p2)
	  {
	    double xdistance = p2.xco - p1.xco;
	    double ydistance = p2.yco - p1.yco;
	    return Math.hypot(xdistance, ydistance);
	  }
	 
	  public static Pair divideAndConquerBase(List<? extends Point> listOfPoints)
	  {
	    List<Point> pointsSortedByXcoordinate = new ArrayList<Point>(listOfPoints);
	    sortByXcoordinate(pointsSortedByXcoordinate);
	    List<Point> pointsSortedByYcoordinate = new ArrayList<Point>(listOfPoints);
	    sortByYcoordinate(pointsSortedByYcoordinate);
	    return divideAndConquerRecursionMethod(pointsSortedByXcoordinate, pointsSortedByYcoordinate);
	  }
	  public static Pair bruteForceApproach(List<? extends Point> listOfPoints)
	  {
	    int numOfPoints = listOfPoints.size();
	    if (numOfPoints < 2)
	      return null;
	    Pair bruteForcePair = new Pair(listOfPoints.get(0), listOfPoints.get(1));
	    if (numOfPoints > 2)
	    {
	      for (int i = 0; i < numOfPoints - 1; i++)
	      {
	        Point point1 = listOfPoints.get(i);
	        for (int j = i + 1; j < numOfPoints; j++)
	        {
	          Point point2 = listOfPoints.get(j);
	          double distance = pairdistance(point1, point2);
	          if (distance < bruteForcePair.distance)
	        	  bruteForcePair.update(point1, point2, distance);
	        }
	      }
	    }
	    return bruteForcePair;
	  }
	 
	  private static Pair divideAndConquerRecursionMethod(List<? extends Point> pointsSortedByXcoordinate, List<? extends Point> pointsSortedByYcoordinate)
	  {
	    int numOfPoints = pointsSortedByXcoordinate.size();
	    if (numOfPoints <= 3)
	      return bruteForceApproach(pointsSortedByXcoordinate);
	 
	    int dividingPoint = numOfPoints >>> 1;
	    List<? extends Point> leftPart = pointsSortedByXcoordinate.subList(0, dividingPoint);
	    List<? extends Point> rightPart = pointsSortedByXcoordinate.subList(dividingPoint, numOfPoints);
	 
	    List<Point> temporaryList = new ArrayList<Point>(leftPart);
	    sortByYcoordinate(temporaryList);
	    Pair closestPair = divideAndConquerRecursionMethod(leftPart, temporaryList);
	 
	    temporaryList.clear();
	    temporaryList.addAll(rightPart);
	    sortByYcoordinate(temporaryList);
	    Pair closestPairRight = divideAndConquerRecursionMethod(rightPart, temporaryList);
	 
	    if (closestPairRight.distance < closestPair.distance)
	      closestPair = closestPairRight;
	 
	    temporaryList.clear();
	    double shortestDistance =closestPair.distance;
	    double centerX = rightPart.get(0).xco;
	    for (Point point : pointsSortedByYcoordinate)
	      if (Math.abs(centerX - point.xco) < shortestDistance)
	    	  temporaryList.add(point);
	 
	    for (int i = 0; i < temporaryList.size() - 1; i++)
	    {
	      Point point1 = temporaryList.get(i);
	      for (int j = i + 1; j < temporaryList.size(); j++)
	      {
	        Point point2 = temporaryList.get(j);
	        if ((point2.yco - point1.yco) >= shortestDistance)
	          break;
	        double distance = pairdistance(point1, point2);
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
        //SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
        SparkConf conf = new SparkConf().setAppName("App").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);	
		//JavaRDD<String> lines = sc.textFile("/home/worker/FarthestPairandClosestPairTestData.csv");
		JavaRDD<String> lines = sc.textFile(args[1]);
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
				Pair localClosestPair=divideAndConquerBase(points);
				System.out.println("The closest Pair in this partition is..");
				System.out.println(" point 1 "+localClosestPair.point1.xco+ " "+localClosestPair.point1.yco);
				System.out.println(" point 2 "+localClosestPair.point2.xco+ " "+localClosestPair.point2.yco);
				
				System.out.println(localConvexHull.contains(new Coordinate(localClosestPair.point1.xco,localClosestPair.point1.yco)));
				System.out.println(localConvexHull.contains(new Coordinate(localClosestPair.point2.xco,localClosestPair.point2.yco)));
				
				if(!localConvexHull.contains(new Coordinate(localClosestPair.point1.xco,localClosestPair.point1.yco)))
					finalPairAndCOnvexHull.add(localClosestPair.point1);
				if(!localConvexHull.contains(new Coordinate(localClosestPair.point2.xco,localClosestPair.point2.yco)))	
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
		//linelengths.saveAsTextFile("/home/worker/ClosestPairPartial");
		linelengths.saveAsTextFile(args[2]);
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
				Pair globalClosestPair=divideAndConquerBase(points);
				List<Point> finalPoints=new ArrayList<Point>();
				finalPoints.add(globalClosestPair.point1);
				finalPoints.add(globalClosestPair.point2);
				return finalPoints;
			}
		});
		FinalList.saveAsTextFile(args[3]);
		//FinalList.saveAsTextFile("/home/worker/ClosestPairResults");
		sc.close();
    }
}
package DdsProject.GeospatialOperations;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate; 

public class farthestPair 
{ 
    public static void main( String[] args )
    {
        System.out.println( "Starting the main method!" );
        
        SparkConf conf = new SparkConf().setAppName("App").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);		
		JavaRDD<String> lines = sc.textFile("/home/worker/Demo.csv");
		System.out.println("RDD created from external file...calling local function..............");
		JavaRDD<Point> linelengths = lines.mapPartitions(new FlatMapFunction<Iterator<String>,Point>(){
		private static final long serialVersionUID = 1L;
		public Iterable<Point> call(Iterator<String> s) 
		{
				List<Point> points=new ArrayList<Point>();
				List<Coordinate> ActiveCoords = new ArrayList<Coordinate>();
				List<Point> farthestPair=new ArrayList<Point>();
				GeometryFactory geom = new GeometryFactory();
				while(s.hasNext())
				{
					String strTemp = s.next();
					String[] fields=strTemp.split(",");
					Point x=new Point(Double.parseDouble(fields[1]),Double.parseDouble(fields[2]));
					Coordinate coord = new Coordinate(Double.parseDouble(fields[1]),Double.parseDouble(fields[2]));
					ActiveCoords.add(coord);
					points.add(x);
				}
				// Find Convex Hull
				ConvexHull ch = new ConvexHull(ActiveCoords.toArray(new Coordinate[ActiveCoords.size()]), geom);
				Geometry g=ch.getConvexHull();
				List<Coordinate> localConvexHull =  Arrays.asList(g.getCoordinates());
				//Find local farthest pairs
				System.out.println("The farthest Pair in this partition is..");
				Coordinate p1,p2;
				p1=localConvexHull.get(0);
				p2=localConvexHull.get(1);
				
				double maxDistance=0;
				int convexHullSize=localConvexHull.size();
				for(int i=0;i<convexHullSize-1;i++)
				{
					double currentDistance=Math.sqrt((localConvexHull.get(i).y)*(localConvexHull.get(i+1).y) +(localConvexHull.get(i).x)*(localConvexHull.get(i+1).x));
					if(currentDistance>maxDistance)
					{
						maxDistance=currentDistance;
						p1=localConvexHull.get(i);
						p2=localConvexHull.get(i+1);
					}
				}
				
				farthestPair.add(new Point(p1.x,p1.y));
				farthestPair.add(new Point(p2.x,p2.y));
				return farthestPair;
			}
		});
		
		System.out.println("Local Farthest Pair Done....................................");
		linelengths.saveAsTextFile("/home/worker/a1");
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
				double maxDistance=0;
				Point p1,p2;
				p1=points.get(0);
				p2=points.get(1);
				int convexHullSize=points.size();
				for(int i=0;i<convexHullSize-1;i++)
				{
					double currentDistance=Math.sqrt((points.get(i).y)*(points.get(i+1).y) +(points.get(i).x)*(points.get(i+1).x));
					if(currentDistance>maxDistance)
					{
						maxDistance=currentDistance;
						p1=points.get(i);
						p2=points.get(i+1);
					}
				}
				
				
				List<Point> finalPoints=new ArrayList<Point>();
				finalPoints.add(p1);
				finalPoints.add(p2);
				return finalPoints;
			}
		});
		FinalList.saveAsTextFile("/home/worker/a2");
		System.out.println("The farthest pair of points is...");
		sc.close();
    }
}
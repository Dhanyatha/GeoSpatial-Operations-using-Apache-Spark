package geospatial.convexHull;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;

//This calcualtes the convex hulls locally
class LocalHull implements FlatMapFunction<Iterator<String>, Coordinate>, Serializable
{
	private static final long serialVersionUID = 1L;

	//Iterate over the points to calcualte the convex hull
	public Iterable<Coordinate> call(Iterator<String> s)
	{
		List<Coordinate> ActiveCoords = new ArrayList<Coordinate>();
		GeometryFactory geom = new GeometryFactory();
		try{
			while(s.hasNext())
			{
				//Read the points
				String strTemp = s.next();
				String[] CoordList = strTemp.split(",");
				Double x1 = Double.parseDouble(CoordList[0]);
				Double y1 = Double.parseDouble(CoordList[1]);
				Coordinate coord = new Coordinate(x1,y1);
				
				//Add the point to the list of Active Coordinates
				ActiveCoords.add(coord);
			}}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		ConvexHull ch = new ConvexHull(ActiveCoords.toArray(new Coordinate[ActiveCoords.size()]), geom);
		Geometry g=ch.getConvexHull();
		Coordinate[] c= g.getCoordinates();

		//Convert the coordinates array to arraylist here
		List<Coordinate> a = Arrays.asList(c);
		return a;
	}
}

//This calculates the global hull using the list of the local hulls.
class GlobalHull implements FlatMapFunction<Iterator<Coordinate>, Coordinate>, Serializable
{
	private static final long serialVersionUID = 1L;

	//Iterates over all the partitions and iteratively calcualtes the convex hull
	public Iterable<Coordinate> call(Iterator<Coordinate> givListIter)
	{
		List<Coordinate> polList = new ArrayList<Coordinate>();
		GeometryFactory geom = new GeometryFactory();
		while(givListIter.hasNext())
		{
			Coordinate tempPol = givListIter.next();
			polList.add(tempPol);
		}
		ConvexHull ch = new ConvexHull(polList.toArray(new Coordinate[polList.size()]), geom);
		Geometry g=ch.getConvexHull();
		Coordinate[] c= g.getCoordinates();

		//Convert the coordinates array to arraylist here
		List<Coordinate> a = Arrays.asList(c);
		return a;
	}
}

public class App
{
	public static void main(String[] args) throws ClassNotFoundException
	{
		SparkConf conf = new SparkConf().setAppName("App").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[1]);
		JavaRDD<Coordinate> MappedPolygons = lines.mapPartitions(new localHull());
		MappedPolygons.saveAsTextFile(args[2]);
		JavaRDD<Coordinate> ReduceList = MappedPolygons.repartition(1);
		JavaRDD<Coordinate> FinalList = ReduceList.mapPartitions(new globalHull());

		//Farthest Pair of Points Calcualtion
		List<Coordinate> convexHullList=FinalList.collect();
		Coordinate p1,p2;
		p1=convexHullList.get(0);
		p2=convexHullList.get(1);
		
		double maxDistance=0;
		int convexHullSize=convexHullList.size();

		//Compare each point on the hull with every other point on the hull
		for(int i=0;i<convexHullSize-1;i++)
		{
			for(int j=i+1;j<convexHullSize;j++)
			{
				double xsquare=(convexHullList.get(i).x-convexHullList.get(j).x)*(convexHullList.get(i).x-convexHullList.get(j).x);
				double ysquare=(convexHullList.get(i).y-convexHullList.get(j).y)*(convexHullList.get(i).y-convexHullList.get(j).y);
				double currentDistance=Math.sqrt(xsquare+ysquare);	

				//Update maxDistance if currentDistance>maxDistance
				if(currentDistance>maxDistance)
				{
					maxDistance=currentDistance;
					p1=convexHullList.get(i);
					p2=convexHullList.get(j);
				}
			}
			
			}


		List<Coordinate> p1p2=new ArrayList<Coordinate>();
		p1p2.add(p1);
		p1p2.add(p2);
		JavaRDD<Coordinate> finalpair=sc.parallelize(p1p2);
		finalpair.saveAsTextFile(args[3]);
		sc.close();
	}
}
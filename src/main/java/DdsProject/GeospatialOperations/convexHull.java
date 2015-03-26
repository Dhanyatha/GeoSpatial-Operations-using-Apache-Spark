package DdsProject.GeospatialOperations;
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

public class convexHull
{
	public static void main(String[] args) throws ClassNotFoundException
	{
		SparkConf conf = new SparkConf().setAppName("App").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[1]);
		JavaRDD<Coordinate> MappedPolygons = lines.mapPartitions(new LocalHull());
		MappedPolygons.saveAsTextFile(args[2]);
		JavaRDD<Coordinate> ReduceList = MappedPolygons.repartition(1);
		JavaRDD<Coordinate> FinalList = ReduceList.mapPartitions(new GlobalHull());
		FinalList.saveAsTextFile(args[3]);
		sc.close();
	}
}
package DdsProject.GeospatialOperations;


import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collection;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

class LocalUnion implements FlatMapFunction<Iterator<String>, Geometry>, Serializable
{
	public Iterable<Geometry> call(Iterator<String> s)
	{	
		List<Geometry> ActivePolygons = new ArrayList<Geometry>();
		List<Geometry> ReturnPolygons = new ArrayList<Geometry>();
		GeometryFactory geom = new GeometryFactory();

		while(s.hasNext())
		{
			String strTemp = s.next();
			String[] CoordList = strTemp.split(",");
			Double x1 = Double.parseDouble(CoordList[0]);
			Double y1 = Double.parseDouble(CoordList[1]);
			Double x2 = Double.parseDouble(CoordList[2]);
			Double y2 = Double.parseDouble(CoordList[3]);
			Geometry poly = geom.createPolygon(new Coordinate[]{new Coordinate(x1,y1),
					new Coordinate(x1,y2),
					new Coordinate(x2,y2),
					new Coordinate(x2,y1),
					new Coordinate(x1,y1)});
			ActivePolygons.add(poly);
		}
		Collection<Geometry> pol = ActivePolygons;
		CascadedPolygonUnion caspol = new CascadedPolygonUnion(pol);
		Geometry gPols = caspol.union();
		int nNumGeo = gPols.getNumGeometries();

		for(int nI=0;nI<nNumGeo;nI++)
		{
			Geometry pAdd = (Geometry) gPols.getGeometryN(nI);
			ReturnPolygons.add(pAdd);
		}
		return ReturnPolygons;
	}
}

class GlobalUnion implements FlatMapFunction<Iterator<Geometry>, Geometry>, Serializable
{
	public Iterable<Geometry> call(Iterator<Geometry> givListIter)
	{	
		List<Geometry> polList = new ArrayList<Geometry>();
		List<Geometry> ReturnPolygons = new ArrayList<Geometry>();
		while(givListIter.hasNext())
		{
			Geometry tempPol = givListIter.next();
			polList.add(tempPol);
		}

		Collection<Geometry> pol = polList;
		CascadedPolygonUnion caspol = new CascadedPolygonUnion(pol);
		Geometry gPols = caspol.union();
		int nNumGeo = gPols.getNumGeometries();

		for(int nI=0;nI<nNumGeo;nI++)
		{
			Geometry pAdd = (Geometry) gPols.getGeometryN(nI);
			ReturnPolygons.add(pAdd);
		}
		return ReturnPolygons;
	}
}

public class polygonUnion
{
	public static void main(String[] args) throws ClassNotFoundException
	{
		SparkConf conf = new SparkConf().setAppName("App").setMaster(args[0]);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[1]);
		JavaRDD<Geometry> MappedPolygons = lines.mapPartitions(new LocalUnion());
		MappedPolygons.saveAsTextFile(args[2]);
		JavaRDD<Geometry> ReduceList = MappedPolygons.repartition(1);
		JavaRDD<Geometry> FinalList = ReduceList.mapPartitions(new GlobalUnion());
		FinalList.saveAsTextFile(args[3]);
	}
}

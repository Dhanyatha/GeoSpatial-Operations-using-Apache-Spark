package DdsProject.GeospatialOperations;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

class LocalUnion implements FlatMapFunction<Iterator<String>, Geometry>, Serializable
{
	
	public Iterable<Geometry> call(Iterator<String> lines)
	{	
		List<Geometry> ActivePolygons = new ArrayList<Geometry>();
		List<Geometry> ReturnPolygons = new ArrayList<Geometry>();
		GeometryFactory geom = new GeometryFactory();
        Geometry geoPoly=null;

		lines.next();
		while(lines.hasNext())
		{
			String strTemp = lines.next();
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
		
        if(!ActivePolygons.isEmpty())
        {
        	geoPoly=ActivePolygons.get(0);
            ActivePolygons.remove(0);
        }
  
        for( int nI=0;!ActivePolygons.isEmpty();nI++)
        {
            if(geoPoly.intersects( ActivePolygons.get(nI)))
            {
            	geoPoly = geoPoly.union(ActivePolygons.get(nI));
                ActivePolygons.remove(nI);
                if(ActivePolygons.isEmpty())
                {
                	ReturnPolygons.add(geoPoly);
                    break;
                }   
                if(!ActivePolygons.isEmpty())
                {
                	nI--;
                }
                
                if(nI + 1== ActivePolygons.size())
                {
                	nI = 0;
                }             
            }
            else if( !ActivePolygons.isEmpty() && nI + 1==ActivePolygons.size() )
            {
            	
            	ReturnPolygons.add(geoPoly);
            	geoPoly=ActivePolygons.get(0);
                ActivePolygons.remove(0);
                if(!ActivePolygons.isEmpty())
                {
                	nI--;
                }
                if(ActivePolygons.isEmpty())
                {
                    break;
                }
                if(ActivePolygons.size() == 1)
                {
                	ReturnPolygons.add(geoPoly);
                }
            }
        }
		return ReturnPolygons;
	}
}

class GlobalUnion implements FlatMapFunction<Iterator<Geometry>, Geometry>, Serializable
{
	public Iterable<Geometry> call(Iterator<Geometry> givListIter)
	{	
		List<Geometry> ActivePolygons = new ArrayList<Geometry>();
		List<Geometry> ReturnPolygons = new ArrayList<Geometry>();
		Geometry geoPoly=null;
		
		while(givListIter.hasNext())
		{
			Geometry tempPol = givListIter.next();
			ActivePolygons.add(tempPol);
		}

        if(!ActivePolygons.isEmpty())
        {
        	geoPoly=ActivePolygons.get(0);
        	ActivePolygons.remove(0);
        }
  
        for( int nI=0;!ActivePolygons.isEmpty();nI++)
        {
            if(geoPoly.intersects(ActivePolygons.get(nI)) )
            {
            	geoPoly = geoPoly.union(ActivePolygons.get(nI));
                ActivePolygons.remove(nI);
                if(ActivePolygons.isEmpty())
                {
                	ReturnPolygons.add(geoPoly);
                    break;
                }   
                if(!ActivePolygons.isEmpty())
                {
                	nI--;
                }
     
                if(nI + 1 == ActivePolygons.size())
                {
                	nI = 0;
                }             
            }
            else if( !ActivePolygons.isEmpty() && nI + 1 ==ActivePolygons.size())
            {
            	
            	ReturnPolygons.add(geoPoly);
            	geoPoly=ActivePolygons.get(0);
                ActivePolygons.remove(0);
                if(ActivePolygons.size() == 1)
                {
                	ReturnPolygons.add(geoPoly);
                	break;
                }
                if(!ActivePolygons.isEmpty())
                {
                	nI--;
                }
                if(ActivePolygons.isEmpty())
                {
                    break;
                }
            }
        }
		return ReturnPolygons;
	}
}

public class App
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
package DdsProject.GeospatialOperations;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.*;


public class spatialJoinQuery {
	

	public static void main(String[] args) 
	{
		
		
		String[] broad;
		
		SparkConf conf=new SparkConf().setAppName("operation6").setMaster(args[0]);
		JavaSparkContext sc=new JavaSparkContext(conf);
		//sc.addJar("/home/karthik/Desktop/operation6.jar");
		//Reading First file
		JavaRDD<String> l2=sc.textFile(args[1]);
		List<String> s=l2.collect();
		String[] st= s.toArray(new String[0]);
		
		Broadcast<String[]> br=sc.broadcast(st);
		broad=br.value();
	    
	   final String ar[]=broad;
	   
	    System.out.println(ar[0]);
	    //Reading the second file
		l2=sc.textFile(args[2]);
		JavaPairRDD<String,String> j=l2.mapToPair(new PairFunction<String,String,String>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String data)
			{
				
				String x = "" ;
				String parts[]=data.split(",");
				String aid = "["+parts[1]+","+parts[2]+","+parts[3]+","+parts[4]+"]";
				double x1=Double.parseDouble(parts[1]);
				double y1=Double.parseDouble(parts[2]);
				double x2=Double.parseDouble(parts[3]);
				double y2=Double.parseDouble(parts[4]);
				
				
				
				for(String part: ar)
				{
					String str[]=part.split(",");
					
					double a1=Double.parseDouble(str[1]);
					double b1=Double.parseDouble(str[2]);
					double a2=Double.parseDouble(str[3]);
					double b2=Double.parseDouble(str[4]);
					
					if((Math.max(a1, a2) > Math.max(x1, x2))&&(Math.max(b1, b2) > Math.max(y1, y2))&&(Math.min(a1, a2) < Math.min(x1, x2))&&(Math.min(b1, b2) < Math.min(y1, y2)))
					{
						x = x + "["+str[1]+","+str[2]+","+str[3]+","+str[4]+"]";  
					}
				
				}
			
				return new Tuple2<String, String>(aid,x);
			}
		}).repartition(1);
		
	    j.saveAsTextFile(args[3]);
	    sc.stop();
	    sc.close();
	}
}
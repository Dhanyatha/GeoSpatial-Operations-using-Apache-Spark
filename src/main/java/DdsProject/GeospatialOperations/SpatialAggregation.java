package DdsProject.GeospatialOperations;

	import org.apache.spark.api.java.*;
	import org.apache.spark.api.java.function.*;
	import org.apache.spark.broadcast.Broadcast;
	import org.apache.spark.SparkConf;
	import scala.Tuple2;
	import java.util.*;


	public class SpatialAggregation {
		

		public static void main(String[] args) 
		{
			
			
			String[] broad;
			
			SparkConf conf=new SparkConf().setAppName("operation6").setMaster(args[0]);
			JavaSparkContext sc=new JavaSparkContext(conf);
			sc.addJar("/home/group16/Desktop/operation6.jar");
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
			JavaPairRDD<String,Integer> j=l2.mapToPair(new PairFunction<String,String,Integer>(){
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public Tuple2<String, Integer> call(String data)
				{
					
					//String x = "" ;
					int count = 0;
					String parts[]=data.split(",");
					String aid = "["+parts[0]+","+parts[1]+","+parts[2]+","+parts[3]+"]";
					double x1=Double.parseDouble(parts[0]);
					double y1=Double.parseDouble(parts[1]);
					double x2=Double.parseDouble(parts[2]);
					double y2=Double.parseDouble(parts[3]);
					
					
					
					for(String part: ar)
					{
						String str[]=part.split(",");
						
						double a1=Double.parseDouble(str[0]);
						double b1=Double.parseDouble(str[1]);
						
						if((a1 < Math.max(x1, x2))&&(b1 < Math.max(y1, y2))&&(a1 > Math.min(x1, x2))&&(b1 > Math.min(y1, y2)))
						{
							count++;
						}
					
					}
				
					return new Tuple2<String, Integer>(aid,count);
				}
			}).repartition(1);
			
		    j.saveAsTextFile(args[3]);
		    sc.stop();
		    sc.close();
		}
	
}

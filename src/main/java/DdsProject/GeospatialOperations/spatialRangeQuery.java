package DdsProject.GeospatialOperations;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
public class spatialRangeQuery 
{
	public static void main( String[] args )
    {
		//Double[] broad;
		SparkConf conf= new SparkConf().setAppName("App").setMaster(args[0]);
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	JavaRDD<String> rects = sc.textFile(args[1]);
    	String result=rects.first();
    	String[] windowArray = result.split(",");
    	Double[] window = new Double[4];
    	for(int i=0; i<4; i++)
    	{
    		window[i] = Double.parseDouble(windowArray[i]);
    	}
    	
    	
    	//String[] window = (String[]) windowArray.toArray();
    	//System.out.println(window[0]);
    	/*Double[] windowArray = new Double[4];
    	try
    	{
    	    //scanner = new Scanner(new File("hdfs://master:54310/content/window.csv"));
    		reader = new BufferedReader(new FileReader("hdfs://master:54310/content/window.csv"));
    	    String window = reader.readLine();
    	    String[] tempArray = window.split(",");
    	    
    	    for(int i=0; i<4; i++)
    	    {
    	    	windowArray[i] = Double.parseDouble(tempArray[i]);
    	    	System.out.println(windowArray[i]);
    	    }
    	    
    	    if(windowArray[0] > windowArray[2])
    	    {
    	    	double swap = windowArray[0];
    	    	windowArray[0] = windowArray[2];
    	    	windowArray[2] = swap;
    	    }
    	    if(windowArray[1] > windowArray[3])
    	    {
    	    	double swap = windowArray[1];
    	    	windowArray[1] = windowArray[3];
    	    	windowArray[3] = swap;
    	    }
    	    
    	}
    	catch(Exception e){System.out.println("*************FILE NOT FOUND EXCEPTION*****************");}
    	*///put coment here
    	
    	Broadcast<Double[]> br = sc.broadcast(window);
    	final Double[] broad = br.value();
    	
    	rects = sc.textFile(args[2]);
    	JavaPairRDD<String, String> enclosed = rects.mapToPair(new PairFunction<String, String, String>()
    			{

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(String data)
    		        {
    		        	String parts[] = data.split(",");
    		        	double x1=Double.parseDouble(parts[0]);
    					double y1=Double.parseDouble(parts[1]);
    					double x2=Double.parseDouble(parts[2]);
    					double y2=Double.parseDouble(parts[3]);
    					
    					double fx1=x1;
    					double fy1=y1;
    					double fx2=x2;
    					double fy2=y2;
    					
    					if(x1 > x2)
    					{
    						fx1 = x2;
    						fx2 = x1;
    					}
    					if(y1 > y2)
    					{
    						fy1 = y2;
    						fy2 = y1;
    					}
    					
    					if((fx1 >= broad[0] && fx2 <= broad[2]) && (fy1 >= broad[1] && fy2 <= broad[3]))
    					//if((fx1 >= br.value()[0] && fx2 <= br.value()[2]) && (fy1 >= br.value()[1] && fy2 <= br.value()[3]))
						{
							return new Tuple2<String, String> (x1+","+y1+","+x2+","+y2, "");
						}       
    					else
						    return new Tuple2<String, String> ("NULL", "b");
    		        }
    			});
    	
    	String data = "";
    	
    	List<Tuple2<String,String>> output = enclosed.collect();
    	List<String> srdd = new ArrayList<String>();
    	for(Tuple2<?,?> tuple: output)
    	{
    		if(!tuple._1().toString().contains("NULL"))
    		{
    		    data += tuple._1() + "\n";
    		    srdd.add(tuple._1().toString());
    		}
    	}
    	/*try
    	{
    		//System.out.println(data);
    	    File outFile = new File("/home/azureuser/workspace2/operation4/src/output.txt");
    	    FileOutputStream is = new FileOutputStream(outFile);
    	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(is));
    	    bw.write(data);
    	    bw.close();
    	}
    	catch(IOException e){}
    	*/
    	JavaRDD<String> op = sc.parallelize(srdd).repartition(1);
    	op.saveAsTextFile(args[3]);
    	
    	
        //System.out.println(output.get(0));
    	//enclosed.saveAsTextFile("output.txt");
    	
        sc.close();
        
    }
}

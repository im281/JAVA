package componentDetection;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IsotopeClusterReducer extends Reducer<Text, 
Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	 
    	 System.out.println("Inside Isotope Cluster Reduce !");
    	 String cValue = "";
         for(Text val : values)
         {     	 
        	//Write out Clusters here       	 
        	cValue = val.toString();
        	context.write(key, new Text(cValue));
         } 
    }
 }
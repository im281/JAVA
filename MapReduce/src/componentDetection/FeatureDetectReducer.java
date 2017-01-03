package componentDetection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.*;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class FeatureDetectReducer extends Reducer<Text, 
Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	 
    	 System.out.println("Inside Feature Detection Reduce !");
    	 String fValue = "";
    	 Detector c = new Detector();
    	 ArrayList<IsotopeCluster> clusters = new ArrayList<IsotopeCluster>();
         for(Text val : values)
         {
        	 //get all the isotope clusters for this key 
        	 //(single monoisotopic mass accross all files)
        	 IsotopeCluster currentCluster = c.GetClustersfromKey(val.toString());
        	 clusters.add(currentCluster);      	 
         }       
         //TODO: get features by ID (need another loop here for multiple IDs)   
         
         //detect feature     
         fValue = c.WriteFeatureValue(c.DetectFeature(clusters));
         
         //write out feature results
         context.write(key, new Text(fValue));
              
         
    }      	    
 }
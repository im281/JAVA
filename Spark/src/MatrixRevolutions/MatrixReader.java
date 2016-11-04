package MatrixRevolutions;

//$example on$
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//Matrix math
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;


public class MatrixReader {
	
	public void ConvertMatrix(String s){
		
		//boiler plate needed to run locally  
				SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
				JavaSparkContext sc = new JavaSparkContext(conf);
				  
			    SparkSession spark = SparkSession
			      .builder()
			      .appName("JavaLinearRegressionWithElasticNetExample")
			      .getOrCreate()
			      .newSession();
		
		Dataset<Row> training = spark.read().format("libsvm")
	    	      .load(s);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) {
		    
	    MatrixReader r = new MatrixReader();
	    r.ConvertMatrix(args[0]);
		
	}
	

}

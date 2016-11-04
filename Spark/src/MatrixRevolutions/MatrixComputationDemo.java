package MatrixRevolutions;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.linalg.Matrices;


import scala.Tuple2;

import java.awt.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;

public class MatrixComputationDemo {

	public static void main(String[] args) {

		// Create a dense vector (1.0, 0.0, 3.0)
		Vector dv = Vectors.dense(1.0, 0.0, 3.0);
		// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
		Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
		
		// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
		Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

		// Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
		Matrix sm = Matrices.sparse(3, 2, new int[] {0, 1, 3}, new int[] {0, 2, 1}, new double[] {9, 6, 8});
		System.out.println("test" + sv + dv);
				
	}
}

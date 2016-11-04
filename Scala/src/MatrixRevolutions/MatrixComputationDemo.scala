//package MatrixRevolutions

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector

object MatrixComputationDemo {
  
  def main(args: Array[String]): Unit = {
  
   val conf = new SparkConf()
             .setMaster("local")
             .setAppName("Demo")
             
    val sc = new SparkContext(conf)
                   
    val spark = SparkSession
      .builder
      .appName("GeneralizedLinearRegressionExample")
      .getOrCreate()
      .newSession()
      
      
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    
    System.out.println(dm.toString() + sm.toString())
    
                  
  }
}

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		int [ ][ ] scores = new int [5][3] ;    // Declares a 2-D array
		
		 // This is the problem we solved in class
	    double[][] A = {
	        // x = 1, y = 2, z = 3
	        { 1,  2, 3, 14 },  // 1x + 2y + 3z = 14
	        { 1, -1, 1,  2 },  // 1x - 1y + 1z = 2
	        { 4, -2, 1,  3 }   // 4x - 2y + 1z = 3	    
	    };
	    
	    double[] b = {1,1,4};
	    
	    double[] s = GaussianElimination.lsolve(A, b);
	    


	}

}

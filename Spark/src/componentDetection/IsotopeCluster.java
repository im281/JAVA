package componentDetection;

import java.util.Comparator;

public class IsotopeCluster implements Comparator<IsotopeCluster>, Comparable<IsotopeCluster> {
	public int FileID;
	public double MimZ;
	public double Intensity;
	public double RT;
	public int ScanNumber;
	
	public int compareTo(IsotopeCluster c) {
		// TODO Auto-generated method stub
		return 0;
	}
	public int compare(IsotopeCluster c1, IsotopeCluster c2) {	
		if (c1.RT < c2.RT) return -1;
        if (c1.RT > c2.RT) return 1;
        return 0;
	}	
}
	

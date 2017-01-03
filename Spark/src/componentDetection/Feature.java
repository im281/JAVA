package componentDetection;

import java.util.ArrayList;
import java.util.Comparator;

public class Feature implements Comparator<Feature>, Comparable<Feature> {
	public int FileID;
	public int FeatureID;
	public double MimZ;
	public double ApexIntensity;
	public double ApexRT;
	public int ApexScanNumber;
	
	ArrayList<IsotopeCluster> clusters = new ArrayList<IsotopeCluster>();

	@Override
	public int compareTo(Feature arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int compare(Feature f1, Feature f2) {
		if (f1.FileID < f2.FileID) return -1;
        if (f1.FeatureID > f2.FileID) return 1;
        return 0;
	}

}

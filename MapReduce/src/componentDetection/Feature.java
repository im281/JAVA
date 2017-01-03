package componentDetection;

import java.util.ArrayList;

public class Feature {
	public int FileID;
	public int FeatureID;
	public double MimZ;
	public double ApexIntensity;
	public double ApexRT;
	public int ApexScanNumber;
	
	ArrayList<IsotopeCluster> clusters = new ArrayList<IsotopeCluster>();

}

package componentDetection;

import java.util.*;

public class Detector {
	public final ArrayList<IsotopeCluster> GetClusters(String scanText) {
		ArrayList<IsotopeCluster> clusters = new ArrayList<IsotopeCluster>();

		String[] elements = scanText.split("[;,]");

		IsotopeCluster cluster = new IsotopeCluster();

		int fileID = Integer.parseInt(elements[0]);

		for (int i = 2; i < elements.length - 1; i++) {
			String[] miPair = elements[i].split("[:]", -1);

			// for simulation purposes. In the test file
			// this threshold is A0 peaks of compounds
			// So cluster simulation only has A0 of abundant compounds
			if (Double.parseDouble(miPair[1]) > 5e6) {
				cluster.FileID = fileID;
				cluster.MimZ = Double.parseDouble(miPair[0]);
				cluster.Intensity = Double.parseDouble(miPair[1]);
				cluster.ScanNumber = Integer.parseInt(miPair[2]);
				cluster.RT = Double.parseDouble(miPair[3]);
				clusters.add(cluster);
				cluster = new IsotopeCluster();
			}
		}

		return clusters;
	}
	
	public String WriteClusterValue(IsotopeCluster cluster) {

		return cluster.FileID + "," + cluster.MimZ + "," + cluster.Intensity
				+ "," + cluster.ScanNumber + "," + cluster.RT;
	}

	public String WriteClusterKey(IsotopeCluster cluster) {

		// TODO: binning algorithm is the key point here
		return Integer.toString((int) Math.floor(cluster.MimZ / .005));
	}
	
	public String WriteFeatureValue(Feature feature) {

		return feature.FileID + "," + feature.MimZ + "," + feature.ApexIntensity
				+ "," + feature.ApexScanNumber + "," + feature.ApexRT;
	}

	public String WriteFeatureKey(Feature feature) {

		return Integer.toString(feature.FeatureID);
	}

	public IsotopeCluster GetClustersfromKey(String clusterText) {

		IsotopeCluster c = new IsotopeCluster();
		String[] cData = clusterText.split(",");
		c.FileID = Integer.parseInt(cData[0]);
		c.MimZ = Double.parseDouble(cData[1]);
		c.Intensity = Double.parseDouble(cData[2]);
		c.ScanNumber = Integer.parseInt(cData[3]);
		c.RT = Double.parseDouble(cData[4]);

		return c;
	}

	public Feature DetectFeature(ArrayList<IsotopeCluster> cluster) {

		//test
		if (cluster.get(0).MimZ < 196 && cluster.get(0).MimZ > 194
				&& cluster.size() > 50) {
			int test = 0;
		}

		// TODO: this is hardcoded
		// M is 2x the expected peak width at base
		int M = 10;
		// sort by increasing RT
		Collections.sort(cluster, new IsotopeCluster());

		// three arrays to hold all the data
		double[] temp = new double[cluster.size()];
		double[] medianFiltered = new double[cluster.size()];
		double[] rawData = new double[cluster.size()];
		double[] bcdData = new double[cluster.size()];

		for (int i = 0; i < rawData.length; i++) {
			rawData[i] = cluster.get(i).Intensity;
		}

		for (int i = 0; i < rawData.length; i++) {
			for (int j = i; j < M; j++) {
				if (j < M && j < rawData.length) {
					temp[j] = cluster.get(j).Intensity;
				} else {
					medianFiltered[i] = median(temp);
					temp = new double[cluster.size()];
					break;
				}
			}
		}

		for (int i = 0; i < cluster.size(); i++) {
			bcdData[i] = rawData[i] - medianFiltered[i];
		}

		// get max intensity for RT index and create feature
		return CreateFeature(cluster.get(GetMax(bcdData)), cluster);

		// TODO: get left and right Rts for each feature
	}

	// the array double[] m MUST BE SORTED
	private static double median(double[] m) {
		Arrays.sort(m);
		m = RemoveZeros(m);
		int middle = m.length / 2;
		if (m.length % 2 == 1) {
			return m[middle];
		} else {
			return (m[middle - 1] + m[middle]) / 2.0;
		}
	}

	private static double[] RemoveZeros(double[] array) {
		int j = 0;
		for (int i = 0; i < array.length; i++) {
			if (array[i] != 0)
				array[j++] = array[i];
		}
		double[] newArray = new double[j];
		System.arraycopy(array, 0, newArray, 0, j);
		return newArray;
	}

	private static int GetMax(double[] array) {
		int maxIndex = 0;
		for (int i = 0; i < array.length; i++) {
			double newnumber = array[i];
			if ((newnumber > array[maxIndex])) {
				maxIndex = i;
			}
		}
		return maxIndex;
	}

	private Feature CreateFeature(IsotopeCluster c, ArrayList<IsotopeCluster> ca) {
		// create feature
		Feature f = new Feature();
		f.FileID = c.FileID;
		f.ApexScanNumber = c.ScanNumber;
		f.MimZ = c.MimZ;
		f.ApexRT = c.RT;
		f.ApexIntensity = c.Intensity;
		f.clusters = ca;
		return f;
	}
}

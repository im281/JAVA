package componentDetection;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonFormat.Features;

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
			if (Double.parseDouble(miPair[1]) > 5e5) {
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
		return Integer.toString((int) Math.floor(cluster.MimZ / .005)) + "_" + cluster.FileID;
	}
	
	public String WriteFeatureValue(Feature feature) {

		return feature.FileID + "," + feature.MimZ + "," + feature.ApexIntensity
				+ "," + feature.ApexScanNumber + "," + feature.ApexRT;
	}
	
	public ArrayList<String> WriteFeatureValues(ArrayList<Feature> features) {

		ArrayList<String> featureValues = new ArrayList<String>();
		
		for(int i = 0; i < features.size();i++){
			
			Feature feature = new Feature();
			feature.FileID = features.get(i).FileID;
			String v = features.get(i).FileID + "," + features.get(i).MimZ + "," + features.get(i).ApexIntensity
			+ "," + features.get(i).ApexScanNumber + "," + features.get(i).ApexRT;
			featureValues.add(v);
			
		}
		
		return featureValues;
	
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

	public Feature DetectFeature(ArrayList<IsotopeCluster> clusters) {

		// TODO: this is hardcoded
		// M is 2x the expected peak width at base
		int M = 10;
		// sort by increasing RT
		Collections.sort(clusters, new IsotopeCluster());

		// three arrays to hold all the data
		double[] temp = new double[clusters.size()];
		double[] medianFiltered = new double[clusters.size()];
		double[] rawData = new double[clusters.size()];
		double[] bcdData = new double[clusters.size()];

		for (int i = 0; i < rawData.length; i++) {
			rawData[i] = clusters.get(i).Intensity;
		}

		for (int i = 0; i < rawData.length; i++) {
			for (int j = i; j < M; j++) {
				if (j < M && j < rawData.length) {
					temp[j] = clusters.get(j).Intensity;
				} else {
					medianFiltered[i] = median(temp);
					temp = new double[clusters.size()];
					break;
				}
			}
		}

		for (int i = 0; i < clusters.size(); i++) {
			bcdData[i] = rawData[i] - medianFiltered[i];
		}

		// get max intensity for RT index and create feature
		return CreateFeature(clusters.get(GetMax(bcdData)), clusters);

		// TODO: get left and right Rts for each feature
	}
	
	public ArrayList<Feature> DetectFeatures(ArrayList<IsotopeCluster> clusters) {

		ArrayList<Feature> features = new ArrayList<Feature>();
		
		// TODO: this is hardcoded
		// M is 2x the expected peak width at base
		int M = 10;
		// sort by increasing RT
		Collections.sort(clusters, new IsotopeCluster());

		// three arrays to hold all the data
		double[] temp = new double[clusters.size()];
		double[] medianFiltered = new double[clusters.size()];
		double[] rawData = new double[clusters.size()];
		double[] bcdData = new double[clusters.size()];

		for (int i = 0; i < rawData.length; i++) {
			rawData[i] = clusters.get(i).Intensity;
		}

		for (int i = 0; i < rawData.length; i++) {
			for (int j = i; j < M; j++) {
				if (j < M && j < rawData.length) {
					temp[j] = clusters.get(j).Intensity;
				} else {
					medianFiltered[i] = median(temp);
					temp = new double[clusters.size()];
					break;
				}
			}
		}

		for (int i = 0; i < clusters.size(); i++) {
			bcdData[i] = rawData[i] - medianFiltered[i];
		}

		// get max intensity for RT index and create feature
		Feature f = CreateFeature(clusters.get(GetMax(bcdData)), clusters);
		features.add(f);	
		Feature f1 = CreateFeature(clusters.get(GetMax(bcdData)), clusters);
		features.add(f1);
		return features;	
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
	
	public String GetFeatureAlignmentOffset(Iterable<String> values){
		System.out.println("Inside feature Alignment mapper!");
		String fValue = "";
		double referenceRT = 0;
		double rtOffset = 0;

		// search for the reference file to set reference RT
		for (String val : values) {
			String line = val.toString();
			String[] elements = line.split("[,]");

			if (elements[0].contains("1")) {
				referenceRT = Double.parseDouble(elements[4]);
				rtOffset = 0;
				fValue = line + "," + rtOffset;
				break;
			}
		}
		if (referenceRT != 0) {
			for (String val : values) {
				String line = val.toString();
				String[] elements = line.split("[,]");

				if (elements.toString().contains("1")) {
					referenceRT = Double.parseDouble(elements[4]);
					rtOffset = 0;
				} else {
					rtOffset = referenceRT - Double.parseDouble(elements[4]);
				}

				fValue = line + "," + rtOffset;
			}
		}
		return fValue;
	}
}

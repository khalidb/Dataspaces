package uk.ac.manchester.dataspaces.annotation_propagation;

public class MappingAnnotation {
	
	double precision, recall, f_measure;

	public double getPrecision() {
		return precision;
	}

	public void setPrecision(double precision) {
		this.precision = precision;
	}

	public double getRecall() {
		return recall;
	}

	public void setRecall(double recall) {
		this.recall = recall;
	}

	public double getF_measure() {
		return f_measure;
	}

	public void setF_measure(double fMeasure) {
		f_measure = fMeasure;
	}
	

}

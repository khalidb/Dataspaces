package uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement;

public interface FeedbackUpdate {
	
	public void insertTP(String integration_query, String[] values, String[] attributes);
	public void insertFP(String integration_query, String[] values, String[] attributes);
	public void insertFN(String integration_query, String[] values, String[] attributes);
	
}

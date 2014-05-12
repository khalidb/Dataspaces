package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;

public interface QueryResultManagement {

	public abstract void storeResults(String integration_table, ResultsSet res);
	public void reinitialiseDS(String integration_query);
	public void initialiseLog(String integration_query);
	public void reinitialisePruningResults();
	public void processResults(String i_query);
	public void updateLog(String integration_query,Vector top_mappings);
	public void setFeedbackAmount(String snapshot_id);
	public void annotateResults(String snapshot_id, String integration_query,
			Vector mappings);
	public Map getActualResultsAnnotations(String integration_query,
			Vector mappings);
	public String getSnapshotID();
	public abstract void updateLog(ArrayList bR,
			HashMap<String, Vector> candidateMappings);

}
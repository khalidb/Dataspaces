/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement;

import java.util.Map;
import java.util.Vector;

/**
 * @author Khalid Belhajjame
 *
 */

public interface PruneMappings {
	
	public void identifyTP(String integration_query);
	
	public void identifyFP(String integration_query);
	
	public void identifyResultWithoutFeedback(String integration_query);
	
	public void order_tp(String integration_query);
	
	public void order_fp(String integration_query);
	
	public void order_awf(String integration_query);
	
	public void annotateMappings(String integration_query);
	
	public Map annotateResultsBasedOnUF(String integration_query, Vector mappings);
	
	public void annotateMappingsCorrectly(String integration_query);
	
	public Vector getMappings(String integration_query);
	
	public void generateCorrectFeedbackInstance(String integration_query);

	public void generateIncorrectTupleFeedbackInstance(String integration_query);
	
	public void generateIncorrectAttributeFeedbackInstance(String integration_query);
	
	public void generateAFalseNegative(String integration_query);

	public void generateCorrectFeedbackInstance(String integration_query, Vector mappings);

	public void generateIncorrectTupleFeedbackInstances(String integration_query, Vector mappings,int num);
	
	public void generateIncorrectAttributeFeedbackInstances(String integration_query, Vector mappings, int num);
	
	public void generateFalseNegatives(String integration_query, Vector mappings,int num);
	
	public void pruneMappings(String integration_query);
	
	public void pruneMapping(String integratioin_query, int map_id);
	
	public void pruneMappings(String integration_query, Vector candidate_mappings);

	public Vector selectTopKMappings(String integration_query, int top_k);
	
	public void generateFeedback(String integration_query);

	void order_tp(String integration_query, int map_id);
	
	void order_fp(String integration_query, int map_id);
	
	void order_awf(String integration_query, int map_id);

	void annotateMapping(String integration_query, int map_id);

	void generateFeedback(String integration_query, int num);
	
	void generateFeedback(String integration_query, int num, Vector mappings);

	Vector chooseRandomlyKMappings(String integration_query, int top_k);

	int getNumberOfCorrectResults(String integration_query);

	double getBeta();

	public Vector selectMappingsThatCoverTP(String integration_query);
	
	public void ruleOutMappings(String snapshot_id,String integration_query, Double f_measure, Double support);

	void generateFeedbackByType(String integration_query, int num,
			Vector mappings, String type);

	boolean did_pruning_results_change(String integration_query);

	void generateFeedbackFromSample(String integration_query, int num,
			Vector mappings);

	public Map getFeedbackRatios(String integration_query, Vector top_mappings, int annotated_tp, int annotated_fp, int annotated_fn);

	public int getMappingsResults(String integration_query, Vector mappings);
	
	public int getMappingsTP(String integration_query, Vector mappings);
	
	public int getMappingsFP(String integration_query, Vector mappings);
	
	public void PruneIntegrationRelation(String integration_relation, int num_experiments, int num_iterations_per_experiment);	


}

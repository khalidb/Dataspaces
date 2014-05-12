/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.util.Vector;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.IntegrationQuery;

/**
 * @author Khalid Belhajjame
 *
 */
public interface SchemaMappingRetrieval {
	
	public Vector getCandidateMappings(String i_query);
	public Vector getCandidateMappings();
	public Vector getPruningResultsIDs(String integration_query);
	public Vector getPruningResults(String id);
	public void exportPruningResults(String i_query);
	public void loadDump(String file);
	public void createDump(String file);

}

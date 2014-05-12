/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.mappinggeneration;

import java.util.Vector;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.Table;

/**
 * @author Khalid Belhajjame
 *
 */
public interface GenerateMappings {
	
	public Vector generateMappingsRandomly();
	public void generateMappings(String integration_query, Vector mappings, int num_rows);
	public Vector getMappings();
	public void generateMappings();

}

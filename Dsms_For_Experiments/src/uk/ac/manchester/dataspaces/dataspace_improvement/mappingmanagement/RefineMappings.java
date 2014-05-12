package uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement;

import java.util.Vector;

public interface RefineMappings {
	
	public void refineMappings(String integration_query);
	public Vector getNeighbours(int map_id,String criterion, String integration_query);

}

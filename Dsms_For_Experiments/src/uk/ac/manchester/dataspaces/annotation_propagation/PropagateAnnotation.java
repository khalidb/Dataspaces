package uk.ac.manchester.dataspaces.annotation_propagation;

import java.util.Map;

public  interface PropagateAnnotation {

	public Map propagate(Query query); 
	public Map propagateBasedOnCompleteKnowledge(Query query); 
	
}

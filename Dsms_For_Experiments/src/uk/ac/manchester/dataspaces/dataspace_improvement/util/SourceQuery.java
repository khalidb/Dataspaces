package uk.ac.manchester.dataspaces.dataspace_improvement.util;

import java.util.Vector;

/**
 * 
 * @author Khalid Belhajjame
 *
 */

public class SourceQuery {
	
	public String query_definition;
	public Vector sources;
	
	public String getQuery_definition() {
		return query_definition;
	}
	
	public void setQuery_definition(String query_definition) {
		this.query_definition = query_definition;
	}
	
	public Vector getSources() {
		return sources;
	}
	
	public void setSources(Vector sources) {
		this.sources = sources;
	}
	

}

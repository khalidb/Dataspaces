/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.util;

/**
 * @author Khalid Belhajjame
 *
 */
public class SchemaMapping {
	
	public int id;
	
	public String IntegrationRelation;
	
	public String s_query;

	public String getIntegrationRelation() {
		return IntegrationRelation;
	}

	public void setIntegrationRelation(String integrationRelation) {
		IntegrationRelation = integrationRelation;
	}

	public String getS_query() {
		return s_query;
	}

	public void setS_query(String s_query) {
		this.s_query = s_query;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

}

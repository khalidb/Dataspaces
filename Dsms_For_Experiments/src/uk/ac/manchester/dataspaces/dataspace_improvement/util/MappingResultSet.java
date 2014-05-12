/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.util;

import java.util.Vector;

/**
 * @author Khalid Belhajjame
 *
 */
public class MappingResultSet {
	
	SchemaMapping map;
	
	Vector head = new Vector();
	
	Vector body = new Vector();

	public Vector getHead() {
		return head;
	}

	public void setHead(Vector head) {
		this.head = head;
	}

	public Vector getBody() {
		return body;
	}

	public void setBody(Vector body) {
		this.body = body;
	}
	
	public void addResultElement(Vector res_element) {
		this.body.add(res_element);
	}

	public SchemaMapping getMap() {
		return map;
	}

	public void setMap(SchemaMapping map) {
		this.map = map;
	}

}

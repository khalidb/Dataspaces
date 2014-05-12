/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.util;

import java.util.Vector;

/**
 * @author Khalid Belhajjame
 *
 */
public class ResultsSet {
	
	Vector head = new Vector();
	
	Vector results = new Vector();

	public Vector getHead() {
		return head;
	}

	public void setHead(Vector head) {
		this.head = head;
	}

	public Vector getResults() {
		return results;
	}

	public void setResults(Vector results) {
		this.results = results;
	}
	
	public void addResults(MappingResultSet res) {
		this.results.add(res);
	}


}

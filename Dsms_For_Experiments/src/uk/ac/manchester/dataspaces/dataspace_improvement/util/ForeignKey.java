/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.util;

import java.util.Vector;

/**
 * @author Khalid Belhajjame
 *
 */
public class ForeignKey {
	
	Vector attributes = new Vector();
	String table_name;
	
	public void add(String att) {
		this.attributes.add(att);
	}

	public Vector getAttributes() {
		return attributes;
	}

	public void setAttributes(Vector attributes) {
		this.attributes = attributes;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

}

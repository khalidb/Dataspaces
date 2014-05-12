/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.util;

import java.util.Vector;

/**
 * @author Khalid Belhajjame
 *
 */
public class Table {
	
	public String schema;
	
	public String name;
	
	public Vector columns = new Vector();
	
	public Vector primary_key;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Vector getColumns() {
		return columns;
	}
	
	public void setColumns(Vector columns) {
		this.columns = columns;
	}
	
	public void addColumn(Column col){
		this.columns.add(col);
	}
	
	public Vector getPrimary_key() {
		return primary_key;
	}
	
	public void setPrimary_key(Vector primary_key) {
		this.primary_key = primary_key;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

}

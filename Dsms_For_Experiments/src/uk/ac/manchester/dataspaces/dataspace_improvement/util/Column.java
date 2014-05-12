/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.util;

/**
 * @author Khalid Belhajjame
 *
 */
public class Column {
	
	public String name;
	
	public String data_type;
	
	public boolean is_nullable;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getData_type() {
		return data_type;
	}

	public void setData_type(String data_type) {
		this.data_type = data_type;
	}

	public boolean isNulable() {
		return is_nullable;
	}

	public void setNulable(boolean nulable) {
		this.is_nullable = nulable;
	}

}

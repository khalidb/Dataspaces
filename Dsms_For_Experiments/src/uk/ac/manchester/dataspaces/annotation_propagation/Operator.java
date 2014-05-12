/**
 * 
 */
package uk.ac.manchester.dataspaces.annotation_propagation;

/**
 * @author khalidbelhajjame
 *
 */

public class Operator {

	String type;
	int selectivity;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public int getSelectivity() {
		return selectivity;
	}
	public void setSelectivity(int selectivity) {
		this.selectivity = selectivity;
	}
	
}

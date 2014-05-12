package uk.ac.manchester.dataspaces.annotation_propagation;

public class Node {
	
	String type;
	Operator op;
	String br;
	
	public void setNode(Operator _op) {
		this.type = "operator";
		this.op = _op; 
	}

	public void setNode(String _br) {
		this.type = "base_relation";
		this.br = _br; 
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Operator getOp() {
		return op;
	}

	public void setOp(Operator op) {
		this.op = op;
	}

	public String getBr() {
		return br;
	}

	public void setBr(String br) {
		this.br = br;
	}


}

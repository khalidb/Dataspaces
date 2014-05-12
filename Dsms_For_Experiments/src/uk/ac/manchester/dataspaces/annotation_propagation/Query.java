/**
 * 
 */
package uk.ac.manchester.dataspaces.annotation_propagation;

import java.util.ArrayList;
import java.util.Vector;

import sun.tools.tree.ThisExpression;

/**
 * @author khalidbelhajjame
 *
 */

public class Query {

	Node node = null;
	Query left_substree = null;
	Query right_subtree = null;
	String pred_right_operand = null; // This is used when the node is a selection or a join operator
	String pred_left_operand = null; // This is used when the node is a selection or a join operato
	ArrayList projected_atributes = null; // This is used when the node is a project attribute
	String alias_left=null; //alias used for referring to the left subtree
	String alias_right=null;// alias used for referring to the right subtree
	
	// When the node is a unary operator, then only the left subtree is instanciated, the right one is null.
	
	public boolean isLeaf() {
		if ((this.left_substree == null) && (this.right_subtree == null))
			return true;
		else
			return false;
	}
	
	
	public Node getNode() {
		return node;
	}
	public void setNode(Node _node) {
		node = _node;
	}
	public Query getLeft_subtree() {
		return left_substree;
	}
	public void setLeft_subtree(Query leftSubStree) {
		left_substree = leftSubStree;
	}
	public Query getRight_subtree() {
		return right_subtree;
	}
	public void setRight_subtree(Query rightSubtree) {
		right_subtree = rightSubtree;
	}


	public String getPred_right_operand() {
		return pred_right_operand;
	}


	public void setPred_right_operand(String predRightOperand) {
		pred_right_operand = predRightOperand;
	}


	public String getPred_left_operand() {
		return pred_left_operand;
	}


	public void setPred_left_operand(String predLeftOperand) {
		pred_left_operand = predLeftOperand;
	}

	
	public static String getSQL(Query query, String level, int label) {
		
		String sql=null;
		
		if (query.isLeaf()) {
			 System.out.println("Start processing leaf node");
			 sql = "(select * from "+query.getNode().getBr()+" as "+query.getAlias_left()+")";
		}
		else {
			
			if (query.getNode().getOp().getType().equals("select")) {
				System.out.println("start processing select node");
				String predicate = "("+query.getPred_left_operand()+" = '"+query.getPred_right_operand()+"')";
				sql = "(select * from "+getSQL(query.getLeft_subtree(),level+"_l",label)+" as "+query.getAlias_left()+" where "+predicate +")";
			}
			
			if (query.getNode().getOp().getType().equals("join")) {
				System.out.println("start processing join node");
				String tab_1 = "tab_"+level+label++;
				String tab_2 = "tab_"+level+label++;
				String predicate = "(tab_1."+query.getPred_left_operand()+" = tab_2."+query.getPred_right_operand()+")";
				sql = "(select * from "+getSQL(query.getLeft_subtree(),level+"_l",label)+" as "+tab_1+", "+getSQL(query.getRight_subtree(),level+"_r",label)+" as "+tab_2+" where "+predicate +")";
			}
			
			if (query.getNode().getOp().getType().equals("union")) {
				sql = "("+getSQL(query.getLeft_subtree(),level+"_l",label)+" union "+getSQL(query.getRight_subtree(),level+"_r",label)+")";
			}
			
			if (query.getNode().getOp().getType().equals("intersect")) {
				sql = "("+getSQL(query.getLeft_subtree(),level+"_l",label)+" intersect "+getSQL(query.getRight_subtree(),level+"_r",label)+")";
			}
			
			if (query.getNode().getOp().getType().equals("project")) {
				String projected_attributes = "";
				for (int i = 0; i< query.getProjected_atributes().size();i++) {
					if (i != 0)
						projected_attributes = ", ";
					projected_attributes = projected_attributes + query.getProjected_atributes().get(i).toString();
				}
				
				sql = "(select "+projected_attributes+" from "+getSQL(query.getLeft_subtree(),level+"_l",label)+")";
			}
			
		}		
		
		return sql;		
	}


	public ArrayList getProjected_atributes() {
		return projected_atributes;
	}


	public void setProjected_atributes(ArrayList _projectedAtributes) {
		projected_atributes = _projectedAtributes;
	}
	
	public ArrayList getBaseRelations() {
		ArrayList BR = null;
		
			if (this.node.getBr() != null) {
				BR = new ArrayList();
				BR.add(this.node.getBr());
			}
			else 
				if (this.node.getOp() != null) {
					ArrayList BR1 = null, BR2 = null;
					if (this.getLeft_subtree() != null)
						BR1 = this.getLeft_subtree().getBaseRelations();
					if (this.getRight_subtree() != null)
						BR2 = this.getRight_subtree().getBaseRelations();
					BR = this.addArray(BR1, BR2);
				}

		return BR;
	}
	
	public ArrayList addArray(ArrayList BR1, ArrayList BR2) {
		
		ArrayList BR = new ArrayList();
		
		if (BR1 != null) {
			for (int i = 0;i<BR1.size();i++)
				BR.add(BR1.get(i));
		}

		if (BR2 != null) {
			for (int i = 0;i<BR2.size();i++)
				BR.add(BR2.get(i));
		}
		
		return BR;
	}
	
	public static void main(String[] args) {
		
		Query query1 = new Query();
		Node node1 = new Node();
		Operator op1 = new Operator();
		op1.setType("join");
		node1.setOp(op1);
		query1.setPred_left_operand("Name1");
		query1.setPred_right_operand("Name2");
		query1.setNode(node1);
		
		Query query1_2 = new Query();
		Node node2 = new Node();
		Operator op2 = new Operator();
		op2.setType("union");
		node2.setOp(op2);
		query1_2.setNode(node2);
		query1.setLeft_subtree(query1_2);

		Query query1_2_1 = new Query();
		Node node3 = new Node();
		node3.setBr("europeancity");
		query1_2_1.setNode(node3);
		query1_2.setLeft_subtree(query1_2_1);

		Query query1_2_2 = new Query();
		Node node4 = new Node();
		node4.setBr("europeancity");
		query1_2_2.setNode(node4);
		query1_2.setRight_subtree(query1_2_2);
		
		Query query1_1 = new Query();
		Node node5 = new Node();
		Operator op3 = new Operator();
		op3.setType("join");
		node5.setOp(op3);
		query1_1.setNode(node5);
		query1_1.setPred_left_operand("Name3");
		query1_1.setPred_right_operand("Name4");
		query1.setRight_subtree(query1_1);

		Query query1_1_1 = new Query();
		Node node6 = new Node();
		node6.setBr("europeancity");
		query1_1_1.setNode(node3);
		query1_1.setLeft_subtree(query1_1_1);
		
		Query query1_1_2 = new Query();
		Node node7 = new Node();
		node7.setBr("europeancity");
		query1_1_2.setNode(node3);
		query1_1.setRight_subtree(query1_1_2);
		
		String sql_query = Query.getSQL(query1, "", 0);
		System.out.println(sql_query);
		
	}


	public String getAlias_left() {
		return alias_left;
	}


	public void setAlias_left(String aliasLeft) {
		alias_left = aliasLeft;
	}


	public String getAlias_right() {
		return alias_right;
	}


	public void setAlias_right(String aliasRight) {
		alias_right = aliasRight;
	}
	
}

package uk.ac.manchester.dataspaces.annotation_propagation;

import java.util.HashMap;
import java.util.Map;

import sun.tools.tree.ThisExpression;

public class PropagateAnnotationBasedOnFeedback implements PropagateAnnotation {

	@Override
	public Map propagate(Query query) {
		
		return this.propagate(query,"", 0);
	}
	
	@Override
	public Map propagateBasedOnCompleteKnowledge(Query query){
		
		return this.propagateBasedOnCompleteKnowledge(query,"", 0);
	} 
	
	public Map propagate(Query query, String level, int label) {
		
		HashMap<String, String> fd = new HashMap();
		String tp,fp,fn;
		
		
		
		if (query.isLeaf()){
			tp = "(select * from tp_"+query.getNode().getBr()+" as "+query.getAlias_left()+")"; 
			fp = "(select * from fp_"+query.getNode().getBr()+" as "+query.getAlias_left()+")"; 
			fn = "(select * from fn_"+query.getNode().getBr()+" as "+query.getAlias_left()+")"; 
			fd.put("tp",tp);
			fd.put("fp",fp);
			fd.put("fn",fn);
			
			System.out.println("leaf tp: "+ tp);
		}
		else
		{ 
			if (query.node.getOp().getType().equals("join")) {
				String tab_1 = "tab_"+level+label++;
				String tab_2 = "tab_"+level+label++;
				String predicate1 = "("+tab_1+"."+query.getPred_left_operand()+" = "+tab_2+"."+query.getPred_right_operand()+")";
				String tab_3 = "tab_"+level+label++;
				String tab_4 = "tab_"+level+label++;
				String predicate2 = "("+tab_3+"."+query.getPred_left_operand()+" = "+tab_4+"."+query.getPred_right_operand()+")";
				String tab_5 = "tab_"+level+label++;
				String tab_6 = "tab_"+level+label++;
				String predicate3 = "("+tab_5+"."+query.getPred_left_operand()+" = "+tab_6+"."+query.getPred_right_operand()+")";

				
				tp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab_1+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" as "+tab_2+" where "+predicate1+ ")";

				fp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" as "+tab_1+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fp")+" as "+tab_2+" where "+predicate1+ ")" + " union "
					 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab_3+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fp")+" as "+tab_4+" where "+predicate2+ ")" + " union "
					 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" as "+tab_5+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" as "+tab_6+" where "+predicate3+ ")";

				fn = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" as "+tab_1+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fn")+" as "+tab_2+" where "+predicate1+ ")" + " union "
				 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab_3+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fn")+" as "+tab_4+" where "+predicate2+ ")" + " union "
				 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" as "+tab_5+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" as "+tab_6+" where "+predicate3+ ")" + " union ";
				
				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			
			if (query.node.getOp().getType().equals("select")) {
				String tab = "tab_"+level+label++;
				String predicate = "("+query.getPred_left_operand()+" = "+query.getPred_right_operand()+")";
				tp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab+" where "+predicate+ ")";

				fp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" as "+tab+" where "+predicate+ ")";

				fn = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" as "+tab+" where "+predicate+ ")";

				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			
			if (query.node.getOp().getType().equals("intersect")) {
				
				tp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")";

				fp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")";
				
				fn = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")" + " union "
				  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")" + " union "
				  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";

				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
				
				System.out.println("intersect tp: "+ tp);
			}
			
			
			if (query.node.getOp().getType().equals("union")) {
				tp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" union "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")" + " union "
					 + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					 + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";

				fp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")";
				fn = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";
				
				
				 // The out-commented fp and fn are to be used when true negatives are considered

				//fp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
				//   + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tn")+")" + " union "
				//   + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")";
				
				//fn = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")" + " union "
				// + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tn")+")" + " union "
				// + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";
				

				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			
			/*
			if (query.node.getOp().getType().equals("union")) {
				tp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" union "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")" + " union "
					 + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					 + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";

				
				fp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union " 
					+"("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" except ("+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getRight_subtree(),level+"_r",label).get("fn")+"))"+" union "
					+"("+propagate(query.getRight_subtree(),level+"_l",label).get("fp")+" except ("+ propagate(query.getLeft_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getLeft_subtree(),level+"_r",label).get("fn")+"))";

				
				fn = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")" + " union " 
				+"("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" except ("+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getRight_subtree(),level+"_r",label).get("fp")+"))"+" union "
				+"("+propagate(query.getRight_subtree(),level+"_l",label).get("fn")+" except ("+ propagate(query.getLeft_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getLeft_subtree(),level+"_r",label).get("fp")+"))";


				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			*/
			
			if (query.node.getOp().getType().equals("project")) {

				// In this case, we need to refer to the feedback stored in the database
				// STILL TO BE DONE.
			}
			
		}
		
		return fd;
		
	}
	
	public Map propagateBasedOnCompleteKnowledge(Query query, String level, int label) {
		
		HashMap<String, String> fd = new HashMap();
		String tp,fp,fn;
		
		
		
		if (query.isLeaf()){
			tp = "(select * from tp_"+query.getNode().getBr()+" as "+query.getAlias_left()+")"; 
			fp = "(select * from fp_"+query.getNode().getBr()+" as "+query.getAlias_left()+")"; 
			fn = "(select * from fn_"+query.getNode().getBr()+" as "+query.getAlias_left()+")"; 
			fd.put("tp",tp);
			fd.put("fp",fp);
			fd.put("fn",fn);
			
			System.out.println("leaf tp: "+ tp);
		}
		else
		{ 
			if (query.node.getOp().getType().equals("join")) {
				String tab_1 = "tab_"+level+label++;
				String tab_2 = "tab_"+level+label++;
				String predicate1 = "("+tab_1+"."+query.getPred_left_operand()+" = "+tab_2+"."+query.getPred_right_operand()+")";
				String tab_3 = "tab_"+level+label++;
				String tab_4 = "tab_"+level+label++;
				String predicate2 = "("+tab_3+"."+query.getPred_left_operand()+" = "+tab_4+"."+query.getPred_right_operand()+")";
				String tab_5 = "tab_"+level+label++;
				String tab_6 = "tab_"+level+label++;
				String predicate3 = "("+tab_5+"."+query.getPred_left_operand()+" = "+tab_6+"."+query.getPred_right_operand()+")";

				
				tp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab_1+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" as "+tab_2+" where "+predicate1+ ")";

				fp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" as "+tab_1+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fp")+" as "+tab_2+" where "+predicate1+ ")" + " union "
					 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab_3+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fp")+" as "+tab_4+" where "+predicate2+ ")" + " union "
					 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" as "+tab_5+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" as "+tab_6+" where "+predicate3+ ")";

				fn = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" as "+tab_1+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fn")+" as "+tab_2+" where "+predicate1+ ")" + " union "
				 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab_3+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("fn")+" as "+tab_4+" where "+predicate2+ ")" + " union "
				 + "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" as "+tab_5+" ,"+propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" as "+tab_6+" where "+predicate3+ ")" + " union ";
				
				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			
			if (query.node.getOp().getType().equals("select")) {
				String tab = "tab_"+level+label++;
				String predicate = "("+query.getPred_left_operand()+" = "+query.getPred_right_operand()+")";
				tp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" as "+tab+" where "+predicate+ ")";

				fp = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" as "+tab+" where "+predicate+ ")";

				fn = "(select * from "+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" as "+tab+" where "+predicate+ ")";

				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			
			if (query.node.getOp().getType().equals("intersect")) {
				
				tp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")";

				fp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")";
				
				fn = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")" + " union "
				  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")" + " union "
				  + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";

				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
				
				System.out.println("intersect tp: "+ tp);
			}
			
			if (query.node.getOp().getType().equals("union")) {
				tp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("tp")+" union "+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+")" + " union "
					 + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union "
					 + "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")";

				
				fp = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fp")+")" + " union " 
					+"("+propagate(query.getLeft_subtree(),level+"_l",label).get("fp")+" except ("+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getRight_subtree(),level+"_r",label).get("fn")+"))"+" union "
					+"("+propagate(query.getRight_subtree(),level+"_l",label).get("fp")+" except ("+ propagate(query.getLeft_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getLeft_subtree(),level+"_r",label).get("fn")+"))";

				
				fn = "("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" intersect "+ propagate(query.getRight_subtree(),level+"_r",label).get("fn")+")" + " union " 
				+"("+propagate(query.getLeft_subtree(),level+"_l",label).get("fn")+" except ("+ propagate(query.getRight_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getRight_subtree(),level+"_r",label).get("fp")+"))"+" union "
				+"("+propagate(query.getRight_subtree(),level+"_l",label).get("fn")+" except ("+ propagate(query.getLeft_subtree(),level+"_r",label).get("tp")+" union "+propagate(query.getLeft_subtree(),level+"_r",label).get("fp")+"))";


				fd.put("tp",tp);
				fd.put("fp",fp);
				fd.put("fn",fn);
			}
			
			if (query.node.getOp().getType().equals("project")) {

				// In this case, we need to refer to the feedback stored in the database
				// STILL TO BE DONE.
			}
			
		}
		
		return fd;
		
	}

	
	public static void main(String[] args) {

		PropagateAnnotation pa = new PropagateAnnotationBasedOnFeedback();
		
		/*
		Query query1 = new Query();
		Node node1 = new Node();
		Operator op1 = new Operator();
		op1.setType("join");
		node1.setOp(op1);
		query1.setPred_left_operand("Name");
		query1.setPred_right_operand("Name");
		query1.setNode(node1);
		
		Query query2 = new Query();
		Node node2 = new Node();
		node2.setBr("europeancity");
		query2.setNode(node2);
		query1.setLeft_subtree(query2);
		
		Query query3 = new Query();
		Node node3 = new Node();
		node3.setBr("europeancity");
		query3.setNode(node2);
		query1.setRight_subtree(query3);		
		*/

		/*
		// Example of a select query
		Query query = new Query();
		
		Node node = new Node();
		Operator op = new Operator();
		op.setType("select");
		node.setOp(op);
		query.setPred_left_operand("country");
		query.setPred_right_operand("'USA'");
		query.setNode(node);
		query.setAlias_left("tab_s");
		
		Query query1 = new Query();
		Node node1 = new Node();
		node1.setBr("integration.favorite_city");
		query1.setNode(node1);
		query1.setAlias_left("tab_l_1");
		query.setLeft_subtree(query1);
		*/
		
		/*
		query.setAlias_left("tab_s");
		
		Node node = new Node();
		Operator op = new Operator();
		op.setType("intersect");
		node.setOp(op);
		//query.setPred_left_operand("country");
		//query.setPred_right_operand("'USA'");
		query.setNode(node);
		
		Query query1 = new Query();
		query1.setAlias_left("tab_l");
		Node node1 = new Node();
		node1.setBr("integration_favorite_city");
		query1.setNode(node1);
		query.setLeft_subtree(query1);
		
		Query query2 = new Query();
		query2.setAlias_left("tab_r");
		Node node2 = new Node();
		node2.setBr("integration_visited_city");
		query2.setNode(node2);
		query.setRight_subtree(query2);
		*/
		
		// Example of a union query
		Query query = new Query();
		query.setAlias_left("tab_s");
		
		Node node = new Node();
		Operator op = new Operator();
		op.setType("union");
		node.setOp(op);
		query.setNode(node);
		
		Query query1 = new Query();
		query1.setAlias_left("tab_l");
		Node node1 = new Node();
		node1.setBr("integration.favorite_city");
		query1.setNode(node1);
		query.setLeft_subtree(query1);
		
		Query query2 = new Query();
		query2.setAlias_left("tab_r");
		Node node2 = new Node();
		node2.setBr("integration.visited_city");
		query2.setNode(node2);
		query.setRight_subtree(query2);

		
		Map fd = pa.propagate(query);
		System.out.println("tp:\n"+fd.get("tp"));
		System.out.println("fp:\n"+fd.get("fp"));
		System.out.println("fn:\n"+fd.get("fn"));
		
	}
	

}

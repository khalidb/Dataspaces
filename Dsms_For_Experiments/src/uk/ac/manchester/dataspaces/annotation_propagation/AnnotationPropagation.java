/**
 * 
 */
package uk.ac.manchester.dataspaces.annotation_propagation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.ConnectionManager;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagement;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagementPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappings;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappingsPostgres;

/**
 * @author khalidbelhajjame
 *
 */
public class AnnotationPropagation {
	
	int expriment_loop = 10, pruning_loop = 110, beta = 1;
	public Jdbc3PoolingDataSource source = ConnectionManager.getSource();
	PruneMappings pm = new PruneMappingsPostgres(source);
	QueryResultManagement qrm = new QueryResultManagementPostgres(this.source);
	PropagateAnnotation pa = new PropagateAnnotationBasedOnFeedback();

	/*
	 * This method specify the query for which the annotation are to be propagated
	 */
	public Query getQuery() {
		
		
		Query query = new Query();
		
		
		// Example of a select query
		Node node = new Node();
		Operator op = new Operator();
		op.setType("select");
		node.setOp(op);
		query.setPred_left_operand("country");
		query.setPred_right_operand("'I'");
		query.setNode(node);
		query.setAlias_left("tab_s");
		
		Query query1 = new Query();
		Node node1 = new Node();
		node1.setBr("integration.favorite_city");
		query1.setNode(node1);
		query1.setAlias_left("tab_l_1");
		query.setLeft_subtree(query1);
		
		
		// example of an intersect query
		/*
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
		*/
		
		return query;
		
	}

	public String getTPQueryBasedOnCompleteKnowledge(int mapping_id, String br) {
		
		String[] attributes = this.getAttributes(br);
		String base_table = br.replace("integration.", "integration_");
		
		String attribute_list ="";
		
		for (int i =0;i < attributes.length;i++) {
			if (i != 0)
				attribute_list = attribute_list + ", ";
			attribute_list = attribute_list + attributes[i];
		}
		
		String query = "(select distinct "+ attribute_list + " from mapping_"+base_table+" m, correctresults_"+base_table+" c where "
						+"(m.result_id = c.id) and (m.mapping_id = "+mapping_id+"))";
		
		return query;
		
	}
	
	
	public String getFPQueryBasedOnCompleteKnowledge(int mapping_id, String br) {
		
		String[] attributes = this.getAttributes(br);
		String base_table = br.replace("integration.", "integration_");
		
		String attribute_list ="";
		
		for (int i =0;i < attributes.length;i++) {
			if (i != 0)
				attribute_list = attribute_list + ", ";
			attribute_list = attribute_list + attributes[i];
		}
		
		String query = "(select distinct "+ attribute_list + " from "+base_table+" i, mapping_"+base_table+" m where "
						+"(i.id = m.result_id) and (m.mapping_id = "+mapping_id+") and (i.id not in (select c.id from correctresults_"+base_table+" c)))";
		
		return query;
		
	}	

	public String getFNQueryBasedOnCompleteKnowledge(int mapping_id, String br) {
		
		String[] attributes = this.getAttributes(br);
		String base_table = br.replace("integration.", "integration_");
		
		String attribute_list ="";
		
		for (int i =0;i < attributes.length;i++) {
			if (i != 0)
				attribute_list = attribute_list + ", ";
			attribute_list = attribute_list + attributes[i];
		}
		
		String query = "(select distinct "+ attribute_list + " from correctresults_"+base_table+" c where (c.id not in (select m.result_id from mapping_"+base_table+" m where "
						+" (m.mapping_id = "+mapping_id+"))))";
		
		return query;
		
	}	
	
	
	public String getTPQuery(int mapping_id, String br) {
		
		String[] attributes = this.getAttributes(br);
		String base_table = br.replace("integration.", "integration_");
		
		String attribute_list ="";
		
		for (int i =0;i < attributes.length;i++) {
			if (i != 0)
				attribute_list = attribute_list + ", ";
			attribute_list = attribute_list + attributes[i];
		}
		
		String query = "(select distinct "+ attribute_list + " from "+base_table+" i, mapping_results m, expected_results c where "
						+"(m.base_table = '"+base_table+"') and (c.base_table = '"+base_table+"') and (i.id = m.result_id) and (m.result_id = c.id) and (m.mapping_id = "+mapping_id+"))";
		
		return query;
		
	}
	
	public String getFPQuery(int mapping_id, String br) {
		
		String[] attributes = this.getAttributes(br);
		String base_table = br.replace("integration.", "integration_");
		
		String attribute_list ="";
		
		for (int i =0;i < attributes.length;i++) {
			if (i != 0)
				attribute_list = attribute_list + ", ";
			attribute_list = attribute_list + attributes[i];
		}
		
		String query = "(select distinct "+ attribute_list + " from "+base_table+" i, mapping_results m, unexpected_results c where "
						+"(m.base_table = '"+base_table+"') and (c.base_table = '"+base_table+"') and (i.id = m.result_id) and (m.result_id = c.id) and (m.mapping_id = "+mapping_id+"))";
		
		return query;
		
	}
	
	public String getFNQuery(int mapping_id, String br) {
		
		String[] attributes = this.getAttributes(br);
		String base_table = br.replace("integration.", "integration_");
		PropagateAnnotation pa = new PropagateAnnotationBasedOnFeedback();
		
		String attribute_list ="";
		
		for (int i =0;i < attributes.length;i++) {
			if (i != 0)
				attribute_list = attribute_list + ", ";
			attribute_list = attribute_list + attributes[i];
		}
		
		String query = "(select distinct "+ attribute_list + " from "+base_table+" i, expected_results c where "
						+"(c.base_table = '"+base_table+"') and (i.id = c.id) and (i.id not in (select m.result_id from mapping_"+base_table+" m where (m.mapping_id = "+mapping_id+"))))";
		
		return query;
		
	}
	
	public String[] getAttributes(String table) {
		
		String[] attributes = null;
		java.sql.PreparedStatement st = null;
		String query = "select * from "+table;
		Connection conn = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			attributes = new String[st.getMetaData().getColumnCount()];
			
			for (int i=0;i<attributes.length;i++) {
				attributes[i] = st.getMetaData().getColumnName(i+1);
			}
			st.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}
		
		return attributes;
	}
	
	public void PruneQuery(Query query) {
		
	  ArrayList BR = query.getBaseRelations();
	  //for (int i=0;i<BR.size();i++)
	  //	  System.out.println("BR "+i+" is "+BR.get(i).toString());
	  
	  HashMap<String,Vector> candidate_mappings = new HashMap();
	  Map feedback_ratios;
	  int[] annotated_tp = new int[BR.size()];
	  int[] annotated_fp = new int[BR.size()];
	  int[] annotated_fn = new int[BR.size()];
	  
	  
	  // Set annotated tp, fp and fn to 0
	  for (int i=0; i<annotated_tp.length; i++)
		  annotated_tp[i] = 0;
	  for (int i=0; i<annotated_fp.length; i++)
		  annotated_fp[i] = 0;
	  for (int i=0; i<annotated_fn.length; i++)
		  annotated_fn[i] = 0;
	  
	  System.out.print("Retrieve Candidate Mappings ... ");
		for (int j =0; j< BR.size(); j++) {
			String br = BR.get(j).toString();
			System.out.println("Candidate mappings for "+br);
			candidate_mappings.put(br,this.pm.getMappings(br));
		//	for (int l=0;l<((Vector) candidate_mappings.get(br)).size();l++)
		//		System.out.println("mapping: "+((Vector) candidate_mappings.get(br)).get(l).toString());
		}
	  System.out.println("done");
	  
	  //Reinitialisation
	  for (int i=0;i<BR.size();i++) {
		  qrm.reinitialiseDS(BR.get(i).toString());
		  qrm.reinitialisePruningResults();
		  qrm.initialiseLog(BR.get(i).toString());
		  this.reinitialiseMappingPermutations();
		  this.reinitialiseQueryCorrectAnnotations();
		  this.reinitialiseExperimentResults();
	  }
	  
	  // Contruct possible mapping permutations
	  System.out.print("Construct and store mapping permutations ... ");
	  ArrayList map_permutations = this.construct_map_permutations(BR, new HashMap(candidate_mappings));
	  this.storeMappingPermutations(map_permutations);
	  //this.displayMappingPermutations(map_permutations);
	  System.out.println("done");
	  
	  	  
	  // Propagate annotations
	  System.out.print("Construct the queries for propagating annotations ... ");
	  Map<String,String> propagated_query = this.pa.propagate(query);
	  this.displayPropagatedQueries(propagated_query);
	  System.out.println("done");
	  
	  //Compute correct annotations for each mapping permutation
	  for (int k =0; k < map_permutations.size(); k++) {
			
			Map annotation = this.AnnotateQueryBasedOnCompleteKnowledge(query, BR, (HashMap) map_permutations.get(k));
			this.storeCorrectQueryAnnotation(k, annotation, "intersect");
			
		}

	  for (int exp_id = 0; exp_id < this.expriment_loop; exp_id ++) {
	  for (int i=0;i<this.pruning_loop;i++) {

				
				System.out.println("Pruning each base relation for the "+i+" time");
				for (int j =0; j< BR.size(); j++) {
					String br = BR.get(j).toString();
					System.out.println("Base relation: "+br);
					
					System.out.println("candidate_mapings for "+br+" is:"+candidate_mappings.get(br));
					System.out.print("Generate feedback ... ");
					feedback_ratios = this.pm.getFeedbackRatios(br,candidate_mappings.get(br),annotated_tp[j],annotated_fp[j],annotated_fn[j]);
					annotated_tp[j] = annotated_tp[j] + (Integer) feedback_ratios.get("tp");
					annotated_fp[j] = annotated_fp[j] + (Integer) feedback_ratios.get("fp");
					annotated_fn[j] = annotated_fn[j] + (Integer) feedback_ratios.get("fn");
					
					this.pm.generateFeedbackByType(br, (Integer) feedback_ratios.get("tp"), candidate_mappings.get(br), "tp");
					this.pm.generateFeedbackByType(br, (Integer) feedback_ratios.get("fp"), candidate_mappings.get(br), "fp_tuple");
					//this.pm.generateFeedbackByType(br, (Integer) feedback_ratios.get("fn"), candidate_mappings.get(br), "fn");
					System.out.println("done");

					System.out.print("Prune mappings given collected feedback instances ... ");
					this.pm.pruneMappings(br);
					System.out.println("done");
					
					System.out.println("Pruning Done for the base relation: "+br);
					
				}
				
				
				this.storeMappingAnnotation(i);
				
				
				// For each mapping permutation compute the precision and recall of the query 
				for (int k =0; k < map_permutations.size(); k++) {
					
					Map annotation = this.AnnotateQuery(propagated_query, BR, (HashMap) map_permutations.get(k));
					this.storeQueryAnnotation(i, k, annotation, "select");
					
				}
				
				System.out.print("Update log ... ");
				this.qrm.updateLog(BR,candidate_mappings);
				System.out.println("done");	
				
				System.out.print("Pruning results reiinitialisation ... ");
				this.qrm.reinitialisePruningResults();
				System.out.println("done");
			
	  }
	  
	  this.storeExperimentResult(exp_id);
	  this.storeExperimentMappingAnnotation(exp_id);
	
	  for (int l=0;l<BR.size();l++) {
		  qrm.reinitialiseDS(BR.get(l).toString());
		  qrm.reinitialisePruningResults();
		  qrm.initialiseLog(BR.get(l).toString());
	  }
	 
	  
	  }
	
	}
	

	public void PruneIntegrationRelation(String integration_relation) {
		
		  Vector candidate_mappings = new Vector();
		  Map feedback_ratios;
		  int annotated_tp = 0;
		  int annotated_fp = 0;
		  int annotated_fn = 0;

		  System.out.print("Retrieve Candidate Mappings for "+integration_relation +" ...");
		  candidate_mappings = this.pm.getMappings(integration_relation);
		  System.out.println("done");
		  
		  //Reinitialisation
		  qrm.reinitialiseDS(integration_relation);
		  qrm.reinitialisePruningResults();
		  qrm.initialiseLog(integration_relation);
		  this.reinitialiseMappingPermutations();
		  this.reinitialiseQueryCorrectAnnotations();
		  this.reinitialiseExperimentResults();

		  for (int exp_id = 0; exp_id < this.expriment_loop; exp_id ++) {
		  for (int i=0;i<this.pruning_loop;i++) {
			  System.out.println("Pruning each base relation for the "+i+" time");
			  System.out.println("candidate_mapings for "+integration_relation+" is:"+candidate_mappings);
						
			  System.out.print("Generate feedback ... ");
			  feedback_ratios = this.pm.getFeedbackRatios(integration_relation,candidate_mappings,annotated_tp,annotated_fp,annotated_fn);
			  annotated_tp = annotated_tp + (Integer) feedback_ratios.get("tp");
			  annotated_fp = annotated_fp + (Integer) feedback_ratios.get("fp");
			  annotated_fn = annotated_fn + (Integer) feedback_ratios.get("fn");
			  
			  this.pm.generateFeedbackByType(integration_relation, (Integer) feedback_ratios.get("tp"), candidate_mappings, "tp");
			  this.pm.generateFeedbackByType(integration_relation, (Integer) feedback_ratios.get("fp"), candidate_mappings, "fp_tuple");
			  //this.pm.generateFeedbackByType(integration_relation, (Integer) feedback_ratios.get("fn"), candidate_mappings.get(integration_relation), "fn");
			  System.out.println("done");
			  
			  System.out.print("Prune mappings given collected feedback instances ... ");
			  this.pm.pruneMappings(integration_relation);
			  System.out.println("done");
			  
			  this.storeMappingAnnotation(i);
			  
			  System.out.print("Update log ... ");
			  this.qrm.updateLog(integration_relation,candidate_mappings);
			  System.out.println("done");	
					
			  System.out.print("Pruning results reiinitialisation ... ");
			  this.qrm.reinitialisePruningResults();
			  System.out.println("done");
				
		  }
		  
		  this.storeExperimentResult(exp_id);
		  this.storeExperimentMappingAnnotation(exp_id);
		  
		  qrm.reinitialiseDS(integration_relation);
		  qrm.reinitialisePruningResults();
		  qrm.initialiseLog(integration_relation);
		 
		  
		  }
		
		}
		

	
	
	private void storeExperimentResult(int exp_id) {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st1,st2 = null;
			String query1 = "insert into exp_mapping_annotation (select "+exp_id+", m.* from his_mapping_cardinal_annotation m)";
			String query2 = "insert into exp_query_annotation_based_on_feedback (select "+exp_id+", q.* from query_annotation_based_on_feedback q)";
			st1 = conn.createStatement();
			st2 = conn.createStatement();
			st1.execute(query1);
			st1.close();
			st2.execute(query2);
			st2.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store experiment results");
			s.printStackTrace();
		}
		
	}
	
	private void reinitialiseMappingPermutations() {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st1 = null;
			String query1 = "delete from mapping_permutation";
			st1 = conn.createStatement();
			st1.execute(query1);
			st1.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store experiment results");
			s.printStackTrace();
		}
		
	}
	
	private void reinitialiseQueryCorrectAnnotations() {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st1 = null;
			String query1 = "delete from correct_query_annotation";
			st1 = conn.createStatement();
			st1.execute(query1);
			st1.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store experiment results");
			s.printStackTrace();
		}
		
	}
	
	private void reinitialiseExperimentResults() {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st1,st2 = null;
			String query1 = "delete from exp_query_annotation_based_on_feedback";
			String query2 = "delete from experiment_mapping_annotation";
			st1 = conn.createStatement();
			st1.execute(query1);
			st1.close();
			st2 = conn.createStatement();
			st2.execute(query2);
			st2.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to reinitialise experiment results");
			s.printStackTrace();
		}
		
	}


	private void displayPropagatedQueries(Map<String, String> propagated_query) {
		
		System.out.println("Propagated query:");
		System.out.println("tp:\n"+propagated_query.get("tp"));
		System.out.println("fp:\n"+propagated_query.get("fp"));
		System.out.println("fn:\n"+propagated_query.get("fn"));
		
	}

	private void displayMappingPermutations(ArrayList map_permutations) {
		
		System.out.println("Mapping permutrations");
		
		for (int i=0;i<map_permutations.size();i++) {		
			Map p = (Map) map_permutations.get(i);
			System.out.println("Permutation: "+i+1+"\n"+p.entrySet());
		}
		
	}
	
	private void displayAnnotation(Map<String,Double> annotation){
		
		System.out.println("precision: "+annotation.get("precision")+", recall: "+annotation.get("recall")+", f_measure: "+annotation.get("f_measure"));
	}

	public Map AnnotateQuery(Map<String,String> propagated_query, ArrayList BR, HashMap<String,Integer> map_permutation) {
		
		HashMap<String, Double> annotation = new HashMap();
		
		String tp = propagated_query.get("tp");
		String fp = propagated_query.get("fp");
		String fn = propagated_query.get("fn");
		String tp_table, fp_table, fn_table;
		
		for (int i = 0; i < BR.size(); i++) {
			
			String br = BR.get(i).toString();
			int mapping_id = map_permutation.get(br);
			
			System.out.println("mapping "+mapping_id+" to be used for populating "+br);
			tp_table = this.getTPQuery(mapping_id, br);
			fp_table = this.getFPQuery(mapping_id, br);
			fn_table = this.getFNQuery(mapping_id, br);
			
			tp = tp.replace("tp_"+br, tp_table);
			tp = tp.replace("fp_"+br, fp_table);
			tp = tp.replace("fn_"+br, fn_table);
			
			fp = fp.replace("tp_"+br, tp_table);
			fp = fp.replace("fp_"+br, fp_table);
			fp = fp.replace("fn_"+br, fn_table);
			
			fn = fn.replace("tp_"+br, tp_table);
			fn = fn.replace("fp_"+br, fp_table);
			fn = fn.replace("fn_"+br, fn_table);
			
		}
		
		System.out.println("tp:\n"+tp);
		System.out.println("fp:\n"+fp);
		System.out.println("fn:\n"+fn);
		
		int num_tp,num_fp,num_fn;
		num_tp = this.getResultNumber(tp);
		num_fp = this.getResultNumber(fp);
		num_fn = this.getResultNumber(fn);
		
		double precision, recall, f_measure;
		precision = (double) num_tp/(double) (num_tp+num_fp);
		recall = (double) num_tp/ (double) (num_tp+num_fn);
		f_measure = ((1 + (beta * beta)) * precision * recall)/ (((beta * beta) * precision) + recall);
		if (((Object)precision).toString().equals("NaN"))
			precision = -1;
		if (((Object)recall).toString().equals("NaN"))
			recall = -1;
		if (((Object)f_measure).toString().equals("NaN"))
			f_measure=-1;
		
		annotation.put("precision", precision);
		annotation.put("recall", recall);
		annotation.put("f_measure", f_measure);
		
		this.displayAnnotation(annotation);
		
		return annotation;
	}
	
	public Map AnnotateQueryBasedOnCompleteKnowledge(Query query, ArrayList BR, HashMap<String,Integer> map_permutation) {
		
		Map<String,String> propagated_query = this.pa.propagateBasedOnCompleteKnowledge(query);
		HashMap<String, Double> annotation = new HashMap();
		
		String tp = propagated_query.get("tp");
		String fp = propagated_query.get("fp");
		String fn = propagated_query.get("fn");
		String tp_table, fp_table, fn_table;
		
		System.out.println("Size of BR: "+BR.size());
		
		System.out.println("Rewrite query");
		
		for (int i = 0; i < BR.size(); i++) {
			
			
			String br = BR.get(i).toString();
			int mapping_id = map_permutation.get(br);
			
			System.out.println("mapping "+mapping_id+" to be used for populating "+br);
			tp_table = this.getTPQueryBasedOnCompleteKnowledge(mapping_id, br);
			fp_table = this.getFPQueryBasedOnCompleteKnowledge(mapping_id, br);
			fn_table = this.getFNQueryBasedOnCompleteKnowledge(mapping_id, br);
			
			/*
			System.out.println("Replace "+br+" in tp with tab_tp\n"+tp_table);
			System.out.println("tab_fp\n"+fp_table);
			System.out.println("tab_fn\n"+fn_table);
			*/
			
			tp = tp.replace("tp_"+br, tp_table);
			tp = tp.replace("fp_"+br, fp_table);
			tp = tp.replace("fn_"+br, fn_table);
			
			fp = fp.replace("tp_"+br, tp_table);
			fp = fp.replace("fp_"+br, fp_table);
			fp = fp.replace("fn_"+br, fn_table);
			
			fn = fn.replace("tp_"+br, tp_table);
			fn = fn.replace("fp_"+br, fp_table);
			fn = fn.replace("fn_"+br, fn_table);
			
		}
		
		System.out.println("correct tp:\n"+tp);
		System.out.println("correct fp:\n"+fp);
		System.out.println("correct fn:\n"+fn);
		
		int num_tp,num_fp,num_fn;
		num_tp = this.getResultNumber(tp);
		num_fp = this.getResultNumber(fp);
		num_fn = this.getResultNumber(fn);
		
		double precision, recall, f_measure;
		precision = (double) num_tp/(double) (num_tp+num_fp);
		recall = (double) num_tp/ (double) (num_tp+num_fn);
		f_measure = ((1 + (beta * beta)) * precision * recall)/ (((beta * beta) * precision) + recall);
		if (((Object)precision).toString().equals("NaN"))
			precision = -1;
		if (((Object)recall).toString().equals("NaN"))
			recall = -1;
		if (((Object)f_measure).toString().equals("NaN"))
			f_measure=-1;
		
		annotation.put("precision", precision);
		annotation.put("recall", recall);
		annotation.put("f_measure", f_measure);
		
		this.displayAnnotation(annotation);
		
		return annotation;
	}


	private ArrayList construct_map_permutations(ArrayList _BR, HashMap<String, Vector> candidate_mappings) {
		
		ArrayList BR = new ArrayList(_BR);
		ArrayList permutations = new ArrayList();
		
		if (BR.size() == 1) {
			for (int i = 0; i < candidate_mappings.get(BR.get(0).toString()).size(); i++) {
				HashMap<String,Integer> p = new HashMap();
				p.put(BR.get(0).toString(), (Integer) candidate_mappings.get(BR.get(0).toString()).get(i));
				permutations.add(p);
			}
		}
	
		if (BR.size() > 1) {
			String br = BR.get(BR.size() - 1).toString();
			Vector mappings = (Vector) candidate_mappings.get(br);
			BR.remove(BR.size() - 1);
			candidate_mappings.remove(br);
			ArrayList permutations1 = this.construct_map_permutations(BR, candidate_mappings); 
			for (int i = 0; i < mappings.size(); i++) {
				for (int j = 0; j < permutations1.size(); j++) {
					HashMap<String,Integer> p = new HashMap((HashMap) permutations1.get(j));
					
					//p = (HashMap) permutations1.get(j);
					p.put(br, (Integer) mappings.get(i));
					permutations.add(p);
				}
			}
		}		
			
		return permutations;
	}
	
	public void storeMappingPermutations(ArrayList mapping_permutations) {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.PreparedStatement st = null;
			String query = "insert into mapping_permutation values(?,?)";
			
			for (int i = 0; i < mapping_permutations.size(); i++) {
			
				Map p = (Map) mapping_permutations.get(i);
				Object[] keys = p.keySet().toArray();
				for (int j = 0; j < p.size(); j++) {
					
					st = conn.prepareStatement(query);
					st.setInt(1, i);
					st.setInt(2, (Integer) p.get(keys[j]));
					st.execute();
					
				}
			
			}
			
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store mapping permutations");
			s.printStackTrace();
		}
		
	}
	
	public void storeQueryAnnotation(int pruning_time, int permutation_id, Map annotation, String query_type) {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st = null;
			String query = "insert into query_annotation_based_on_feedback values("+pruning_time+","+permutation_id+",'"+query_type+"',"+annotation.get("precision").toString()+","+annotation.get("recall").toString()+","+annotation.get("f_measure")+")";
			System.out.println("");
			st = conn.createStatement();
			st.execute(query);
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store query annotation");
			s.printStackTrace();
		}

		
	}
	
	public void storeMappingAnnotation(int pruning_time) {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st = null;
			String query = "insert into mapping_annotation (select "+pruning_time+", m.* from mapping_cardinal_annotation m)";
			st = conn.createStatement();
			st.execute(query);
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store experiment mapping annotation");
			s.printStackTrace();
		}
	}
		
		public void storeExperimentMappingAnnotation(int exp_id) {
			
			try  {
				
				Connection	conn = source.getConnection(); 
				java.sql.Statement st = null;
				String query = "insert into experiment_mapping_annotation (select "+exp_id+", m.* from mapping_annotation m)";
				st = conn.createStatement();
				st.execute(query);
				st.close();
				conn.close();
				
			}catch (SQLException s){
				System.err.println("Error while trying to store experiment mapping annotation");
				s.printStackTrace();
			}	

		
	}
	
	public void storeCorrectQueryAnnotation(int permutation_id, Map annotation, String query_type) {
		
		try  {
			
			Connection	conn = source.getConnection(); 
			java.sql.Statement st = null;
			String query = "insert into correct_query_annotation values("+permutation_id+",'"+query_type+"',"+annotation.get("precision").toString()+","+annotation.get("recall").toString()+","+annotation.get("f_measure")+")";
			System.out.println("");
			st = conn.createStatement();
			st.execute(query);
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to store query annotation");
			s.printStackTrace();
		}

		
	}
	
	public int getResultNumber(String query_1) {
		
		int result_number = -1;
		String query = "select count(*) from ("+query_1+") tab_count";
		
		try  {
			Connection	conn = source.getConnection(); 
			java.sql.Statement st = null;

			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			if (rs.next())
				result_number = rs.getInt(1);
			st.close();
			rs.close();
			conn.close();
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}
		

		return result_number;
		
	}

	public void CorrectSnapshotId() {
		
		String query = null;
		
		try  {
			Connection	conn = source.getConnection(); 
			java.sql.Statement st = null;
			
			st = conn.createStatement();
			
			for (int i = 0; i < 199; i++) {
				query = "update exp_map set snapshotid = "+i
						+" where (snapshotid in" 
						+"(select min(e.snapshotid) from exp_map e where (char_length(e.snapshotid) > 3) group by e.exp_id))";
				st.executeUpdate(query);
			}
			st.close();
			conn.close();
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}
				
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Get Ratios of the feedback to be generated for each base relation
		// For this we need to revisit the following methods
		// PruneMappingsPostgres.getFeedbackRatios(this.wb.integration_query,top_mappings,annotated_tp,annotated_fp,annotated_fn)
		// PruneMappingsPostgres.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("tp"), top_mappings, "tp");
		// QueryResultManagement.updateLog(this.wb.integration_query,top_mappings);
		// PruneMappingsPostgres.pruneMappings(this.wb.integration_query);
		
		
		AnnotationPropagation ap = new AnnotationPropagation();
		String integration_relation = "integration.favorite_city";
		ap.PruneIntegrationRelation(integration_relation);
		
		
		//ap.CorrectSnapshotId();
		//Query query = ap.getQuery();
		//ap.PruneQuery(query);
		/*
		System.out.println(ap.getFNQuery(1,"integration.favorite_city"));
		*/
	}

}

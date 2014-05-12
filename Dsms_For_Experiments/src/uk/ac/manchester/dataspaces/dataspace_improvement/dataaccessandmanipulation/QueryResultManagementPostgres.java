/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappings;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappingsPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;

/**
 * @author Khalid Belhajjame
 *
 */

public class QueryResultManagementPostgres implements QueryResultManagement {

	
    //String integration_table = "europeancities";
	PruneMappings pm = null;
	public String snapshot_id;
	public Jdbc3PoolingDataSource source = null;
	Connection conn = null;
	
	public QueryResultManagementPostgres(Jdbc3PoolingDataSource _source) {
		super();
		this.source = _source;
		pm = new PruneMappingsPostgres(this.source);
		
	}


	@Override
	public void storeResults(String integration_query, ResultsSet res){
		String integration_table = integration_query.replace("integration.", "integration_");
		String[] attribute_names = new String[res.getHead().size()];
		for (int i = 0; i<attribute_names.length; i++) {
			attribute_names[i] =  res.getHead().get(i).toString();
		}
		
		
		for (int i =0;i<res.getResults().size();i++) {
			
			MappingResultSet map_res = (MappingResultSet) res.getResults().get(i);
			Vector map_res_vector = map_res.getBody();
			map_res_vector = erase_duplicates(map_res_vector);
			for (int j = 0; j< map_res_vector.size();j++) {
				Vector row = (Vector) map_res_vector.get(j);
				int result_id = exists(integration_table,attribute_names,row);
				if (result_id != -1) {
					addRef(integration_table,result_id,map_res.getMap().getId());
				}
				else {
					result_id = insertRow(integration_table,attribute_names,row);
					addRef(integration_table,result_id,map_res.getMap().getId());
				}
			}
			
			
		}
		
	}
	
	
	private Vector erase_duplicates(Vector _map_res_vector) {

		Vector map_res_vector = new Vector();
		
		for (int i = 0; i<_map_res_vector.size();i++) {
			Vector row = (Vector) _map_res_vector.get(i);
			if (!exists(map_res_vector,row))
				map_res_vector.add(row);
			
		}
		
		return map_res_vector;
	}

	private boolean exists(Vector map_res_vector, Vector row) {
		boolean found = false;
		search:
		for (int i=0; i< map_res_vector.size(); i++) {
			Vector v = (Vector) map_res_vector.get(i);
			for (int j = 0; j<v.size();j++) {
				if ((row.get(j) != null) && (v.get(j) != null))
					if (!row.get(j).toString().equals(v.get(j).toString()))
						break;
				if (j == v.size() - 1) {
					found = true;
					break search;
				}
			}
		}
		
		return found;
	}




	private void addRef(String integration_table, int result_id,
			int map_id) {
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 

			java.sql.Statement st = null;
			String query = "insert into mapping_"+integration_table+" values ("+map_id+",'"+result_id+"')";
				
		
			st = conn.createStatement();
			st.executeUpdate(query);
			
			st.close();
			conn.close();
		
		}catch (SQLException s){
			s.printStackTrace();		
		}

	
	}

	private int insertRow(String integration_table, String[] attribute_names,
			Vector row) {
		int result_id = 0;
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			
			java.sql.Statement st = null;
			String query = "insert into "+integration_table+" values (DEFAULT,";
		
		for (int i = 0; i<row.size();i++){
			if (row.get(i) == null)
				query = query + "null";
			else
				query = query + "'"+row.get(i).toString()+"'";
			if (i == row.size()-1)
				query = query +")";
			else
				query = query+",";
		}
		
		

			st = conn.createStatement();
			st.executeUpdate(query);
		
			ResultSet rs = st.getGeneratedKeys();
			if ( rs.next() ) {
			    // Retrieve the auto generated key(s).
			    result_id = rs.getInt(1);
			}
			rs.close();
			st.close();
			conn.close();
			
		}catch (SQLException s){
			s.printStackTrace();
			
		}
		
		return result_id;
	}

	private int exists(String integration_table, String[] attribute_names,
			Vector row) {


		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			java.sql.Statement st = null;
			String query = "select * from "+integration_table+" where ";
			String where_clause = "";
			for (int i=0;i<attribute_names.length;i++) {
				if (row.get(i) == null)
					if (where_clause.equals(""))
						where_clause = "("+ attribute_names[i] +" is "+ null+")";
					else
						where_clause = where_clause +" and ("+ attribute_names[i] +" is "+ null+")";
				else
					if (where_clause.equals(""))
						where_clause = "("+ attribute_names[i] +" = '"+ row.get(i).toString()+"')";
					else
						where_clause = where_clause+" and ("+ attribute_names[i] +" = '"+ row.get(i).toString()+"')";
			}
			query =  query + where_clause;
		
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			if (rs.next()) {
				return rs.getInt(1);
			}
			
			rs.close();
			st.close();
			conn.close();
			
		}catch (SQLException s){
			s.printStackTrace();
			
		}
		
		return -1;
	}
	

	@Override
	public void reinitialisePruningResults() {
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
		this.deleteFromTable("awf_order");
		this.deleteFromTable("tp_order");
		this.deleteFromTable("fp_order");
		this.deleteFromTable("mapping_cardinal_annotation");
		this.deleteFromTable("top_mappings");
		
		
		conn.close();
		
		} catch (SQLException s){
			s.printStackTrace();
			
		}
		
	}
	
	@Override
	public void reinitialiseDS(String integration_query) {
		
		
		String integration_table = integration_query.replace("integration.", "integration_");
		String query = "delete from attvaluepair";
		java.sql.Statement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from feedback_provenance";
			st = conn.createStatement();
			st.executeUpdate(query);

		
			query = "delete from feedback";
			st = conn.createStatement();
			st.executeUpdate(query);


			//query = "delete from tp_"+integration_table;
			query = "delete from expected_results";
			st = conn.createStatement();
			st.executeUpdate(query);


			//query = "delete from fp_"+integration_table;
			query = "delete from unexpected_results";
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from awf";
			st = conn.createStatement();
			st.executeUpdate(query);
 
			query = "delete from tp_order";
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from fp_order";
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from awf_order";
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from mapping_cardinal_annotation";
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from already_annotated_tuples";
			st = conn.createStatement();
			st.executeUpdate(query);

			query = "delete from already_annotated_attribute";
			st = conn.createStatement();
			st.executeUpdate(query);

			
			query = "delete from query_annotation_based_on_feedback";
			st = conn.createStatement();
			st.executeUpdate(query);
			
			query = "delete from mapping_annotation";
			st = conn.createStatement();
			st.executeUpdate(query);
			
			
			st.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();		
		}
		
	}
	
	public void initialiseLog(String integration_query){
		
		
		// Intialisation of the log
		
		String integration_table = integration_query.replace("integration.", "integration_");
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
		
			this.deleteFromTable("his_attvaluepair");
			this.deleteFromTable("his_awf");
			this.deleteFromTable("his_awf_order");
			this.deleteFromTable("his_feedback");
			this.deleteFromTable("his_feedback_provenance");
			this.deleteFromTable("his_unexpected_results");
			this.deleteFromTable("his_fp_order");
			this.deleteFromTable("his_mapping_cardinal_annotation");
			this.deleteFromTable("his_expected_results");
			this.deleteFromTable("his_tp_order");
			this.deleteFromTable("his_results_annotations");
			this.deleteFromTable("his_feedback_amount");
			this.deleteFromTable("his_top_mappings");
			this.deleteFromTable("his_ruled_out_mappings");

			
			conn.close();
			
		}catch (SQLException s){
			s.printStackTrace();		
		}
		
	}
	
	
	
	
	private void deleteFromTable(String table){
		
		String query = "delete from "+table;
		java.sql.Statement st = null;

		try{
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			st = conn.createStatement();
			st.executeUpdate(query);
			
			st.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}
	
	@Override
	public void processResults(String i_query) {

		String integration_table = i_query.replace("integration.", "integration_");
		Vector attributes = new Vector();
		Vector removed_results = new Vector();
		
		int id, id_1;

		// Retrieve information about the attributes that are projected by 
		// the integration query
		
		java.sql.Statement st = null;
		java.sql.Statement st_1 = null;
		ResultSet rs = null;
		ResultSet rs_1 = null; 
		String query = "SELECT * FROM "+integration_table;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
			// Retrieve information about existing operations in the database

			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			for (int i=1; i<=rs.getMetaData().getColumnCount();i++) {
				String att_name = rs.getMetaData().getColumnName(i);
				if (!att_name.equals("id"))
						attributes.add(att_name);
			}

			while (rs.next()){
				id = rs.getInt(1);
				Vector values = new Vector();
				for (int i=1;i<=attributes.size();i++)
					values.add(rs.getString(i+1));
				
				String query_1 = this.constructQuery(integration_table,attributes,values);
				
				st_1 = conn.createStatement();
				rs_1 = st_1.executeQuery(query_1);
				
				while (rs_1.next()) {
					
					id_1 = rs_1.getInt(1);
					if(!this.contains(removed_results, Integer.toString(id_1))) {
						this.changeId(integration_table,id_1,id);
						this.removeResult(integration_table,id_1);
						removed_results.add(Integer.toString((id_1)));
					}
		
				}
				
				rs_1.close();
				st_1.close();
				
			}
			
			rs.close();
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
		
	}

	private void removeResult(String integration_table, int id_1) {



		
		java.sql.PreparedStatement st = null;
		String query = "delete from "+integration_table+" where id = ?";
		
		
		// Retrieve information about existing operations in the database

		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.prepareStatement(query);
			st.setInt(1, id_1);
			st.executeUpdate();
			
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}

	private void changeId(String integration_table, int id_1, int id) {

		
		java.sql.PreparedStatement st = null;
		String query = "update mapping_"+integration_table+" set result_id = ? where result_id = ?";
		
		// Retrieve information about existing operations in the database

		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.prepareStatement(query);
			st.setInt(1, id);
			st.setInt(2, id_1);
			st.executeUpdate();
			
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		
	}

	private boolean contains(Vector removed_results, String id_1) {
		
		boolean found = false;
		for (int i=0;i<removed_results.size();i++)
			if (removed_results.get(i).toString().equals(id_1)) {
				found = true;
				break;
			}
		return found;
	}

	private String constructQuery(String integration_table, Vector attributes,
			Vector values) {
		
		String query = null;
		
		query = "select id from "+integration_table+
				" where ";
		for (int i=0;i<attributes.size();i++) {
			
			query = query + " ('"+attributes.get(i).toString()+"' = '"+values.get(i).toString()+"')";
			if (i != attributes.size() - 1)
				query = query + " and ";
			
		}
		
		return query;
	}


	@Override
	public void setFeedbackAmount(String snapshot_id) {
		

		String query = "select count(*) from feedback";
		String query1 = "insert into his_feedback_amount values (?,?)";
		java.sql.Statement st = null;
		java.sql.PreparedStatement st1 = null;
		ResultSet rs = null;
		int amount = 0;

		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			if (rs.next())
				amount = rs.getInt(1);
			
			
			st1 = conn.prepareStatement(query1);
			st1.setString(1,snapshot_id);
			st1.setInt(2,amount);
			
			st1.executeUpdate();
			
			rs.close();
			st.close();
			st1.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}
	
	@Override
	public void updateLog(String integration_query,Vector top_mappings) {
		
		
		String integration_table = "europeancities";


	   java.util.Date t = new java.util.Date();
	   String snapshot_id = new String(""+t.getTime()+"");
	   this.snapshot_id = snapshot_id;

			this.setFeedbackAmount(snapshot_id);
			this.updateTable("attvaluepair",snapshot_id);
			this.updateTable("awf",snapshot_id);
			this.updateTable("awf_order",snapshot_id);
			this.updateTable("feedback",snapshot_id);
			this.updateTable("feedback_provenance",snapshot_id);
			this.updateTable("unexpected_results",snapshot_id);
			this.updateTable("fp_order",snapshot_id);
			this.updateTable("mapping_cardinal_annotation",snapshot_id);
			this.updateTable("expected_results",snapshot_id);
			this.updateTable("tp_order",snapshot_id);
			this.updateTable("top_mappings", snapshot_id);
				
			this.annotateResults(snapshot_id, integration_query, top_mappings);
			
			
	}


	@Override
	public void annotateResults(String snapshot_id,String integration_query,Vector mappings) {

		int map_tp;
		int map_fp;
		double precision, recall, f_measure;

		// Retrieve the number of correct results
		int known_tp = this.pm.getNumberOfCorrectResults(integration_query);
		//System.out.println("Number of known correct results: "+known_tp);
		
		if (known_tp != 0) {
			// for each of the candidate mappings
				// compute the number of true positives
				map_tp = this.getCorrectResults(integration_query,mappings);
				System.out.println("Number of known true positives : "+map_tp);
				
				// compute the number of false positives
				map_fp = this.getIncorrectResults(integration_query, mappings);
				System.out.println("Number of false positives : "+map_fp);

				if ((map_tp != 0) || (map_fp != 0)) { 
					// compute the precison, recall and f_measure
					precision = (double) map_tp/(double) (map_tp+map_fp);
					//System.out.println("Precision: "+precision);
					recall = (double) map_tp/ (double) known_tp;
					//System.out.println("recall: "+recall);
					f_measure = ((1 + (this.pm.getBeta() * this.pm.getBeta())) * precision * recall)/ (((this.pm.getBeta() * this.pm.getBeta()) * precision) + recall);
					if (((Object)f_measure).toString().equals("NaN"))
						f_measure=0.0;
					//System.out.println("F measure: "+f_measure);

					// store the annotations in the dataspace repository
					this.saveResultsAnnotations(snapshot_id,precision,recall,f_measure,integration_query);
				}
			
		}
		
	}

	
	@Override
	public Map getActualResultsAnnotations(String integration_query,Vector mappings) {

		Map annotations = new HashMap();
		
		int map_tp;
		int map_fp;
		double precision, recall, f_measure;

		// Retrieve the number of correct results
		int known_tp = this.pm.getNumberOfCorrectResults(integration_query);
		//System.out.println("Number of known correct results: "+known_tp);
		
		if (known_tp != 0) {
			// for each of the candidate mappings
				// compute the number of true positives
				map_tp = this.getCorrectResults(integration_query,mappings);
				//System.out.println("Number of known true positives : "+map_tp);
				
				// compute the number of false positives
				map_fp = this.getIncorrectResults(integration_query, mappings);
				//System.out.println("Number of false positives : "+map_fp);

				if ((map_tp != 0) || (map_fp != 0)) { 
					// compute the precison, recall and f_measure
					precision = (double) map_tp/(double) (map_tp+map_fp);
					//System.out.println("Precision: "+precision);
					recall = (double) map_tp/ (double) known_tp;
					//System.out.println("recall: "+recall);
					f_measure = ((1 + (this.pm.getBeta() * this.pm.getBeta())) * precision * recall)/ (((this.pm.getBeta() * this.pm.getBeta()) * precision) + recall);
					if (((Object)f_measure).toString().equals("NaN"))
						f_measure=0.0;
					//System.out.println("F measure: "+f_measure);
					
					annotations.put("precision", precision);
					annotations.put("recall", recall);
					annotations.put("f_measure", f_measure);

					System.out.println("Actual Annotation: precision "+precision+", recall "+recall+", f_measure "+f_measure);
					
				}
			
		}
		
		return annotations;
	}

	

	
	private int getCorrectResults(String integration_query, Vector mappings) {
		
		int tp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		
		java.sql.PreparedStatement st = null;
		String query = "SELECT count(distinct r.result_id)  FROM mapping_"+integration_table+" r, correctresults_"+integration_table+" c where (r.result_id = c.id) and (";
		
		for (int i=0;i<mappings.size();i++) {
			if (i!=0)
				query = query + " or ";
			query = query + " (r.mapping_id = ?)";
		}
		query = query +")";
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.prepareStatement(query);
			for (int i=0;i<mappings.size();i++)
				st.setInt(i+1, (Integer)mappings.get(i));
			
			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				tp = rs.getInt(1);
				
			}
			
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		
		return tp;
	}

	private int getIncorrectResults(String integration_query, Vector mappings) {
		
		int tp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		
		java.sql.PreparedStatement st = null;
		String query = "SELECT count(distinct r.result_id)  FROM mapping_"+integration_table+" r where (";
		for (int i=0;i<mappings.size();i++) {
			if (i!=0)
				query = query + " or ";
			query = query + " (r.mapping_id = ?)";
		}
		query = query +") and (r.result_id not in (select c.id from correctresults_"+integration_table+" c))"; 

		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.prepareStatement(query);
			for (int i=0;i<mappings.size();i++)
				st.setInt(i+1, (Integer) mappings.get(i));
			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				tp = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		return tp;
	}

	private void saveResultsAnnotations(String snapshot_id, double precision,
			double recall, double f_measure, String base_table) {

		//System.out.println("Save Results annotations invoked: snapshot_id: "+snapshot_id+", precision: "+precision+", recall: "+recall+", f measure"+f_measure);

		
		java.sql.PreparedStatement st = null;
		String query = "insert into his_results_annotations values (?,?,?,?,?)";
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.prepareStatement(query);
			st.setString(1, snapshot_id);
			st.setDouble(2, precision);
			st.setDouble(3, recall);
			st.setDouble(4, f_measure);
			st.setString(5, base_table);
			st.executeUpdate();
			
			st.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}

	
	private void updateTable(String table, String snapshot_id) {
		
		String query = "insert into his_"+table+" select '"+snapshot_id+"', "+table+".* from "+table;
		java.sql.Statement st = null;

		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st = conn.createStatement();
			st.executeUpdate(query);
			
			st.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

	}


	@Override
	public String getSnapshotID() {

		return this.snapshot_id;
		
	}
	
	public static void main(String[] args) {
		
		Jdbc3PoolingDataSource source = ConnectionManager.getSource();
		QueryResultManagementPostgres qrm = new QueryResultManagementPostgres(source);
		Vector mappings = new Vector();
		//mappings.add(1);
		//mappings.add(2);
		//mappings.add(3);
		//mappings.add(4);
		//mappings.add(5);
		//mappings.add(6);
		//mappings.add(7);
		//mappings.add(8);
		//mappings.add(9);
		//mappings.add(11);
		mappings.add(12);
		//mappings.add(14);
		//mappings.add(15);
		//mappings.add(16);
		//mappings.add(17);
		//mappings.add(18);
		//mappings.add(19);
		//mappings.add(20);
		//mappings.add(22);
		//mappings.add(24);
		//mappings.add(23);
		//mappings.add(26);
		//mappings.add(27);
		//mappings.add(28);
		mappings.add(29);
		//mappings.add(30);
		//mappings.add(31);
		//mappings.add(32);
		//mappings.add(34);
		//mappings.add(35);
		//mappings.add(36);
		//mappings.add(37);
		//mappings.add(38);
		//mappings.add(39);
		//mappings.add(40);

		qrm.getActualResultsAnnotations("integration.europeancities", mappings);
	}


	@Override
	public void updateLog(ArrayList BR,
			HashMap<String, Vector> candidateMappings) {
	
		String integration_table = "europeancities";


		   java.util.Date t = new java.util.Date();
		   String snapshot_id = new String(""+t.getTime()+"");
		   this.snapshot_id = snapshot_id;

				this.setFeedbackAmount(snapshot_id);
				this.updateTable("attvaluepair",snapshot_id);
				this.updateTable("awf",snapshot_id);
				this.updateTable("awf_order",snapshot_id);
				this.updateTable("feedback",snapshot_id);
				this.updateTable("feedback_provenance",snapshot_id);
				this.updateTable("unexpected_results",snapshot_id);
				this.updateTable("fp_order",snapshot_id);
				this.updateTable("mapping_cardinal_annotation",snapshot_id);
				this.updateTable("expected_results",snapshot_id);
				this.updateTable("tp_order",snapshot_id);
				this.updateTable("top_mappings", snapshot_id);
					
				String integration_query;
				Vector mappings;
				for (int i=0;i<BR.size();i++) {
					integration_query = BR.get(i).toString();
					mappings = (Vector) candidateMappings.get(integration_query);
					this.annotateResults(snapshot_id,integration_query,mappings);
				}
		
	}


}

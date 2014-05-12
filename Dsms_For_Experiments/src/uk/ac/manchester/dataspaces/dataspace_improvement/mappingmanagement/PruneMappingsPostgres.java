package uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.ConnectionManager;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagement;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagementPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrieval;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrievalPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdate;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdatePostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;


public class PruneMappingsPostgres implements PruneMappings {

	public Jdbc3PoolingDataSource source = null;
	Connection conn = null;

	public SchemaMappingRetrieval smr = null;
    double beta = 1;
	FeedbackUpdate fu = null;
	Random generator = new Random();
	int num_attempts = 3;
	
	public PruneMappingsPostgres(Jdbc3PoolingDataSource _source) {
		super();
		this.source = _source;
		smr = new SchemaMappingRetrievalPostgres(this.source);
		fu = new FeedbackUpdatePostgres(this.source);
	}

	@Override
	public double getBeta() {
		return this.beta;
	}
	

	@Override
	public void identifyTP(String integration_query) {

		String integration_table = integration_query.replace("integration.", "integration_");
		
		String query_td = "SELECT f.id FROM feedback f where (f.relation = '"+integration_table+"') and (f.exists is true)";
		
		java.sql.Statement st_fd = null;
		java.sql.PreparedStatement st_v_a_p = null;
		java.sql.Statement st_tp = null;
		java.sql.PreparedStatement st_insert_tp = null;
		ResultSet rs_fd = null;
		ResultSet rs_v_a_p = null;
		ResultSet rs_tp = null;
		String query_tp = null;
		String query_insert_tp = "insert into expected_results values(?,?)";

		//System.out.println("query_fd: "+query_fd);
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection(); 
			st_fd = conn.createStatement();
			rs_fd = st_fd.executeQuery(query_td);
			int fd_id = 0;

			while ( rs_fd.next() ) {

				query_tp = "select id from "+integration_table+" where ";
				fd_id = rs_fd.getInt(1);
				String query_v_a_p = "SELECT attribute_name,value FROM attvaluepair a where a.feedback_id = ?";
				
				try {
					st_v_a_p = conn.prepareStatement(query_v_a_p);
					st_v_a_p.setInt(1, fd_id);
					rs_v_a_p = st_v_a_p.executeQuery();

					if (rs_v_a_p.next()) {
						query_tp = query_tp + " ("+rs_v_a_p.getString(1)+" = '"+rs_v_a_p.getString(2)+"') ";
					}
					while (rs_v_a_p.next()) {
						query_tp = query_tp + " and ("+rs_v_a_p.getString(1)+" = '"+rs_v_a_p.getString(2)+"') ";
					}
		
					//System.out.println("query_tp: "+query_tp);
				
					try {
						st_tp = conn.createStatement();
						rs_tp = st_tp.executeQuery(query_tp);
						st_insert_tp = conn.prepareStatement(query_insert_tp);
						
						while (rs_tp.next()){
							try {
								st_insert_tp.setInt(1, rs_tp.getInt(1));
								st_insert_tp.setString(2, integration_table);
								st_insert_tp.executeUpdate();

							}catch (SQLException s){
								System.err.println("Error while trying to execute the following query: "+query_insert_tp);
								s.printStackTrace();	
							}
						}
						
						st_insert_tp.close();
						st_tp.close();
						rs_tp.close();
						
					}catch (SQLException s){
						System.err.println("Error while trying to execute the following query: "+query_tp);
						s.printStackTrace();	
					}
					
					st_v_a_p.close();
					rs_v_a_p.close();
					
				}catch (SQLException s){
					System.err.println("Error while trying to execute the following query: "+query_v_a_p);
					s.printStackTrace();	
				}
				
			}
			
			st_fd.close();
			rs_fd.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query_td);
			s.printStackTrace();	
		}

		
	}

	@Override
	public void identifyResultWithoutFeedback(String integration_query) {

		String integration_table = integration_query.replace("integration.", "integration_");
		
		String query = "insert into awf SELECT id, '"+integration_table+"' FROM "+integration_table+" f where f.id not in (select id from expected_results e where (e.base_table ='"+integration_table+"') union select id from unexpected_results ue where (ue.base_table = '"+integration_table+"'))";
		
		
		java.sql.Statement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
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
	public void identifyFP(String integration_query) {

		String integration_table = integration_query.replace("integration.", "integration_");
		
		String query_fd = "SELECT f.id FROM feedback f where (f.relation = '"+integration_table+"') and (f.exists is false)";
		
		
		java.sql.Statement st_fd = null;
		java.sql.PreparedStatement st_v_a_p = null;
		java.sql.Statement st_fp = null;
		java.sql.PreparedStatement st_insert_fp = null;
		ResultSet rs_fd = null;
		ResultSet rs_v_a_p = null;
		ResultSet rs_fp = null;
		String query_fp = null;
		String query_insert_fp = "insert into unexpected_results values(?,?)";

		//System.out.println("query_fd: "+query_fd);
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection(); 
			st_fd = conn.createStatement();
			rs_fd = st_fd.executeQuery(query_fd);
			int fd_id = 0;

			while ( rs_fd.next() ) {

				query_fp = "select id from "+integration_table+" where ";
				fd_id = rs_fd.getInt(1);
				String query_v_a_p = "SELECT attribute_name,value FROM attvaluepair a where a.feedback_id = ?";
				
				try {
					st_v_a_p = conn.prepareStatement(query_v_a_p);
					st_v_a_p.setInt(1, fd_id);
					rs_v_a_p = st_v_a_p.executeQuery();

					if (rs_v_a_p.next()) {
						query_fp = query_fp + " ("+rs_v_a_p.getString(1)+" = '"+rs_v_a_p.getString(2)+"') ";
					}
					while (rs_v_a_p.next()) {
						query_fp = query_fp + " and ("+rs_v_a_p.getString(1)+" = '"+rs_v_a_p.getString(2)+"') ";
					}
		
					//System.out.println("query_fp: "+query_fp);
				
					try {
						st_fp = conn.createStatement();
						rs_fp = st_fp.executeQuery(query_fp); 
						st_insert_fp = conn.prepareStatement(query_insert_fp);

						while (rs_fp.next()){
							try {
								st_insert_fp.setInt(1, rs_fp.getInt(1));
								st_insert_fp.setString(2, integration_table);
								st_insert_fp.executeUpdate();
								//System.out.println("Result id: "+rs_fp.getInt(1)+" inserted in the false positives table");

							}catch (SQLException s){
								System.err.println("Error while trying to execute the following query: "+query_insert_fp);
								s.printStackTrace();	
							}
						}
						
						st_insert_fp.close();
						st_fp.close();
						rs_fp.close();
						
					}catch (SQLException s){
						System.err.println("Error while trying to execute the following query: "+query_fp);
						s.printStackTrace();	
					}
					
					st_v_a_p.close();
					rs_v_a_p.close();
					
				}catch (SQLException s){
					System.err.println("Error while trying to execute the following query: "+query_v_a_p);
					s.printStackTrace();	
				}
				
			}
			
			st_fd.close();
			rs_fd.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query_fd);
			s.printStackTrace();	
		}


	}
	
	public boolean subsumees(String integration_table,int map_1, int map_2, String criterion){
		
		boolean subsumee = false;
		

		
		if (getNumberOfResults(map_1,integration_table,criterion) != 0) {
		
				java.sql.PreparedStatement st = null;
				String query = "select * from "+integration_table+" i_1, mapping_"+integration_table+" mi_1, "+criterion+"_"+integration_table+" ct_1" 
				+" where (i_1.id = mi_1.result_id) and (ct_1.id = i_1.id) and (mi_1.mapping_id = ?) and (i_1.id not in (select i_2.id from "
				+integration_table+" i_2, mapping_"+integration_table+" mi_2, "+criterion+"_"+integration_table+" ct_2"
				+" where (i_2.id = mi_2.result_id) and (ct_2.id = i_2.id) and (mi_2.mapping_id = ?)))";
		
				//System.out.println("Query: "+query);
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection(); 		
				st = conn.prepareStatement(query);
				st.setInt(1, map_1);
				st.setInt(2, map_2);
				ResultSet rs = st.executeQuery();
		
				if (rs.next())
					subsumee = false;
				else
					subsumee = true;
			
				//System.out.println("map_1: "+map_1+" map_2: "+map_2);
			
				//System.out.println("Subsumee: "+subsumee);
				
				st.close();
				rs.close();
				conn.close();

			}catch (SQLException s){
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
	
		return subsumee;
	}

	public int getNumberOfResults(int map_id,String integration_table,String criterion) {
		
		int num_results = 0;

		java.sql.PreparedStatement st = null;
		String query = "SELECT count(distinct m.result_id) FROM mapping_"+integration_table+" m, "+criterion+"_"+integration_table+" i where (m.result_id = i.id) and (m.mapping_id = ?)";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection(); 
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
			ResultSet rs = st.executeQuery();
	
			if (rs.next())
				num_results = rs.getInt(1);
			
			st.close();
			rs.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}

		return num_results;
	}

	@Override
	public void order_awf(String integration_query) {

		this.order_mappings(integration_query, "awf");
		
	}

	@Override
	public void order_fp(String integration_query) {

		this.order_mappings(integration_query, "fp");
		
	}

	@Override
	public void order_tp(String integration_query) {

		this.order_mappings(integration_query, "tp");
		
	}

	@Override
	public void order_awf(String integration_query, int map_id) {

		this.order_mappings(integration_query, map_id, "awf");
		
	}

	@Override
	public void order_fp(String integration_query, int map_id) {

		this.order_mappings(integration_query, map_id, "fp");
		
	}

	@Override
	public void order_tp(String integration_query, int map_id) {

		this.order_mappings(integration_query, map_id, "tp");
		
	}

	
	public void order_mappings(String integration_query, String criterion) {
		
		
		Vector mappings = smr.getCandidateMappings(integration_query);
	
		String integration_table = integration_query;
		integration_table = integration_table.replace("integration.", "integration_");

		
		//System.out.println("Number of candidate mappings: "+mappings.size());
		
		for (int i = 0; i<mappings.size(); i++) {
			
			SchemaMapping map_1 = (SchemaMapping) mappings.get(i);
			Vector Map = new Vector();
			for (int j =0;j<mappings.size();j++){
				Map.add(mappings.get(j));
			}
			Map.remove(i);

			for (int j = 0; j< Map.size() ; j++) {
				SchemaMapping map_2 = (SchemaMapping) Map.get(j);
								
				if (subsumees(integration_table,map_1.id,map_2.id,criterion))
					insert(map_1.id,map_2.id,criterion);
			}
		}
		
	}

	public void order_mappings(String integration_query, int map_id, String criterion) {
		
		
		Vector mappings = smr.getCandidateMappings(integration_query);
	
		String integration_table = integration_query;
		integration_table = integration_table.replace("integration.", "integration_");


		for (int i = 0; i< mappings.size() ; i++) {

			SchemaMapping map_i = (SchemaMapping) mappings.get(i);
			
			if (map_id != map_i.id) {
				if (subsumees(integration_table,map_id,map_i.id,criterion))
					insert(map_id,map_i.id,criterion);
				if (subsumees(integration_table,map_i.id,map_id,criterion))
					insert(map_i.id,map_id,criterion);
			}
		}
				
	}

	
	
	public void insert(int map_1, int map_2, String criterion) {
		

		String query = "insert into "+criterion+"_order"+" values("+map_1+","+map_2+")";
		java.sql.Statement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			st.executeUpdate(query);

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
	}

	@Override
	public void annotateMappings(String integration_query) {

		// Retrieve the set of candidate mappings for the integration query
		Vector candidate_mapping_Ids = this.getMappings(integration_query);
		int map_tp;
		int map_fp;
		int map_results;
		double precision, recall, f_measure, annotated_results;
		
		// Retrieve the number of known true positives
		int known_tp = this.getNumberOfTruePositives(integration_query);
		
	
		if (known_tp != 0) {
			// for each of the candidate mappings
			for(int i=0;i<candidate_mapping_Ids.size();i++) {
				int map_id = (Integer) candidate_mapping_Ids.get(i);
				System.out.println("------------ mapping : "+map_id);
				
				// Retrieve the number of results returned by the mapping
				map_results = this.getMappingResults(integration_query,map_id);
				//System.out.println("Number of results returned by the mapping: "+map_results);
				
				// compute the number of true positives
				map_tp = this.getMappingTP(integration_query,map_id);
				
				//System.out.println("Number of true positives : "+map_tp);
				
				// compute the number of false positives
				map_fp = this.getMappingFP(integration_query, map_id);
				//System.out.println("Number of false positives : "+map_fp);

				if ((map_tp != 0) || (map_fp != 0)) { 
					// compute the precison, recall and f_measure
					precision = (double) map_tp/(double) (map_tp+map_fp);
					//precision = (((double) map_tp) * 0.12)/((((double)map_tp) * 0.12) + (((double) map_fp)* 0.88));
					//precision = 1 / (1 + (2.78 * ((double) map_fp / (double) map_tp)));
					//precision = 1 / (1 + (9 * ((double) map_fp / (double) map_tp)));
					//System.out.println("Precision: "+precision);
					//recall = (double) map_tp/ (double) known_tp;
					recall = (double) map_tp/ (double) known_tp;
					//System.out.println("recall: "+recall);
					f_measure = ((1 + (beta * beta)) * precision * recall)/ (((beta * beta) * precision) + recall);
					if (((Object)f_measure).toString().equals("NaN"))
						f_measure=0.0;
					//System.out.println("F measure: "+f_measure);
					annotated_results = (double) (map_tp + map_fp) / (double) map_results;
					//System.out.println("Ration of annotated results: "+annotated_results);
					// store the annotations in the dataspace repository
					this.saveAnnotations(map_id,precision,recall,f_measure,annotated_results);
				}
			
		}
		}
		
	}
	
	

	@Override
	public void annotateMapping(String integration_query, int map_id) {

		// Retrieve the set of candidate mappings for the integration query
		int map_tp;
		int map_fp;
		int map_results;
		double precision, recall, f_measure, annotated_results;
		
		// Retrieve the number of known true positives
		int known_tp = this.getNumberOfTruePositives(integration_query);
		
	
		if (known_tp != 0) {
				// Retrieve the number of results returned by the mapping
				map_results = this.getMappingResults(integration_query,map_id);
			
				// compute the number of true positives
				map_tp = this.getMappingTP(integration_query,map_id);
				
				//System.out.println("Number of known true positives : "+known_tp);
				
				// compute the number of false positives
				map_fp = this.getMappingFP(integration_query, map_id);
				//System.out.println("Number of false positives : "+map_fp);

				if ((map_tp != 0) || (map_fp != 0)) { 
					// compute the precison, recall and f_measure
					precision = (double) map_tp/(double) (map_tp+map_fp);
					//System.out.println("Precision: "+precision);
					recall = (double) map_tp/ (double) known_tp;
					//System.out.println("recall: "+recall);
					f_measure = ((1 + (beta * beta)) * precision * recall)/ (((beta * beta) * precision) + recall);
					if (((Object)f_measure).toString().equals("NaN"))
						f_measure=0.0;
					//System.out.println("F measure: "+f_measure);
					annotated_results = (double) (map_tp + map_fp) / (double) map_results;
					// store the annotations in the dataspace repository
					this.saveAnnotations(map_id,precision,recall,f_measure,annotated_results);
				}
			
		}
		
	}

	
	@Override
	public Map annotateResultsBasedOnUF(String integration_query, Vector mappings) {

		Map annotations = new HashMap();
		
		// Retrieve the set of candidate mappings for the integration query
		int maps_tp;
		int maps_fp;
		int maps_results;
		double precision, recall, f_measure, annotated_results;
		
		// Retrieve the number of known true positives
		int known_tp = this.getNumberOfTruePositives(integration_query);
		
	
		if (known_tp != 0) {
				// Retrieve the number of results returned by the mappings
				maps_results = this.getMappingsResults(integration_query,mappings);
			
				// compute the number of true positives
				maps_tp = this.getMappingsTP(integration_query,mappings);
				
				//System.out.println("Number of known true positives : "+maps_tp);
				
				// compute the number of false positives
				maps_fp = this.getMappingsFP(integration_query, mappings);
				//System.out.println("Number of false positives : "+maps_fp);

				if ((maps_tp != 0) || (maps_fp != 0)) { 
					// compute the precison, recall and f_measure
					precision = (double) maps_tp/(double) (maps_tp+maps_fp);
					//System.out.println("Precision: "+precision);
					recall = (double) maps_tp/ (double) known_tp;
					//System.out.println("recall: "+recall);
					f_measure = ((1 + (beta * beta)) * precision * recall)/ (((beta * beta) * precision) + recall);
					if (((Object)f_measure).toString().equals("NaN"))
						f_measure=0.0;
					//System.out.println("F measure: "+f_measure);
					annotated_results = (double) (maps_tp + maps_fp) / (double) maps_results;
					
					annotations.put("precision",precision);
					annotations.put("recall",recall);
					annotations.put("f_measure",f_measure);
					
					System.out.println("Annotation Based on UF: precision "+precision+", recall "+recall+", f_measure "+f_measure);
				}
			
		}
		
		return annotations;
		
	}

	
	@Override
	public void annotateMappingsCorrectly(String integration_query) {

		// Retrieve the set of candidate mappings for the integration query
		Vector candidate_mapping_Ids = this.getMappings(integration_query);
		int map_tp;
		int map_fp;
		double precision, recall, f_measure;
		
		// Retrieve the number of correct results
		int known_tp = this.getNumberOfCorrectResults(integration_query);
		System.out.println("Number of known true positives : "+known_tp);
		
		if (known_tp != 0) {
			// for each of the candidate mappings
			for(int i=0;i<candidate_mapping_Ids.size();i++) {
				int map_id = (Integer) candidate_mapping_Ids.get(i);
				System.out.println("mapping : "+map_id);
				
				// compute the number of true positives
				map_tp = this.getMappingCorrectResults(integration_query,map_id);
				System.out.println("#tp: "+map_tp);
				
				
				
				// compute the number of false positives
				map_fp = this.getMappingIncorrectResults(integration_query, map_id);
				System.out.println("Number of false positives : "+map_fp);

				if ((map_tp != 0) || (map_fp != 0)) { 
					// compute the precison, recall and f_measure
					precision = (double) map_tp/(double) (map_tp+map_fp);
					System.out.println("Precision: "+precision);
					recall = (double) map_tp/ (double) known_tp;
					System.out.println("recall: "+recall);
					f_measure = ((1 + (beta * beta)) * precision * recall)/ (((beta * beta) * precision) + recall);
					if (((Object)f_measure).toString().equals("NaN"))
						f_measure=0.0;
					//System.out.println("F measure: "+f_measure);

					// store the annotations in the dataspace repository
					this.saveCorrectAnnotations(map_id,precision,recall,f_measure);
				}
			
		}
		}
		
	}


	
	public void saveAnnotations(int map_id, double precision,
			double recall, double f_measure, double annotated_results) {

		
		java.sql.PreparedStatement st = null;
		String query = "insert into mapping_cardinal_annotation values (?,?,?,?,?)";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
			st.setDouble(2, precision);
			st.setDouble(3, recall);
			st.setDouble(4, f_measure);
			st.setDouble(5, annotated_results);
			st.executeUpdate();
			st.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}

	public void saveCorrectAnnotations(int map_id, double precision,
			double recall, double f_measure) {

		
		java.sql.PreparedStatement st = null;
		String query = "insert into correct_mapping_cardinal_annotation values (?,?,?,?)";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
			st.setDouble(2, precision);
			st.setDouble(3, recall);
			st.setDouble(4, f_measure);
			st.executeUpdate();
			st.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}



	
	public int getMappingTP(String integration_query, int map_id) {
		
		int tp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");

		
		java.sql.PreparedStatement st = null;
		String query = "select count(distinct tp.id) from expected_results tp, mapping_results m where (tp.base_table = '"+integration_table+"') and (m.base_table = '"+integration_table+"') and (tp.id = m.result_id) and (m.mapping_id = ?)";
		
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				tp = rs.getInt(1);
				
			}
			
			st.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		return tp;
	}

	public int getMappingsTP(String integration_query, Vector mappings) {
		
		int tp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		
		java.sql.PreparedStatement st = null;
		String query = "select count(distinct tp.id) from expected_results tp, mapping_results m where (tp.base_table = '"+integration_table+"') and (m.base_table = '"+integration_table+"') and (tp.id = m.result_id) and ";
		
		for (int i=0;i<mappings.size();i++) {
			if (i == 0)
				query = query + " (";
			else
				query = query + " or ";
			query = query + " (m.mapping_id = ?) ";
		}
		query = query +")";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			
			for (int i=0;i< mappings.size();i++) {
				st.setInt(i+1, (Integer) mappings.get(i));
			}

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
	
	public int getMappingsFP(String integration_query, Vector mappings) {
		
		int fp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		

		java.sql.PreparedStatement st = null;
		String query = "select count(distinct fp.id) from unexpected_results fp, mapping_results m where (fp.base_table = '"+integration_table+"') and (m.base_table = '"+integration_table+"') and (fp.id = m.result_id) and ";
		
		for (int i=0;i<mappings.size();i++) {
			if (i == 0)
				query = query + " (";
			else
				query = query + " or ";
			query = query + " (m.mapping_id = ?) ";
		}
		query = query +")";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			
			for (int i=0;i< mappings.size();i++) {
				st.setInt(i+1, (Integer) mappings.get(i));
			}
			
			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				fp = rs.getInt(1);
				
			}
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		
		return fp;
	}
	
	public int getMappingResults(String integration_query, int map_id) {
		
		int results = 0;
		String integration_table = integration_query.replace("integration.", "integration_");

		java.sql.PreparedStatement st = null;
		String query = "select count(distinct m.result_id) from mapping_"+integration_table+" m  where m.mapping_id = ?";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				results = rs.getInt(1);
				
			}
		
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		
		return results;
	}
	
	
	@Override
	public int getMappingsResults(String integration_query, Vector mappings) {
		
		int results = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		java.sql.PreparedStatement st = null;
		String query = "select count(distinct m.result_id) from mapping_"+integration_table+" m";
		
		for (int i=0;i<mappings.size();i++) {
			if (i==0)
				query = query + " where ";
			else
				query = query + " or ";
			query = query + " (m.mapping_id = ?) ";	
		}
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			
			for (int i=0;i<mappings.size();i++)
				st.setInt(i+1, (Integer) mappings.get(i));

			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				results = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		
		return results;
	}

	
	public int getMappingCorrectResults(String integration_query, int map_id) {
		
		int tp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		java.sql.PreparedStatement st = null;
		String query = "SELECT count(distinct r.result_id)  FROM mapping_"+integration_table+" r, correctresults_"+integration_table+" c where (r.result_id = c.id) and (r.mapping_id = ?)"; 
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
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

	
	public int getMappingIncorrectResults(String integration_query, int map_id) {
		
		int tp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		java.sql.PreparedStatement st = null;
		String query = "SELECT count(distinct r.result_id)  FROM mapping_"+integration_table+" r where (r.mapping_id = ?) and (r.result_id not in (select c.id from correctresults_"+integration_table+" c))"; 
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
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

	

	
	
	public int getMappingFP(String integration_query, int map_id) {
		
		int fp = 0;
		String integration_table = integration_query.replace("integration.", "integration_");

		
		java.sql.PreparedStatement st = null;
		String query = "select count(distinct fp.id) from unexpected_results fp, mapping_results m where (fp.base_table = '"+integration_table+"') and (m.base_table = '"+integration_table+"') and (fp.id = m.result_id) and (m.mapping_id = ?)";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map_id);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()) {
			
				fp = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		
		return fp;
	}

	
	public int getNumberOfTruePositives(String integration_query) {
		
		int num = 0;
		String integration_table = integration_query.replace("integration.", "integration_");

		java.sql.Statement st = null;
		String query = "select count(distinct id) from expected_results where (base_table = '"+integration_table+"')";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			if (rs.next()) {
			
				num = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
		return num;
	}

	@Override
	public int getNumberOfCorrectResults(String integration_query) {
		
		int num = 0;
		String integration_table = integration_query.replace("integration.", "integration_");
		java.sql.Statement st = null;
		String query = "select count(distinct id) from correctresults_"+integration_table;
		
		try  {
			if ((conn == null) || conn.isClosed() )  {
				conn = source.getConnection();
			}
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			if (rs.next()) {
			
				num = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
		return num;
	}

	
	@Override
	public Vector getMappings(String integration_query) {
		
		Vector candidate_mapping_Ids = new Vector();
		java.sql.PreparedStatement st = null;
		String query = "select id from schemamappings where integration_query = ?";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = this.source.getConnection();
			st = conn.prepareStatement(query);
			st.setString(1, integration_query);
			ResultSet rs = st.executeQuery();
		
			while (rs.next()) {
			
				candidate_mapping_Ids.add(rs.getInt(1));
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		return candidate_mapping_Ids;
	}

	@Override
	public void pruneMappings(String integration_query) {

		this.identifyTP(integration_query);
		this.identifyFP(integration_query);
		this.identifyResultWithoutFeedback(integration_query);
		//this.order_tp(integration_query);
		//this.order_fp(integration_query);
		//this.order_awf(integration_query);
		this.annotateMappings(integration_query);
		
	}
	
	
	@Override
	public void pruneMapping(String integration_query, int map_id)  {
	
		//this.order_tp(integration_query, map_id);
		//this.order_fp(integration_query, map_id);
		//this.order_awf(integration_query, mapi_id);
		this.annotateMapping(integration_query, map_id);		
		
	}

	@Override
	public boolean did_pruning_results_change(String integration_query) {
		
		String his_id = null;
		int map_1, map_2;
		Vector prev_mapping_ranking = new Vector();
		Vector curr_mapping_ranking = new Vector();
		
		java.sql.Statement st_1 = null, st_3 = null;
		java.sql.PreparedStatement st_2 = null;
		ResultSet rs_1 = null, rs_2 = null, rs_3 = null;
		String query_1 = "select snapshotid from his_mapping_cardinal_annotation order by snapshotid DESC limit 1";
		String query_2 = "select h.mapping_id from his_mapping_cardinal_annotation h where (h.snapshotid = ?) order by h.f_measure DESC";
		String query_3 = "select m.mapping_id from mapping_cardinal_annotation m order by m.f_measure DESC";

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st_1 = conn.createStatement();
			rs_1 = st_1.executeQuery(query_1);
		
			if (rs_1.next()) 
				his_id = rs_1.getString(1);
			else
				return true;
			
			st_2 = conn.prepareStatement(query_2);
			st_2.setString(1,his_id);
			rs_2 = st_2.executeQuery();
			
			while (rs_2.next())
				prev_mapping_ranking.add(rs_2.getInt(1));
			
			st_3 = conn.createStatement();
			rs_3 = st_3.executeQuery(query_3);
			
			while (rs_3.next())
				curr_mapping_ranking.add(rs_3.getInt(1));
			
			if (prev_mapping_ranking.size() < curr_mapping_ranking.size())
				return true;
			else 
				for (int i=0;i<curr_mapping_ranking.size();i++) {
					map_1 = (Integer) curr_mapping_ranking.get(i);
					map_2 = (Integer) prev_mapping_ranking.get(i);
					if (map_1 != map_2)
						return true;
				}

			st_3.close();
			rs_3.close();
			st_2.close();
			rs_2.close();			
			st_1.close();
			rs_1.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query_1);
			s.printStackTrace();
			
		}
		
		return false;
		
	}
	
	
	@Override
	public Vector selectTopKMappings(String integration_query, int top_k) {
		
		Vector candidate_mapping_Ids = new Vector();
		java.sql.PreparedStatement st = null;
		String query = "select id from schemamappings sm, mapping_cardinal_annotation mca where (sm.integration_query = ?) and (sm.id = mca.mapping_id) ORDER BY mca.f_measure DESC";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setString(1, integration_query);
			st.setMaxRows(top_k);
			ResultSet rs = st.executeQuery();
		
			while (rs.next()) {
			
				candidate_mapping_Ids.add(rs.getInt(1));
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		if (candidate_mapping_Ids.size() == 0)
			candidate_mapping_Ids = this.chooseRandomlyKMappings(integration_query, top_k);
		
		this.SaveTopMappings(candidate_mapping_Ids);
		
		System.out.print("Top mappings: ");
		for (int i=0;i<candidate_mapping_Ids.size();i++)
			System.out.print(" "+candidate_mapping_Ids.get(i)+" ");
		System.out.println();
		
		return candidate_mapping_Ids;
	}

	@Override
	public Vector selectMappingsThatCoverTP(String integration_query) {
		 
		Vector candidate_mapping_Ids = new Vector();
		String integration_table = integration_query.replace("integration.", "integration_");
		java.sql.Statement st = null;
		String query = "select distinct m.mapping_id from mapping_results m, expected_results t where (t.base_table = '"+integration_table+"') and (m.base_table = '"+integration_table+"') and (m.result_id = t.id) and " +
								"(m.mapping_id not in (select o.mapping_id_1 from tp_order o))";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			while (rs.next()) {
			
				candidate_mapping_Ids.add(rs.getInt(1));
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		if (candidate_mapping_Ids.size() == 0)
			candidate_mapping_Ids = this.chooseRandomlyKMappings(integration_query, 1);
		
		this.SaveTopMappings(candidate_mapping_Ids);
		
		System.out.print("Tope mappings: ");
		for (int i=0;i<candidate_mapping_Ids.size();i++)
			System.out.print(" "+candidate_mapping_Ids.get(i)+" ");
		System.out.println();
		
		return candidate_mapping_Ids;
	}

	
	
	@Override
	public Vector chooseRandomlyKMappings(String integration_query, int top_k) {

		Vector candidate_mapping_Ids = new Vector();
		java.sql.Statement st = null;
		String query = "select id from (select distinct id from schemamappings sm) rm ORDER BY RANDOM() LIMIT "+top_k;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			while (rs.next()) {
			
				candidate_mapping_Ids.add(rs.getInt(1));
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		return candidate_mapping_Ids;

		
	}
	
	public void TestIntersection() {

		
		String query1 = "select * from integration_europeancities where id <= 11784";
		String query2 = "select * from integration_europeancities where id <= 11780";
		
		this.getProjectedAttributes(query1);
		
		/*
		java.sql.PreparedStatement st = null;
		try {
			st = dataspaces_repository_con.prepareStatement(query);
			ResultSet rs = st.executeQuery();
			
			while (rs.next()) {
				System.out.println("ID: "+rs.getInt(1));
			}

			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		*/
		
	}
	
	public String[] getProjectedAttributes(String query){
		
		String[] attributes = null;
		java.sql.PreparedStatement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			
			attributes = new String[st.getMetaData().getColumnCount() - 1];
			
			for (int i=2;i<=st.getMetaData().getColumnCount();i++)
				attributes[i-2] = st.getMetaData().getColumnName(i);
			
			st.close();
			conn.close();

				
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		
		return attributes;
		
	}
	
	public String[] getAttributeValues(String query, String[] attributes){
		
		String[] values = null;
		java.sql.Statement st = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			
			if (rs.next()) {
				values = new String[attributes.length];
				for (int i=0;i<attributes.length;i++) 
					values[i] = rs.getObject(attributes[i]).toString();
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}

		return values;
		
	}
	


	@Override
	public void generateCorrectFeedbackInstance(String integration_query) {
		
		String integration_table = integration_query.replace("integration.", "integration_");
		int result_id = this.selectRandomlyACorrectResult(integration_table);
		//System.out.println("result id: "+result_id);
		if (result_id != -1) {
			String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
			String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+result_id,attributes);
			if (values != null) {
				//System.out.println("Generated true positive: "+result_id);
				fu.insertTP(integration_query,values,attributes);
			}
		}
	}

	@Override
	public void generateCorrectFeedbackInstance(String integration_query, Vector mappings) {
		
		String integration_table = integration_query.replace("integration.", "integration_");
		int result_id = this.selectRandomlyACorrectResult(integration_table,mappings);
		//System.out.println("result id: "+result_id);
		if (result_id != -1) {
			String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
			String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+result_id,attributes);
			if (values != null) {
				//System.out.println("Generated true positive: "+result_id);
				fu.insertTP(integration_query,values,attributes);
				this.insertAleradyAnnotatedTuple(result_id,integration_table);
			}
		}
	}

	private void insertAleradyAnnotatedTuple(int id, String integration_table) {
		
		String query = "insert into already_annotated_tuples values (?,?)";
		java.sql.PreparedStatement st = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, id);
			st.setString(2, integration_table);
			st.executeUpdate();
			st.close();
			conn.close();
	
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}
	
	private void insertAleradyAnnotatedAttribute(String att_name, String att_value, String integration_table) {
		
		String query = "insert into already_annotated_attribute values (?,?,?)";
		java.sql.PreparedStatement st = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setString(1, att_name);
			st.setString(2, att_value);
			st.setString(3, integration_table);
			st.executeUpdate();
			st.close();
			conn.close();
	
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
	}

	
	private void SaveTopMappings(Vector mappings){
		
		String query = "insert into top_mappings values (?,?)";
		java.sql.PreparedStatement st = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			for (int i=0;i<mappings.size();i++) {
				st.setInt(1, (Integer) mappings.get(i));
				st.setInt(2,i+1);
				st.executeUpdate();
			}
			st.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
	}

	
	public int selectRandomlyACorrectResult(String integration_table) {
		
		int result_id = -1;
		String query = "select id from (SELECT i.id FROM "+integration_table+" i, correctresults_"+integration_table+" c where (i.id = c.id)) rm order by RANDOM() limit 1";
		java.sql.Statement st = null;
		ResultSet rs = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			do {
				st = conn.createStatement();
			
				rs = st.executeQuery(query);
			
				if (rs.next()) {
					String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
					String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+rs.getInt(1),attributes);
					if (!this.alreadyGenerated(integration_table,attributes, values, true))
						result_id = rs.getInt(1);
				}
			} while ((--num_attempts >=0) && (result_id == -1));
			st.close();
			rs.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return result_id;
		
	}

	public int selectRandomlyACorrectResult(String integration_table, Vector mappings) {
		
		int result_id = -1;
		
		String query = "select id from (SELECT distinct i.id FROM "+integration_table+" i, correctresults_"+integration_table+" c, mapping_"+integration_table+" m where"+
				"(i.id = m.result_id) and (i.id = c.id) and (";
		for (int i=0; i<mappings.size();i++) {
			if (i != 0)
				query = query +" or ";
			int map_id = (Integer) mappings.get(i);
			query = query +"(m.mapping_id = "+map_id+") ";
		}
		
		query = query + ") and (i.id not in (select a.id from already_annotated_tuples a where a.base_table = '"+integration_table+"'))) rm order by RANDOM() limit 1";
		
		
		System.out.println("Query used for selecting a true positive\n"+query);
		
		java.sql.Statement st = null;
		ResultSet rs = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();		
			do {
				st = conn.createStatement();
			
				rs = st.executeQuery(query);
			
				if (rs.next()) {
					String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
					String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+rs.getInt(1),attributes);
					if (!this.alreadyGenerated(integration_table,attributes, values, true))
						result_id = rs.getInt(1);
				}
			} while ((--num_attempts >=0) && (result_id == -1));
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return result_id;
		
	}

	
	public int selectRandomlyAnIncorrectResult(String integration_table) {
		
		int result_id = -1;
		String query = "select id from (SELECT i.id FROM "+integration_table+" i where (i.id not in (select c.id from correctresults_"+integration_table+" c)) and (i.id not in (select a.id from already_annotated_tuples a))) rm order by RANDOM() limit 1"; 
		java.sql.Statement st = null;
		ResultSet rs = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			do {
				st = conn.createStatement();
			
				rs = st.executeQuery(query);
			
				if (rs.next()) {
					String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
					String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+rs.getInt(1),attributes);
					if (!this.alreadyGenerated(integration_table,attributes, values, false))
						result_id = rs.getInt(1);
				}
			} while ((--num_attempts >=0) && (result_id == -1));
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return result_id;
		
	}


	public Vector selectRandomlyAnIncorrectResults(String integration_table, Vector mappings, int num) {
		
		
		Vector result_ids = new Vector();
	
		/*
		String query = "SELECT distinct i.id FROM "+integration_table+" i where (i.id not in (select c.id from correctresults_"+integration_table+" c)) and (i.id in (select m.result_id from mapping_"+integration_table+" m where "; 
		for (int i=0; i<mappings.size();i++) {
			int map_id = (Integer) mappings.get(i);
			if (i!=0)
				query = query + " or ";
			query = query +" (m.mapping_id = "+map_id+") ";
		}
		*/

		String query = "select result_id from (SELECT i.result_id FROM mapping_"+integration_table+" i where " +
				// The following line was added temporarily
				//"((select count(e.result_id) from mapping_integration_europeancities e where e.result_id = i.result_id) = 3) and "+
				"(i.result_id not in (select c.id from correctresults_"+integration_table+" c)) and ("; 
		for (int i=0; i<mappings.size();i++) {
			int map_id = (Integer) mappings.get(i);
			if (i!=0)
				query = query + " or ";
			query = query +" (i.mapping_id = "+map_id+") ";
		}

		query = query + ") and (i.result_id not in (select a.id from already_annotated_tuples a where (a.base_table = '"+integration_table+"')))) rm order by RANDOM() limit "+num;

		//System.out.println(" Query for randomly selecting an incorrect answer:\n"+query);
		
		java.sql.Statement st = null;
		ResultSet rs  = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
				st = conn.createStatement();
			
				rs = st.executeQuery(query);
				
				
				while (rs.next()) {
					/*
					String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
					String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+rs.getInt(1),attributes);
					if (!this.alreadyGenerated(integration_table,attributes, values, false))
					*/
					result_ids.add(rs.getInt(1));
				}
				
				st.close();
				rs.close();
				conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return result_ids;
		
	}

		


	@Override
	public void generateIncorrectTupleFeedbackInstance(String integration_query) {

		String integration_table = integration_query.replace("integration.", "integration_");
		int result_id = this.selectRandomlyAnIncorrectResult(integration_table);
		//System.out.println("result id: "+result_id);
		if (result_id != -1) {
			//System.out.println("Generated False positive: "+result_id);
			String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
			String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+result_id,attributes);
			if (values != null) {
				fu.insertFP(integration_query,values,attributes);
			}
		}
		
	}

	@Override
	public void generateIncorrectTupleFeedbackInstances(String integration_query, Vector mappings, int num) {

		String integration_table = integration_query.replace("integration.", "integration_");
		Vector result_ids = this.selectRandomlyAnIncorrectResults(integration_table,mappings,num);
		for(int i=0;i<result_ids.size();i++) {
			//System.out.println("Generated False positive: "+result_id);
			String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
			String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+(Integer) result_ids.get(i),attributes);
			if (values != null) {
				fu.insertFP(integration_query,values,attributes);
				this.insertAleradyAnnotatedTuple((Integer)result_ids.get(i),integration_table);
			}
		}
		
	}

	
	@Override
	public void generateIncorrectAttributeFeedbackInstance(
			String integration_query) {
		
		String integration_table = integration_query.replace("integration.", "integration_");
		String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
		
		String selectedAttribute = attributes[generator.nextInt(attributes.length - 1)];
		
		String selectedValue = this.getRandomValue(integration_table,selectedAttribute);
		
		if (selectedValue != null) {
				String[] atts = new String[1];
				atts[0] = selectedAttribute;
				String[] vals = new String[1];
				vals[0] = selectedValue;
				fu.insertFP(integration_query,vals,atts);

		}
		
	}

	@Override
	public void generateIncorrectAttributeFeedbackInstances(
			String integration_query, Vector mappings, int num) {
		
		String integration_table = integration_query.replace("integration.", "integration_");
		String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
		
		String selectedAttribute = attributes[generator.nextInt(attributes.length - 1)];
		
		Vector selected_values = this.getRandomValues(integration_table,selectedAttribute,mappings,num);
		
		for (int i=0;i<selected_values.size();i++) {
				String[] atts = new String[1];
				atts[0] = selectedAttribute;
				String[] vals = new String[1];
				vals[0] = selected_values.get(i).toString();
				fu.insertFP(integration_query,vals,atts);
				this.insertAleradyAnnotatedAttribute(atts[0],vals[0],integration_table);

		}
		
	}

	
	public String getRandomValue(String integration_table,
			String selectedAttribute) {
		String selectedValue = null;
		String query = "SELECT "+selectedAttribute+" from (SELECT i."+selectedAttribute+" FROM "+integration_table+" i where (i."+selectedAttribute+" not in (select c."+selectedAttribute+" from correctresults_"+integration_table+
		" c)) and (i."+selectedAttribute+" not in (select a.attribute_value from already_annotated_attribute a where (a.attribute_name = '"+selectedAttribute+"')))) rm order by RANDOM() limit 1"; 

		java.sql.Statement st = null;
		ResultSet rs  = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			do {
				st = conn.createStatement();
			
				rs = st.executeQuery(query);
			
				if (rs.next()) {
					String[] attributes = new String[1];
					attributes[0] = selectedAttribute;
					String[] values = new String[1];
					values[0] = rs.getObject(1).toString();
					if (!this.alreadyGenerated(integration_table,attributes, values, false))
						selectedValue = rs.getObject(1).toString();
				}
			} while ((--num_attempts >=0) && (selectedValue == null));
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return selectedValue;
	}

	public boolean alreadyGenerated(String integration_table, String[] attributes, String[] values,boolean exists){

		boolean generated = false;
		String query = "select uf.id from feedback uf, ";
		for (int i=0;i<attributes.length;i++){
			query = query +" attvaluepair av_"+i;
			if (i != attributes.length - 1)
				query = query +", ";
		}
		query = query + " where (relation = ?) and (uf.exists = ?) ";
	
		for (int i=0;i<attributes.length;i++){
			query = query +" and (uf.id = av_"+i+".feedback_id) and (av_"+i+".attribute_name = ?) and (av_"+i+".value = ?) ";
		}
		
		int j=1;
		
		java.sql.PreparedStatement st = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			st = conn.prepareStatement(query);
			st.setString(j++, integration_table);
			st.setBoolean(j++, exists);
			
			for (int i=0;i<attributes.length;i++) {
				st.setObject(j++, attributes[i]);
				st.setObject(j++, values[i]);
			}
				
			
			ResultSet rs = st.executeQuery();
			
			if (rs.next())
				generated = true;
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		
		return generated;
	}
	
	public Vector getRandomValues(String integration_table,
			String selectedAttribute, Vector mappings, int num) {
		Vector selected_values = new Vector();
		
		if (mappings.size() ==0)
			return selected_values;

		String query = "SELECT "+selectedAttribute+" from (SELECT i."+selectedAttribute+" FROM "+integration_table+" i, mapping_"+integration_table+" m where (i."+selectedAttribute+" not in (select c."+selectedAttribute+" from correctresults_"+integration_table+" c)) and (i.id = m.result_id)  and ("; 
				for (int i=0; i<mappings.size();i++) {
					if (i != 0)
						query = query +" or ";
					int map_id = (Integer) mappings.get(i);
					query = query +"(m.mapping_id = "+map_id+") ";
				}
				query = query + ") and (i."+selectedAttribute+" not in (select a.attribute_value from already_annotated_attribute a where (a.attribute_name = '"+selectedAttribute+"') and (a.base_table = '"+integration_table+"')))) rm order by RANDOM() limit "+num; 

		//System.out.println("Query for randomly generating an incorrect value for the attribute "+selectedAttribute+" is "+query);		
				
		java.sql.Statement st = null;
		ResultSet rs = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			//do {
				st = conn.createStatement();
			
				rs = st.executeQuery(query);
			
				while (rs.next()) {
					/*
					String[] attributes = new String[1];
					attributes[0] = selectedAttribute;
					String[] values = new String[1];
					values[0] = rs.getObject(1).toString();
					if (!this.alreadyGenerated(attributes, values, false))
					*/
						selected_values.add(rs.getObject(1).toString());
				}
			//} while ((--num_attempts >=0) && (selectedValue == null));
				
				st.close();
				rs.close();
				conn.close();
				
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return selected_values;
	}

	
	@Override
	public void generateAFalseNegative(String integration_query) {

		String integration_table = integration_query.replace("integration.", "integration_");
		int result_id = this.selectRandomlyAFalseNegative(integration_table);
		//System.out.println("result id: "+result_id);
		if (result_id != -1) {
			String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
			String[] values = this.getAttributeValues("select * from correctresults_"+integration_table+" where id="+result_id,attributes);
			if (values != null) {
				fu.insertTP(integration_query,values,attributes);
			}
		}
		
	}


	@Override
	public void generateFalseNegatives(String integration_query, Vector mappings, int num) {

		String integration_table = integration_query.replace("integration.", "integration_");
		Vector result_ids = this.selectRandomlyFalseNegatives(integration_table,mappings,num);
		//System.out.println("result id: "+result_id);
		for (int i=0;i<result_ids.size();i++){
			String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
			String[] values = this.getAttributeValues("select * from correctresults_"+integration_table+" where id="+(Integer) result_ids.get(i),attributes);
			if (values != null) {
				fu.insertTP(integration_query,values,attributes);
				this.insertAleradyAnnotatedTuple((Integer) result_ids.get(i),integration_table);
			}
		}
	}

	
	public int selectRandomlyAFalseNegative(String integration_table) {

		int result_id = -1;
		String query = "SELECT i.id from (SELECT distinct i.id FROM correctresults_"+integration_table+" i where (i.id not in (select c.id from "+integration_table+" c))) rm order by RANDOM() limit 1"; 
		java.sql.Statement st = null;
		ResultSet rs = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			if (rs.next()) {
				String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
				String[] values = this.getAttributeValues("select * from correctresults_"+integration_table+" where id="+rs.getInt(1),attributes);
				//System.out.println("Query for FALSE POSITIVE: "+"select * from correctresults_"+integration_table+" where id="+rs.getInt(1));
				if (!this.alreadyGenerated(integration_table,attributes, values, true))
					result_id = rs.getInt(1);
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return result_id;
		
	}

	public Vector selectRandomlyFalseNegatives(String integration_table, Vector mappings, int num) {

		Vector result_ids = new Vector();
		String query = "select i.id from (SELECT distinct i.id FROM correctresults_"+integration_table+" i where (i.id not in (select j.* from ((select m.result_id from mapping_"+integration_table+" m  where "; 
				for (int i=0; i<mappings.size();i++) {
					int map_id = (Integer) mappings.get(i);
					if (i != 0)
						query = query + " or ";
					query = query +" (m.mapping_id = "+map_id+") ";
				}
				query = query + ") union (select a.id from already_annotated_tuples a where (a.base_table = '"+integration_table+"'))) j))) rm order by RANDOM() limit "+num;
				
				System.out.println("Query for selecting a false negative\n"+query);
		java.sql.Statement st = null;
		ResultSet rs = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			
			rs = st.executeQuery(query);
			
			while (rs.next()) {
				/*
				String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
				String[] values = this.getAttributeValues("select * from correctresults_"+integration_table+" where id="+rs.getInt(1),attributes);
				if (!this.alreadyGenerated(attributes, values, true))
				*/
				result_ids.add(rs.getInt(1));
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return result_ids;
		
	}

	
	@Override
	public void generateFeedback(String integration_query) {

		int positive_feedback = 3;
		int negative_feedback_tuple = 3;
		int negative_feedback_attribute = 3;
		int false_negatives = 1;
		
		for (int i=0;i<positive_feedback;i++)
			this.generateCorrectFeedbackInstance(integration_query);

		for (int i=0;i<negative_feedback_tuple;i++)
			this.generateIncorrectTupleFeedbackInstance(integration_query);

		for (int i=0;i<negative_feedback_attribute;i++)
			this.generateIncorrectAttributeFeedbackInstance(integration_query);
		
		for (int i=0;i<false_negatives;i++)
			this.generateAFalseNegative(integration_query);
		
	}

	@Override
	public void generateFeedback(String integration_query, int num) {

		int false_negatives = 1;
		
		for (int i=0;i<num;i++)
			this.generateCorrectFeedbackInstance(integration_query);

		for (int i=0;i<num;i++)
			this.generateIncorrectTupleFeedbackInstance(integration_query);

		for (int i=0;i<num;i++)
			this.generateIncorrectAttributeFeedbackInstance(integration_query);
		
		for (int i=0;i<false_negatives;i++)
			this.generateAFalseNegative(integration_query);
		
	}


	@Override
	public void generateFeedback(String integration_query, int num, Vector mappings) {


		
		int false_negatives = 1;
		
		for (int i=0;i<num;i++)
			this.generateCorrectFeedbackInstance(integration_query, mappings);

		//for (int i=0;i<num;i++)
			this.generateIncorrectTupleFeedbackInstances(integration_query, mappings,num);

		//for (int i=0;i<num;i++)
			this.generateIncorrectAttributeFeedbackInstances(integration_query, mappings,num);
		
		for (int i=0;i<false_negatives;i++)
			this.generateFalseNegatives(integration_query, mappings,num);
		
	}


	@Override
	public void generateFeedbackFromSample(String integration_query, int num, Vector mappings) {

		int num_tp=0, num_fp=0;
		String integration_table = integration_query.replace("integration.", "integration_");
		
		Vector sample = this.getSample(integration_table,num,mappings);
		
		for (int i=0;i<sample.size();i++) {

			int result_id = (Integer) sample.get(i);

			if (isResultExpected(integration_table,result_id)) {
				
				String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
				String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+result_id,attributes);
				if (values != null) {
					fu.insertTP(integration_query,values,attributes);
					this.insertAleradyAnnotatedTuple(result_id,integration_table);
					num_tp++;
				}				
			}
			
			else {
				String[] attributes = this.getProjectedAttributes("select * from "+integration_table);
				String[] values = this.getAttributeValues("select * from "+integration_table+" where id="+result_id,attributes);
				if (values != null) {
					fu.insertFP(integration_query,values,attributes);
					this.insertAleradyAnnotatedTuple(result_id,integration_table);
					num_fp++;
				}				
				
			}
		
		}
			System.out.println("Number of true positives is "+num_tp);
			System.out.println("Number of false positives is "+num_fp);
			
	}

	
	public boolean isResultExpected(String integration_table, int result_id) {
		
		boolean expected = false;
		
		String query = "select * from correctresults_"+integration_table+" where (id = ?)";
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			
			st = conn.prepareStatement(query);
			st.setInt(1,result_id);
			rs = st.executeQuery();
			if (rs.next())
				expected = true;
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return expected;
		
	}

	public Vector getSample(String integration_table,int num,Vector mappings) {
	
		Vector sample = new Vector();
	
		String query = "select result_id from (select i.result_id from mapping_"+integration_table+" i where (";
		for (int i=0;i<mappings.size();i++) {
			if (i != 0)
				query = query +" or ";
			query = query + "(i.mapping_id = "+(Integer)mappings.get(i)+")";
		}
		query = query +") and (i.result_id not in (select a.id from already_annotated_tuples a))) rm order by RANDOM()";
		
		java.sql.Statement st = null;
		ResultSet rs = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			st = conn.createStatement();
			st.setMaxRows(num);
			rs = st.executeQuery(query);
			while (rs.next())
				sample.add(rs.getInt(1));
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}

		return sample;
		
	}
	
	@Override
	public void generateFeedbackByType(String integration_query, int num, Vector mappings, String type) {


		if (type.equals("tp"))
			for (int i=0;i<num;i++)
				this.generateCorrectFeedbackInstance(integration_query, mappings);

		if (type.equals("fp_tuple")) {
				//System.out.print("Start the generation of tuple false positives ... ");
				this.generateIncorrectTupleFeedbackInstances(integration_query, mappings,num);
				//System.out.println("done");
			}
		
		if (type.equals("fp_attribute")) {
				//System.out.print("Start the generation of attribute false positives ... ");
				this.generateIncorrectAttributeFeedbackInstances(integration_query, mappings,num);
				//System.out.println("done");
			}

		if (type.equals("fn"))
			//for (int i=0;i<num;i++)
				this.generateFalseNegatives(integration_query, mappings,num);
		
	}

	
	@Override
	public void pruneMappings(String integration_query,
			Vector candidate_mappings) {
		
		int map_id;
		for (int i=0;i<candidate_mappings.size();i++) {
			map_id = (Integer) candidate_mappings.get(i);
			this.pruneMapping(integration_query, map_id);
		}
		
	}

	@Override
	public void ruleOutMappings(String snapshot_id, String integration_query, Double f_measure,
			Double support) {
	
		String query = "insert into his_ruled_out_mappings select '"+snapshot_id+"', m.mapping_id from mapping_cardinal_annotation m where (m.f_measure < ?) and (m.annotated_results > ?)";
		String query1 = "delete from mapping_cardinal_annotation where mapping_id in (select mapping_id from his_ruled_out_mappings)";
		String query2 = "delete from schemamappings where id in (select mapping_id from his_ruled_out_mappings)";
		
		java.sql.PreparedStatement st = null;
		java.sql.Statement st1,st2 = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();	
			st = conn.prepareStatement(query);
			st.setDouble(1,f_measure);
			st.setDouble(2,support);
			st.executeUpdate();
			
			st1 = conn.createStatement();
			st1.executeUpdate(query1);
			
			st2 = conn.createStatement();
			st2.executeUpdate(query2);
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query1+"\n or the query\n"+query2);
			s.printStackTrace();	
		}
			
	}

	@Override
	public Map getFeedbackRatios(String integration_query, Vector top_mappings, int annotated_tp, int annotated_fp, int annotated_fn) {
		
		HashMap ratios = new HashMap();
		int tp = 0, fp =0, fn=0;
		
		String integration_table = integration_query.replace("integration.", "integration_");

		
		// Query to get the number of expected results that are retrieved by top_mappings
		String query1 = "select count(distinct c.id) from correctresults_"+integration_table+" c, mapping_"+integration_table+" m where (c.id = m.result_id) and (";

		for (int i=0;i<top_mappings.size();i++) {
			if (i != 0)
				query1 = query1 + " or ";
			query1 = query1 +"(m.mapping_id = "+(Integer) top_mappings.get(i)+")"; 
		}
		query1 = query1 +")";
				
		// Query to get the number of results returned by top_mappings
		
		String query2 = "select count(distinct m.result_id) from mapping_"+integration_table+" m where ";
		for (int i=0;i<top_mappings.size();i++) {
			if (i != 0)
				query2 = query2 + " or ";
			query2 = query2 +"(m.mapping_id = "+(Integer) top_mappings.get(i)+")"; 
		}
		
		// Query get total number of expected results returned by all mappings
		String query3 = "select count(distinct c.id) from correctresults_"+integration_table+" c";
		

		java.sql.Statement st1=null,st2=null,st3 = null;
		ResultSet rs1=null,rs2=null,rs3=null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			
			st1 = conn.createStatement();
			rs1 = st1.executeQuery(query1);
			
			if (rs1.next())
				tp = rs1.getInt(1);
			
			st2 = conn.createStatement();
			rs2 = st2.executeQuery(query2);
			
			if (rs2.next())
				fp = rs2.getInt(1) - tp;
			
			st3 = conn.createStatement();
			rs3 = st3.executeQuery(query3);
			
			if (rs3.next())
				fn = rs3.getInt(1) - tp;
			
			st1.close();
			rs1.close();
			st2.close();
			rs2.close();
			st3.close();
			rs3.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query1+"\n or the query\n"+query2);
			s.printStackTrace();	
		}

				
		System.out.println("\n Remaining tp "+tp+", fp "+fp+", fn "+fn);
		
		int total = tp +fp+fn;
		double tp_r = (double) tp * (10 + annotated_tp + annotated_fp + annotated_fn) /(double) total;
		double fp_r = (double) fp * (10 + annotated_tp + annotated_fp + annotated_fn) /(double) total;
		double fn_r = (double) fn * (10 + annotated_tp + annotated_fp + annotated_fn) /(double) total;

		System.out.println(" rounded tp ratio "+Math.round(tp_r - annotated_tp)+", fp ratio "+Math.round(fp_r - annotated_fp)+", fn ratio "+Math.round(fn_r - annotated_fn));	
		
		ratios.put("tp",(int) Math.round(tp_r - annotated_tp));
		ratios.put("fp",(int) Math.round(fp_r - annotated_fp));
		ratios.put("fn",(int) Math.round(fn_r - annotated_fn));
		
		return ratios;
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
	
	public void PruneIntegrationRelation(String integration_relation, int num_experiments, int num_iterations_per_experiment) {
		
		  QueryResultManagement qrm = new QueryResultManagementPostgres(this.source);
		
		  Vector candidate_mappings = new Vector();
		  Map feedback_ratios;
		  int annotated_tp = 0;
		  int annotated_fp = 0;
		  int annotated_fn = 0;

		  System.out.print("Retrieve Candidate Mappings for "+integration_relation +" ...");
		  candidate_mappings = this.getMappings(integration_relation);
		  System.out.println("done");
		  
		  //Reinitialisation
		  qrm.reinitialiseDS(integration_relation);
		  qrm.reinitialisePruningResults();
		  qrm.initialiseLog(integration_relation);
		  this.reinitialiseExperimentResults();

		  for (int exp_id = 0; exp_id < num_experiments; exp_id ++) {
		  for (int i=0;i<num_iterations_per_experiment;i++) {
			  System.out.println("Pruning each base relation for the "+i+" time");
			  System.out.println("candidate_mapings for "+integration_relation+" is:"+candidate_mappings);
						
			  System.out.print("Generate feedback ... ");
			  feedback_ratios = this.getFeedbackRatios(integration_relation,candidate_mappings,annotated_tp,annotated_fp,annotated_fn);
			  annotated_tp = annotated_tp + (Integer) feedback_ratios.get("tp");
			  annotated_fp = annotated_fp + (Integer) feedback_ratios.get("fp");
			  annotated_fn = annotated_fn + (Integer) feedback_ratios.get("fn");
			  
			  this.generateFeedbackByType(integration_relation, (Integer) feedback_ratios.get("tp"), candidate_mappings, "tp");
			  this.generateFeedbackByType(integration_relation, (Integer) feedback_ratios.get("fp"), candidate_mappings, "fp_tuple");
			  //this.generateFeedbackByType(integration_relation, (Integer) feedback_ratios.get("fn"), candidate_mappings.get(integration_relation), "fn");
			  System.out.println("done");
			  
			  System.out.print("Prune mappings given collected feedback instances ... ");
			  this.pruneMappings(integration_relation);
			  System.out.println("done");
			  
			  this.storeMappingAnnotation(i);
			  
			  System.out.print("Update log ... ");
			  qrm.updateLog(integration_relation,candidate_mappings);
			  System.out.println("done");	
					
			  System.out.print("Pruning results reiinitialisation ... ");
			  qrm.reinitialisePruningResults();
			  System.out.println("done");
				
		  }
		  
		  this.storeExperimentResult(exp_id);
		  this.storeExperimentMappingAnnotation(exp_id);
		  
		  qrm.reinitialiseDS(integration_relation);
		  qrm.reinitialisePruningResults();
		  qrm.initialiseLog(integration_relation);
		 
		  
		  }
		
		}
		

	
	public static void main(String[] args) {
		
		Jdbc3PoolingDataSource source = ConnectionManager.getSource();
		PruneMappingsPostgres pm = new PruneMappingsPostgres(source);
		
		String integration_relation = "integration.favorite_city";
		int num_experiments = 2;
		int num_iterations_per_experiment = 110;
		
		pm.PruneIntegrationRelation(integration_relation, num_experiments, num_iterations_per_experiment);
		
		/*pm.TestIntersection();
		Vector mappings = new Vector();
		mappings.add(29);
		mappings.add(12);
		mappings.add(11);
		pm.annotateResultsBasedOnUF("integration.europeancities",mappings);
		*/
	}
	
}

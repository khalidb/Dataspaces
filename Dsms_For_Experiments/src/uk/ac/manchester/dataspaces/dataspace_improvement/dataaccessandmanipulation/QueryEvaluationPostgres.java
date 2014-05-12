package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Vector;
import java.util.Random;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;


import uk.ac.manchester.dataspaces.dataspace_improvement.util.IntegrationQuery;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;


public class QueryEvaluationPostgres implements QueryEvaluation {

	Jdbc3PoolingDataSource source = null;
	Connection conn = null;
	
	/*
	Connection sources_con = null;
    Connection dataspaces_repository_con = null;
    Connection integration_con = null;
    String url = "jdbc:mysql://localhost:3306/";
    String sources_db = "mondial";
    String integration_db = "integration";
    String dataspaces_repository_db = "dataspaces_repository";
    String driver = "com.mysql.jdbc.Driver";
    String user = "dataspaces";
    String pass = "adana";
    */
    String mapping_selection = "ALL";
    Random generator = new Random();
    

	public QueryEvaluationPostgres(Jdbc3PoolingDataSource _source) {
		super();
		this.source = _source;
	}

	@Override
	public MappingResultSet evaluateSourceQuery(String s_query) {
		 

		MappingResultSet res = new MappingResultSet();
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection(); 
		
			java.sql.PreparedStatement st = null;
			String query = null;
		
			st = conn.prepareStatement(s_query);
			ResultSet rs = st.executeQuery();
			int number_of_columns = rs.getMetaData().getColumnCount();
			Vector res_head = new Vector();
			for (int i=1; i<=number_of_columns;i++){
				res_head.add(rs.getMetaData().getColumnName(i));
			}
			res.setHead(res_head);
			
			while (rs.next()){
				
				Vector res_e = new Vector();
				
				for (int i=1;i<=number_of_columns;i++) {
					res_e.add(rs.getObject(i));
				}
				
				res.addResultElement(res_e);
			}
			st.close();
			rs.close();
			conn.close();
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+s_query);
			s.printStackTrace();
		}
		
		//this.displayResults(res);

		return res;
	}
	
	private void displayResults(MappingResultSet res){
		
		System.out.print("\nHead:   ");

		for (int i=0;i<res.getHead().size();i++)
			System.out.print("    "+res.getHead().get(i));
		
		for (int i=0;i<res.getBody().size();i++) {
			System.out.print("\n t_"+i);
			Vector res_e = (Vector)res.getBody().get(i);

			for (int j=0;j<res_e.size();j++) 
				System.out.print("    "+res_e.get(j).toString());
		}
	}
	

	@Override
	public ResultsSet evaluateIntegrationQuery(String  i_query) {
		
		ResultsSet results_set = new ResultsSet();
		
		results_set.setHead(this.getHead(i_query));
		
		Vector results = new Vector();
		
		Vector c_mappings = this.getCandidateMappings(i_query);
		Vector s_mappings = this.selectMappings(c_mappings);
		
		for (int i=0;i<s_mappings.size();i++) {
			SchemaMapping map = (SchemaMapping) s_mappings.get(i);
			MappingResultSet rs = this.evaluateSourceQuery(map.getS_query());	
			rs.setMap(map);
			results_set.addResults(rs);
		}
		
		return results_set;
	}

	@Override
	public Vector getCandidateMappings(String i_query) {
		Vector c_mappings = new Vector();
		
		try {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection(); 
		
			java.sql.PreparedStatement st = null;
			String query = "SELECT * FROM schemamappings WHERE integration_query = ?";
		
		
			// Retrieve information about existing operations in the database
			st = conn.prepareStatement(query);
			st.setString(1, i_query);
			ResultSet rs = st.executeQuery();

			while (rs.next()){
				SchemaMapping map = new SchemaMapping();
				map.setId(rs.getInt(1));
				map.setIntegrationRelation(rs.getString(2));
				map.setS_query(rs.getString(3));
				c_mappings.add(map);
			}
			st.close();
			rs.close();
			conn.close();
		}catch (SQLException s){
			s.printStackTrace();
			
		}
		
		return c_mappings;
	}

	@Override
	public Vector selectMappings(Vector c_mappings) {
		Vector s_mappings = null;
		
		if (this.mapping_selection.equals("ALL"))
			s_mappings = c_mappings;
		
		if (this.mapping_selection.equals("RANDOM")) {
			s_mappings = new Vector();
			int pick = this.generator.nextInt(c_mappings.size());
			s_mappings.add(c_mappings.get(pick));
		}
		
		return s_mappings;
	}

	@Override
	public Vector getHead(String i_query) {
		Vector head = new Vector();
		
		try {
			if (conn == null || conn.isClosed()) 	
				conn = source.getConnection(); 

			java.sql.Statement st = null;
			String query = "SELECT * FROM "+i_query;

			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query) ;
			
			// Get the metadata
		    ResultSetMetaData md = rs.getMetaData();

		    // Print the column labels
		    for( int i = 1; i <= md.getColumnCount(); i++ ) {
		    	head.add(md.getColumnLabel(i));
		    	System.out.println(md.getColumnLabel(i));
		    }
		   st.close();
		   rs.close();
		   conn.close();

		}catch (SQLException s){
			s.printStackTrace();
			
		}
		
		
		return head;
	}


	public static void main(String[] args){
		
		Jdbc3PoolingDataSource source = ConnectionManager.getSource();
		QueryEvaluation qe = new QueryEvaluationPostgres(source);
		qe.getHead("integration.protein");
		
		qe.evaluateSourceQuery("select * from sources.pedro_sample");
		
		source.close();
		/*
		Vector results = qe.evaluateSourceQuery(s_query);
		for (int i=0; i<results.size();i++) {
			Result op = (Result) results.get(i);
			System.out.println("results: "+op.getName());
		}
		*/
			
	}


}

/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;

/**
 * @author Khalid Belhajjame
 *
 */
public class FeedbackUpdatePostgres implements FeedbackUpdate {
	
	Jdbc3PoolingDataSource source = null;
	Connection conn = null;

	
	public FeedbackUpdatePostgres(Jdbc3PoolingDataSource _source) {
		super();
		this.source = _source;
	}

	@Override
	public void insertFN(String integration_query, String[] values, String[] attributes) {
		
		String integration_table = integration_query.replace("integration.", "integration_");
		Map att_value_pairs = new HashMap();
		if (values.length == attributes.length) {
			for(int i = 0; i<values.length; i++)
				att_value_pairs.put(attributes[i], values[i]);
			this.insertFeedback(integration_table, att_value_pairs, true, true);
		}
		else
			System.err.println("The attributes and values list must be of the same size");


	}

	@Override
	public void insertFP(String integration_query, String[] values, String[] attributes) {
		String integration_table = integration_query.replace("integration.", "integration_");
		Map att_value_pairs = new HashMap();
		if (values.length == attributes.length) {
			for(int i = 0; i<values.length; i++)
				att_value_pairs.put(attributes[i], values[i]);
			this.insertFeedback(integration_table, att_value_pairs, false, false);
		}
		else
			System.err.println("The attributes and values list must be of the same size");
		
	}

	@Override
	public void insertTP(String integration_query, String[] values, String[] attributes) {

		String integration_table = integration_query.replace("integration.", "integration_");
		Map att_value_pairs = new HashMap();
		if (values.length == attributes.length) {
			for(int i = 0; i<values.length; i++)
				att_value_pairs.put(attributes[i], values[i]);
			this.insertFeedback(integration_table, att_value_pairs, true, false);
		}
		else
			System.err.println("The attributes and values list must be of the same size");
		

	}
	
	private void insertFeedback( String integration_table, Map att_value_pairs, boolean exists, boolean user_specified){
		
		
		java.sql.Statement st_fd = null;
		java.sql.Statement st_attValuePair = null;
		ResultSet rs_fd = null;
		String query_fd = "insert into feedback values(DEFAULT,'"+integration_table+"',"+user_specified+","+exists+")";

		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 
			st_fd = conn.createStatement();
			
			/*
			st_fd.executeUpdate(query_fd);
			rs_fd = st_fd.getGeneratedKeys();
			
			int key_fd=0;
			if ( rs_fd.next() ) {
			    // Retrieve the auto generated key(s).
			    key_fd = rs_fd.getInt(1);
			    System.out.println("Generate key: "+key_fd);
			}
			*/
			
			// We cannot use the JDBC operation getGeneratedKey as listed in the above commented 
			// code. Because the current version of postgres does not support it. 
			rs_fd = st_fd.executeQuery(query_fd + " RETURNING feedback.id");
			rs_fd.next();
			int key_fd = rs_fd.getInt(1); 
			
			
			 String query_attValuePair  = null;
			 String attribute_name = null;
			 String attribute_value = null;
		     Set set= att_value_pairs.keySet (  ) ; 
		     Iterator iter = set.iterator (  ) ; 
		     int i=1; 
		     while ( iter.hasNext (  )  )  { 
		    	 attribute_name = iter.next().toString();
		    	 attribute_value = att_value_pairs.get (attribute_name).toString();
		    	 query_attValuePair = "insert into attvaluepair values(DEFAULT,'"+attribute_name+"','"+attribute_value+"',"+key_fd+")";
		    	 st_attValuePair = conn.createStatement();
		    	 st_attValuePair.executeUpdate(query_attValuePair);
		    	 st_attValuePair.close();

		      }  
		     
		     st_fd.close();
		     rs_fd.close();
		     conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query_fd);
			s.printStackTrace();
			
		}
	}
	


}

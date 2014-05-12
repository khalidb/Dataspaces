package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

/*
 * Used for creating a connection to the dataspaces repository
 */

public final class ConnectionManager {
		
	
	public static Jdbc3PoolingDataSource getSource(){
		
		
		// Initialize the proxy settings etc.
		ResourceBundle resbundle = ResourceBundle.getBundle("dataspaces_repository");
	    Properties systemProps = System.getProperties();
	    Enumeration ks = resbundle.getKeys();
		while (ks.hasMoreElements()) {
	            String key = (String) ks.nextElement();
	            String value = (String) resbundle.getString(key);
		    systemProps.put(key, value);
	        }
		
		Jdbc3PoolingDataSource source = new Jdbc3PoolingDataSource();
		source.setDataSourceName("dataspaces");
		source.setServerName(System.getProperty("host"));
		source.setDatabaseName(System.getProperty("db_name"));
		System.out.println("DATABASE NAME: "+source.getDatabaseName());
		source.setUser(System.getProperty("user_name"));
		source.setPassword(System.getProperty("passwd"));
		source.setMaxConnections(5);

		return source;
		
	}
	
	public static Connection connect(){
		
		Connection conn = null;
		
		// Initialize the proxy settings etc.
		ResourceBundle resbundle = ResourceBundle.getBundle("dataspaces_repository");
	    Properties systemProps = System.getProperties();
	    Enumeration ks = resbundle.getKeys();
		while (ks.hasMoreElements()) {
	            String key = (String) ks.nextElement();
	            String value = (String) resbundle.getString(key);
		    systemProps.put(key, value);
	        }
		String db_url = System.getProperty("db_url");
		String driver = System.getProperty("driver");
		String userName = System.getProperty("user_name");
		String passwd = System.getProperty("passwd");
		
        try
        {
            Class.forName (driver).newInstance ();
            conn = DriverManager.getConnection (db_url, userName, passwd);
            System.out.println ("Database connection established");
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        	System.err.println ("Cannot connect to database server");
        }
        
        return conn;
	
	}
	
	
	public static void close(Connection conn) {
		
        if (conn != null)
        {
            try
            {
                conn.close ();
                System.out.println ("Database connection terminated");
            }
            catch (Exception e) { /* ignore close errors */ }
        }

	}

	
	public static Connection connectPool(Jdbc3PoolingDataSource source) {
		
		Connection con = null;
		try {
		    con = source.getConnection();
		    System.out.println ("Database connection established");
		} catch(SQLException e) {
        	e.printStackTrace();
        	System.err.println ("Cannot connect to database server");
		   
		}
		
		return con;
	}

	public static void main(String[] args) {
		
		Jdbc3PoolingDataSource source = ConnectionManager.getSource();
		
		Connection conn = ConnectionManager.connectPool(source);
		
		ConnectionManager.close(conn);
		
	}
	
}

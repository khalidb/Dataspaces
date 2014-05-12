/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.mappinggeneration;

import java.sql.Connection;
import java.util.Vector;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.Column;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.Table;

/**
 * @author Khalid Belhajjame
 *
 */
public class GenerateMappingsMySQL implements GenerateMappings {

	Vector mappings = new Vector();
    Connection sources_con = null;
    Connection dataspaces_repository_con = null;
    String url = "jdbc:mysql://localhost:3306/";
    String sources_schema = "information_schema";
    String sources_db = "mondial";
    String integration_db = "integration";
    String dataspaces_repository = "dataspaces_repository";
    String driver = "com.mysql.jdbc.Driver";
    String user = "dataspaces";
    String pass = "adana";
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		/*
		//mappings generated using Clio
		Vector mappings = new Vector();
		mappings.add("SELECT c.Name, c.Country FROM mondial.city c");
		mappings.add("SELECT S0.Capital,S0.Name FROM  mondial.country S0");
		mappings.add("SELECT S0.Name,S0.Country FROM  mondial.ethnic_group S0");
		mappings.add("SELECT S0.City,S0.Country FROM  mondial.located S0");
		mappings.add("SELECT S0.City,S0.Country FROM  mondial.organization S0");
		mappings.add("SELECT S0.Capital,S0.Country FROM  mondial.province S0");
		*/
		
		GenerateMappingsMySQL gm = new GenerateMappingsMySQL();
		//gm.generateMappings();
		gm.generatePaths("mondial","city", "Country","Country");
		gm.generatePaths("mondial","city", "Name","City");
	}
	
	/*
	 * This method returns the mappings generated
	 */
	
	public Vector getMappings() {
		return this.mappings;
	}
	
	/*
	 * This method display the mappings generated
	 */
	private void displayMapping(Vector mappings){
		
		for (int i=0;i<mappings.size();i++) {
			SchemaMapping map = (SchemaMapping) mappings.get(i);
			System.out.println("_____________\nmap "+i+"\nIntegration query: "+map.getIntegrationRelation()+"\nSource query: "+map.getS_query());
		}
		
	}

	
	/*
	 * This method is used to save the mappings generated in the dataspaces repository
	 */
	private void saveMappings(Vector mappings) {
		
		String query = null;
		java.sql.PreparedStatement st = null;
		
		if (dataspaces_repository_con == null) 
			connect();
		
		for (int i=0;i<mappings.size();i++) {
			try{
				SchemaMapping map = (SchemaMapping) mappings.get(i);
				query = "insert into SchemaMappings values (?,?,?)";
				st = dataspaces_repository_con.prepareStatement(query);
				st.setString(1,"map_"+map.getIntegrationRelation()+"_"+i);
				st.setString(2, map.getIntegrationRelation());
				st.setString(3, map.getS_query());
				st.executeUpdate();
			} catch (SQLException s){
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
	}

	public void insertMapping(int id, String integration_query, String source_query) {

		java.sql.PreparedStatement st = null;
		String query = null;
		
		if (dataspaces_repository_con == null) 
			connect();
		
		try{
			query = "insert into SchemaMappings values (?,?,?)";
			st = dataspaces_repository_con.prepareStatement(query);
			st.setInt(1,id);
			st.setString(2, integration_query);
			st.setString(3, source_query);

			st.executeUpdate();
		} catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}
	}
		
	
	
	/*
	 * This method is used to remove existing mappings from the dataspaces repository
	 */
	private void removeExistingMappings() {
		
		String query = null;
		java.sql.PreparedStatement st = null;
		
		if (dataspaces_repository_con == null) {	
			try {
				Class.forName(driver);
				dataspaces_repository_con = DriverManager.getConnection(url+dataspaces_repository, user, pass);
			} catch (SQLException e) {
				System.err.println("Failed to create a mysql connection to "+dataspaces_repository);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.err.println("Cannot find the MYSQL driver class");
				e.printStackTrace(); 
			}
		}
		
		try{
			query = "delete from SchemaMappings";
			st = dataspaces_repository_con.prepareStatement(query);
			st.executeUpdate();
		} catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}
	}

	
	@Override
	public Vector generateMappingsRandomly() {
		
		mappings.removeAllElements();
		String s_query = null;
		Vector i_tables = this.getIntegrationTables();
		
		for (int i =0; i<i_tables.size();i++) {
			Table i_tab = (Table) i_tables.get(i); 
			if (sources_con == null) {	
				try {
					Class.forName(driver);
					sources_con = DriverManager.getConnection(url+sources_schema, user, pass);
				} catch (SQLException e) {
					System.err.println("Failed to create a mysql connection to "+sources_schema);
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					System.err.println("Cannot find the MYSQL driver class");
					e.printStackTrace(); 
				}
			}
		
			java.sql.PreparedStatement st_1 = null;
			java.sql.PreparedStatement st_2 = null;
			String query_1 = null;
			String query_2 = null;
	
			try{
				query_1 = "SELECT table_schema, table_name FROM information_schema.TABLES T where table_schema = ?;";
				st_1 = sources_con.prepareStatement(query_1);
				st_1.setString(1, sources_db);
				ResultSet rs_1 = st_1.executeQuery();
				int id = 0;
				while (rs_1.next()){
					Table s_tab = new Table();
					s_tab.schema = rs_1.getString(1);
					s_tab.name = rs_1.getString(2);

					query_2 = "SELECT column_name, data_type, is_nullable FROM information_schema.COLUMNS C where (table_schema = ?) and (table_name = ?)";
					st_2 = sources_con.prepareStatement(query_2);
					st_2.setString(1, s_tab.getSchema());
					st_2.setString(2, s_tab.getName());
					ResultSet rs_2 = st_2.executeQuery();
				
					while (rs_2.next()){
						Column col = new Column();
						col.name = rs_2.getString(1);
						col.data_type = rs_2.getString(2);
						col.is_nullable = rs_2.getBoolean(3);
				
						s_tab.addColumn(col);
					}
				
					SchemaMapping map = new SchemaMapping();
					map.setIntegrationRelation(i_tab.schema+"."+i_tab.name);
					s_query = "SELECT ";
				
					for (int j=0;j<i_tab.columns.size();j++) {
						if (s_tab.columns.size() > j)
							s_query += ((Column) s_tab.getColumns().get(j)).name;
						else
							s_query += "null";
						if (j != i_tab.columns.size() - 1)
							s_query += ", ";
					}
				
					s_query += " From "+s_tab.schema+"."+s_tab.name;
					map.setS_query(s_query);
					map.setId(id++);
					mappings.add(map);
				}
			}catch (SQLException s){
				System.err.println("Error while trying to execute the following query: "+query_1);
				s.printStackTrace();
			}
			
		}
		
		this.displayMapping(mappings);
		this.removeExistingMappings();
		this.saveMappings(mappings);
		return mappings;
	}
	
	private Vector getIntegrationTables() {
		
		Vector i_tables = new Vector();
		if (sources_con == null) {	
			try {
				Class.forName(driver);
				sources_con = DriverManager.getConnection(url+sources_schema, user, pass);
			} catch (SQLException e) {
				System.err.println("Failed to create a mysql connection to "+sources_schema);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.err.println("Cannot find the MYSQL driver class");
				e.printStackTrace(); 
			}
		}
		
		java.sql.PreparedStatement st_1 = null;
		java.sql.PreparedStatement st_2 = null;
		String query_1 = null;
		String query_2 = null;
		
		try{
			query_1 = "SELECT table_schema, table_name FROM information_schema.TABLES T where table_schema = ?;";
			st_1 = sources_con.prepareStatement(query_1);
			st_1.setString(1, integration_db);
			ResultSet rs_1 = st_1.executeQuery();
			while (rs_1.next()){
				Table i_tab = new Table();
				i_tab.schema = rs_1.getString(1);
				i_tab.name = rs_1.getString(2);
	
				query_2 = "SELECT column_name, data_type, is_nullable FROM information_schema.COLUMNS C where (table_schema = ?) and (table_name = ?)";
				st_2 = sources_con.prepareStatement(query_2);
				st_2.setString(1, i_tab.getSchema());
				st_2.setString(2, i_tab.getName());
				ResultSet rs_2 = st_2.executeQuery();
				while (rs_2.next()){
				
					Column col = new Column();
					col.name = rs_2.getString(1);
					col.data_type = rs_2.getString(2);
					col.is_nullable = rs_2.getBoolean(3);
					
					i_tab.addColumn(col);
				}
				
				i_tables.add(i_tab);
			}
				 
		}catch (SQLException s){
				System.err.println("Error while trying to execute the following query: "+query_1);
				s.printStackTrace();
		}
		
		return i_tables;
	}

	public void mutateMapping(String integration_query, String source_query, String attribute, String value) {

		String query = null;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		
		if (dataspaces_repository_con == null)
			connect();

		try{

			query = "insert into schemamappings values(null,\""+integration_query+"\",\""+source_query + " where ("+attribute+" = '"+value+"')\")";
			st = dataspaces_repository_con.prepareStatement(query);

			st.executeUpdate();
			
		} catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}

		
	}

	public void mutateMapping(String integration_query, String source_query, String[] attributes, String[] values) {

		String query = null;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		
		for(int i =0;i<attributes.length;i++){
			if (i==0)
				source_query = source_query + " where ";
			else
				source_query = source_query +" and ";
			source_query = source_query +"("+attributes[i]+"='"+values[i]+"')";
		}
		
		if (dataspaces_repository_con == null)
			connect();

		try{

			query = "insert into schemamappings values(null,\""+integration_query+"\",\""+source_query + "\")";
			st = dataspaces_repository_con.prepareStatement(query);

			st.executeUpdate();
			
		} catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}

		
	}

	
	public void connect(){

		try {
			Class.forName(driver);
			dataspaces_repository_con = DriverManager.getConnection(url+dataspaces_repository, user, pass);
		} catch (SQLException e) {
			System.err.println("Failed to create a mysql connection to "+dataspaces_repository);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("Cannot find the MYSQL driver class");
			e.printStackTrace(); 
		}

	}
	
	@Override
	public void generateMappings(String integration_query, Vector mappings, int num_rows) {

		String source_query = null;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		String[] attributes;
		String[] values;
		int n;

		
		if (dataspaces_repository_con == null)
			connect();
		
		for (int i=0;i<mappings.size();i++) {
			try{
				source_query = mappings.get(i).toString();
				st = dataspaces_repository_con.prepareStatement(source_query+" order by rand() limit "+num_rows);
				n = st.getMetaData().getColumnCount();
				attributes = new String[n];
				for(int j=0;j<n;j++)
					attributes[j] = st.getMetaData().getColumnName(j+1);
				
				rs = st.executeQuery();
				
				while (rs.next()) {
					values = new String[attributes.length];
					for (int j=0;j<attributes.length;j++) {
						if (rs.getObject(j+1) != null) {
							values[j] = rs.getObject(j+1).toString();
							this.mutateMapping(integration_query, source_query, attributes[j], values[j]);
						}
						this.mutateMapping(integration_query, source_query, attributes, values);
					}
					
				}
				
				
			} catch (SQLException s){
				System.err.println("Error while trying to execute the following query: "+source_query);
				s.printStackTrace();
			}
		}

		
	}
	
	public void generatePaths(String schema_name,String s_table_name,String s_column_name, String column_name) {
		String table_name = null;
		int path_id=-1, position;
		if (dataspaces_repository_con == null)
			connect();

		java.sql.PreparedStatement st_1 = null, st_2 = null, st_3 = null;
		ResultSet rs_1 =null, rs_2 =null, rs_3 = null;
		String query_1 = "SELECT table_name FROM information_schema.`COLUMNS` C where (c.table_schema = ?) and (c.column_name = ?)";	
		String query_2 = "insert into path values(null)";
		String query_3 = "insert into path_node values(?,?,?,?)";
		
		try {

			st_1 = dataspaces_repository_con.prepareStatement(query_1);
			st_2 = dataspaces_repository_con.prepareStatement(query_2);
			st_3 = dataspaces_repository_con.prepareStatement(query_3);
			
			st_1.setString(1, schema_name);
			st_1.setString(2, column_name);
			rs_1 = st_1.executeQuery();
		
			while (rs_1.next()) {
				table_name = rs_1.getString(1);
				
				st_2.executeUpdate();
				rs_2 = st_2.getGeneratedKeys();
				if ( rs_2.next() ) {
				    // Retrieve the auto generated key(s).
				    path_id = rs_2.getInt(1);
				}
				position = 1;
				st_3.setString(1, s_column_name);
				st_3.setString(2, schema_name+"."+s_table_name);
				st_3.setInt(3, position++);
				st_3.setInt(4, path_id);
				
				st_3.executeUpdate();
				
				st_3.setString(1, column_name);
				st_3.setString(2, schema_name+"."+table_name);
				st_3.setInt(3, position);
				st_3.setInt(4, path_id);
				
				st_3.executeUpdate();
				
			}

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query_1);
			s.printStackTrace();	
		}
	

	}
	@Override
	public void generateMappings() {

		int map_id = 1;
		int num = 2;
		
		String integration_query = "integration.europeancities";
		String[] source_queries = new String[14];
		
		source_queries[0] = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'F') or (c.Country = 'Ma') or (c.Country = 'I') order by rand() limit 230";
		source_queries[1]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'F') or (c.Country = 'Ma') or (c.Country = 'I') order by rand() limit 100";
		source_queries[2]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'Ma') or (c.Country = 'I') order by rand() limit 200";
		source_queries[3]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'F')or (c.Country = 'I') order by rand() limit 220";
		source_queries[4]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'F') or (c.Country = 'Ma') order by rand() limit 205";
		source_queries[5]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'Ma') or (c.Country = 'I') order by rand() limit 60";
		source_queries[6]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'I') order by rand() limit 200";
		source_queries[7]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'F') order by rand() limit 200";
		source_queries[8]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'F') or (c.Country = 'Ma') order by rand() limit 40";
		source_queries[9]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'Ma') order by rand() limit 140";
		source_queries[10]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') order by rand() limit 100";
		source_queries[11]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'F') order by rand() limit 20";
		source_queries[12]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'Ma') order by rand() limit 10";
		source_queries[13]  = "select c.Name, c.Country from mondial.city c where (c.Country = 'I') order by rand() limit 30";

		for (int i=0;i<source_queries.length;i++) {
			this.insertMapping(map_id++, integration_query,source_queries[i]);
			this.insertMapping(map_id++, integration_query,source_queries[i]);
		}
		
		String query_15 = "Select l.city, l.country from mondial.located l where l.Sea = 'Mediterranean Sea'";
		this.insertMapping(map_id++, integration_query,query_15);
		
		String query_16 = "SELECT c.Name, c.country FROM mondial.City c, mondial.language l where (c.Country = l.Country)  and (l.Name = 'English')";
		this.insertMapping(map_id++, integration_query,query_16);

		String query_17 = "SELECT c.Name, c.Country FROM mondial.City c, mondial.religion r where (c.Country = r.Country) and (r.Name = 'Anglican')";
		this.insertMapping(map_id++, integration_query,query_17);
		
	}

}

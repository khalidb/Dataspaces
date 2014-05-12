/**
 * 
 */
package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author Khalid Belhajjame
 *
 */
public class SpecifyCorrectResults {

    Connection dataspaces_repository_con = null;
    String url = "jdbc:mysql://localhost:3306/";
    String dataspaces_repository_db = "dataspaces_repository";
    String driver = "com.mysql.jdbc.Driver";
    String user = "dataspaces";
    String pass = "adana";

	
	public void fillCorrectResults(String integration_query, String source_query, String[] attributes){
		
		String integration_table = integration_query.replace("integration.", "integration_");
		
		if (dataspaces_repository_con == null) {	
			try {
				Class.forName(driver);
				dataspaces_repository_con = DriverManager.getConnection(url+dataspaces_repository_db, user, pass);
			} catch (SQLException e) {
				System.err.println("Failed to create a mysql connection to "+dataspaces_repository_db);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.err.println("Cannot find the MYSQL driver class");
				e.printStackTrace(); 
			}
		}
		
		int max_id = this.getMaxId(integration_table);
		System.out.println("max = "+max_id);
		
		java.sql.PreparedStatement st = null;
		java.sql.PreparedStatement st_1 = null;
		java.sql.PreparedStatement st_2 = null;
		ResultSet rs_1 = null;
		String query_1 = "insert into correctresults_"+integration_table+" values (?";
		for (int i=0;i<attributes.length;i++)
			query_1 = query_1+",?";
		query_1 = query_1+")";
		
		try {

			st = dataspaces_repository_con.prepareStatement(source_query);
			ResultSet rs = st.executeQuery();
			String[] values = new String[attributes.length];
			
			String query = null; 
			
			while (rs.next()) {
			
				query = "select id";
				for (int i =0; i<attributes.length; i++) { 
					values[i] = rs.getString(i+1).toString();
					System.out.println(" value of "+attributes[i]+": "+values[i]);
				}
				
			
				query = query + " from "+integration_table+ " where ";
				
				for (int i =0; i<attributes.length; i++) {
					query = query + " ("+attributes[i]+" = ?)" ;
					if (i != attributes.length - 1)
						query = query + " and ";
				}
				

				st_1 = dataspaces_repository_con.prepareStatement(query);
				
				for (int i =0; i<values.length; i++) 
					st_1.setString(i+1,values[i]);
				

				rs_1 = st_1.executeQuery();
			
				st_2 = dataspaces_repository_con.prepareStatement(query_1);;
				if (rs_1.next()) 
					st_2.setInt(1,rs_1.getInt(1));
				else
					st_2.setInt(1,++max_id);
				
				for (int i=0;i<values.length;i++)
					st_2.setObject(i+2,values[i]);
				
				st_2.executeUpdate();

			}
		}catch (SQLException s){
			s.printStackTrace();	
		}
		
	}
	
	private int getMaxId(String integration_table) {
		
		int max = -1;
		
		if (dataspaces_repository_con == null) {	
			try {
				Class.forName(driver);
				dataspaces_repository_con = DriverManager.getConnection(url+dataspaces_repository_db, user, pass);
			} catch (SQLException e) {
				System.err.println("Failed to create a mysql connection to "+dataspaces_repository_db);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				System.err.println("Cannot find the MYSQL driver class");
				e.printStackTrace(); 
			}
		}
		
		java.sql.PreparedStatement st = null;
		try {
			
			st = dataspaces_repository_con.prepareStatement("select max(id) from "+integration_table);
			ResultSet rs = st.executeQuery();
			
			
			
			if (rs.next()) {
				max = rs.getInt(1);
			}

		}catch (SQLException s){
			s.printStackTrace();	
		}

		
		return max;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SpecifyCorrectResults scr = new SpecifyCorrectResults();		
		String integration_query = "integration.europeancities";
		//String source_query = "SELECT c.Name, c.Country FROM mondial.city c where (c.Country = 'GB') or (c.Country = 'F') or (c.Country = 'I')";
		//String source_query = "select * from mondial.city c where (c.country = 'F') or (c.country = 'I') or (c.country = 'GB') or (c.country = 'S') or (c.country = 'P') or (c.country = 'D') or (c.country = 'G')";
		//String source_query = "select c.Name, c.Country from mondial.city c";
		//String source_query = "select c.Name, c.Country from mondial.city c where (c.Name not like '%e%') and (c.Name not like '%b%')";
		//String source_query = "SELECT c.Name, c.Country FROM mondial.city c where (c.Country = 'GB') or (c.Country = 'F') or (c.Country = 'I') order by rand() limit 200";
		//Good quality mappings
		//String source_query = "select c.Name, c.Country from mondial.city c where (c.Country = 'GB') or (c.Country = 'F') or (c.Country = 'Ma') or (c.Country = 'I') order by rand() limit 250";
		//String source_query = "select c.Name, c.Country from mondial.city c order by rand() limit 100";
		// poor quality mappings experiment: 
		//String source_query = "(Select l.city, l.country from mondial.located l where l.Sea = 'Mediterranean Sea') union (SELECT c.Name, c.country FROM mondial.City c, mondial.language l where (c.Country = l.Country)  and (l.Name = 'Portuguese'))";

		
		// For amalgam
		//String source_query = "(select a.title, a.journal as book from amalgam.s1_article a limit 50) union (select i.title, i.org as book from amalgam.s1_manual i limit 50) union (select i.title, i.inst as book from amalgam.s1_techreport i limit 50) union (SELECT t.title, j.jrnlNm as book FROM amalgam.s2_titles t, amalgam.s2_citjournal ji, amalgam.s2_journal j where (t.citKey= ji.citKey) and (ji.jrnlID = j.jrnlID) limit 100) union (select i.title, i.bktitle as book from amalgam.s1_incollection i limit 50)";
		
		// NEW Set of mappings
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c, mondial.mycity m where ((c.Country = 'GB') or (c.Country = 'USA') or (c.Country = 'D')) and (c.Population > 50000) and (c.Name = m.Name)";
		
		// Correct Mapping 1 for refinement
		// ((m1 union m2) intersection m14) join m37
		// That is: 
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c, mondial.located l where ((c.Country = 'GB') or (c.Country = 'D') or (c.Country = 'USA')) and (c.Name = l.city)";
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c where ((c.Country = 'USA') or (c.Country = 'DK') or (c.Country = 'CH') or (c.Country = 'F') or (c.Country = 'I') or (c.Country = 'MA') or (c.Country = 'BR') or (c.Country = 'MEX') or (c.Country = 'IND')) and (((c.Population > 50000) and (c.Country = 'USA')) or (c.Name like 'a%'))";
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c where ((c.Country = 'GB') or (c.Country = 'USA') or (c.Country = 'D')) and (c.Population > 50000)";
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c where ((c.Country = 'GB') or (c.Country = 'USA') or (c.Country = 'D')) and (c.Population > 100000)";
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c, mondial.located l where ((c.Country = 'GB') or (c.Country = 'USA')) and (c.Name = l.city)
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c, mondial.located l where ((c.Country = 'GB') or (c.Country = 'USA')) and (c.Name = l.city)
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c, mondial.located l where ((c.Country = 'GB') or (c.Country = 'USA')) and (c.Name = l.city)";
		String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c, mondial.located l where ((c.Country = 'GB') or (c.Country = 'D') or (c.Country = 'USA'))";
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c where ((c.Country = 'DK') or (c.Country = 'CH') or (c.Country = 'F') or (c.Country = 'I') or (c.Country = 'MA') or (c.Country = 'BR') or (c.Country = 'MEX') or (c.Country = 'IND')) and (((c.Population > 50000)))";
		//String source_query = "SELECT distinct c.Name, c.Country FROM mondial.city c where ((c.Country = 'DK') or (c.Country = 'CH') or (c.Country = 'F') or (c.Country = 'I') or (c.Country = 'MA') or (c.Country = 'BR') or (c.Country = 'MEX') or (c.Country = 'IND')) and (((c.Population > 50000)))";
		String[] attributes = {"city_name","country"};
		scr.fillCorrectResults(integration_query, source_query, attributes);
	}

}

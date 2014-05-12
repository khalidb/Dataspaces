package uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.ConnectionManager;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrieval;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrievalPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ForeignKey;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;

public class RefineMappingsPostgres implements RefineMappings {

	
	PruneMappings pm = null;
	int number_of_iterations;
	int top_k, selected_mappings = 20, nb_new_generation,mutation_loop=0;
	int num_of_selected_mappings = 20;
	Random random = new Random();
    Jdbc3PoolingDataSource source = null;
    Connection conn = null;
    
    private SchemaMappingRetrieval smr = null;
	
	public RefineMappingsPostgres(Jdbc3PoolingDataSource _source){
		super();
		this.source = _source;
		this.pm = new PruneMappingsPostgres(this.source);
		this.top_k = 5;
		this.smr = new SchemaMappingRetrievalPostgres(this.source);
	}
	
	@Override
	public void refineMappings(String integration_query) {
		
		Vector candidate_mappings = this.pm.getMappings(integration_query);
		//this.top_k = candidate_mappings.size();
		this.nb_new_generation = candidate_mappings.size();
		Vector topK_mappings;
		Vector candidate_mappings_1, candidate_mappings_2;
		
		this.initialiseTerminationCondition();
		int i=1;
		
		while (!termination_condition_met()) {
			
			System.out.println("Refining mappings for the "+i+++" time");
			topK_mappings = this.pm.selectTopKMappings(integration_query,this.top_k);
			candidate_mappings_1 = this.applyVariationOperators(integration_query,topK_mappings);
			
			System.out.print("The mappings pruned are: ");
			for (int k=0;k<candidate_mappings_1.size();k++)
				System.out.print(" "+(Integer)candidate_mappings_1.get(k)+" ");
			System.out.println();
			
			//candidate_mappings_2 = this.mutateMappings(integration_query,topK_mappings);
			//this.pm.pruneMappings(integration_query,this.add(candidate_mappings_1,candidate_mappings_2));
			this.pm.pruneMappings(integration_query,candidate_mappings_1);
			this.selectMappings(integration_query,candidate_mappings_1,top_k);
			//this.selectMappings(integration_query);
			
		}
		
	}
	
	public Vector selectTopMappings(Vector mappings, int topk){
		
		Vector selected_mappings = new Vector();
		
		java.sql.Statement st_1 = null;
		java.sql.PreparedStatement st_2 = null;
		ResultSet rs1 = null, rs2 = null;
		String query_1 = "select s.id from schemamappings s, mapping_cardinal_annotation m where (s.id = m.mapping_id) ";
		String query_2 = "select m1.mapping_id from mapping_cardinal_annotation m1, mapping_cardinal_annotation m2 where (m2.mapping_id = ?) and (m1.mapping_id != m2.mapping_id) and (m1.precision = m2.precision) and (m1.recall = m2.recall)";
		
		if (mappings.size() != 0) {
			query_1 = query_1 + " and (";
			for (int i=0;i<mappings.size();i++) {
				if (i != 0)
					query_1 = query_1 + " or ";
				query_1 = query_1 + "(s.id = "+(Integer) mappings.get(i)+")";
			}
			query_1 = query_1 +") order by m.f_measure DESC";
		}

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection(); 

			st_1 = conn.createStatement();
			st_2 = conn.prepareStatement(query_2);
			
			st_1.setMaxRows(topk);
			rs1 = st_1.executeQuery(query_1);
			
			while (rs1.next())
				selected_mappings.add(rs1.getInt(1));
			
			
			for (int i=selected_mappings.size()-1;i>=0;i--) {
				st_2.setInt(1, (Integer) selected_mappings.get(i));
				rs2 = st_2.executeQuery();
				if (rs2.next())
					selected_mappings.remove(i);
			}
			
			st_1.close();
			st_2.close();
			rs1.close();
			rs2.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query_1);
			s.printStackTrace();	
		}
		
		return selected_mappings;
	}
	
	public void selectMappings(String integration_query, Vector selected_mappings, int topk)  {
		
		// remove the mappings that are not in the top from the mappings
		// in candidate_mappings
		
		String integration_table = integration_query.replace("integration.", "integration_");

		
			Vector top_mappings = this.selectTopMappings(selected_mappings,topk);
			
			for (int i=0;i<top_mappings.size();i++) {
				int m = (Integer) top_mappings.get(i);
				
				for (int j=selected_mappings.size()-1;j>=0;j--) 
					if (((Integer) selected_mappings.get(j)) == m)
						selected_mappings.remove(j);
			}
			
		if (selected_mappings.size() != 0) {
			
			java.sql.Statement st_1 = null;
			java.sql.Statement st_2 = null;
			java.sql.Statement st_3 = null;

			String query_1 = "delete from schemamappings where ";
			String query_2 = "delete from mapping_cardinal_annotation where mapping_id not in (select id from schemamappings)";
			String query_3 = "delete from mapping_"+integration_table+" where mapping_id not in (select m.id from schemamappings m)";
			
			
			for (int i=0;i<selected_mappings.size();i++) {
				if (i != 0)
					query_1 = query_1 + " or ";
				query_1 = query_1 + "(id = "+(Integer) selected_mappings.get(i)+")";
			}
			
			System.out.print("The mappings deleted by refinement are: ");
			for (int i=0;i<selected_mappings.size();i++) {
				System.out.print(" "+(Integer) selected_mappings.get(i)+" ");
			}

			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();

				st_1 = conn.createStatement();
				st_1.executeUpdate(query_1);

				st_2 = conn.createStatement();
				st_2.executeUpdate(query_2);

				st_3 = conn.createStatement();
				st_3.executeUpdate(query_3);
				
				st_1.close();
				st_2.close();
				st_3.close();
				conn.close();

			}catch (SQLException s){
				System.err.println("Error while trying to execute the following query: "+query_1);
				s.printStackTrace();	
			}

			
		}

		
	}

	/*
	 *  This method is hard coded for the moment
	 *  In the sense that it considers only one specific data source
	 */
	/*
	public int createJoinMapping(String integration_query,int map) {
		
		int join_map = -1;
		
		//String table_1 = this.getRandomTableByColumn("mondial","city");
		String table_2 = this.getRandomTableByColumn("mondial","country");
		if (table_2 !=  null) {
			String t_1 = this.generateTableName();
			String t_2 = this.generateTableName();
			String s_query = "select "+t_1+".* from ("+this.getSourceQuery(map)+") as "+t_1+" inner join mondial."+table_2+" as "+t_2+"  on ("+t_1+".country = "+t_2+".country)";
			String query = "insert into schemamappings values (null,?,?)";
			if (dataspaces_repository_con == null)
				connect();
			java.sql.PreparedStatement st = null;
			
			try{
				st = dataspaces_repository_con.prepareStatement(query);
				st.setString(1, integration_query);
				st.setString(2, s_query);
				
				st.executeUpdate();
				
				ResultSet rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    join_map = rs.getInt(1);
				}
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
			
		}
		
	} */
	

	public int createJoinMapping(String integration_query,int map) {
		
		int join_map = -1;
		
		// 1- get randomly the name of a table that is involved in map, 'tab'
		
		String map_source = this.getSourceQuery(map);
		String table_name = this.getTableFromQuery(map_source);
		// 2- get randomly 'p', a path that goes through an attribute in that table
		if (table_name != null) {
			int path_id = this.getPath(table_name);
		
			System.out.println("Table name: "+table_name+", path id: "+path_id);
		// 3- construct the source query obtained by joining the source query of map 
		// with the relations in the paths 'p'
		
			if (path_id != -1) {

				String s_query = this.constructJoinQuery(map_source, path_id, table_name);
				System.out.println("Joining the source query of the mapping: "+map+"\n using the path: "+path_id);
				join_map = this.createSchemaMapping(integration_query,s_query);
			}
		}
		return join_map;
		
	}

	private int createSchemaMapping(String integration_query, String source_query) {
		
		int map_id = -1;
		String query = "insert into schemamappings values (DEFAULT,?,?)"+ " RETURNING id";
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setString(1, integration_query);
			st.setString(2, source_query);
			
			/*
			st.executeUpdate();
			rs = st.getGeneratedKeys();
			if ( rs.next() ) {
			    // Retrieve the auto generated key(s).
			    map_id = rs.getInt(1);
			}
			*/
			
			// We cannot use the JDBC operation getGeneratedKey as listed in the above commented 
			// code. Because the current version of postgres does not support it. 
			rs = st.executeQuery();
			if (rs.next())
				map_id = rs.getInt(1); 
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}

		return map_id;
	}
	
	private String getTableFromQuery(String query) {
		String table_name = null;
		
		Vector table_names = new Vector();
		
		Pattern MY_PATTERN = Pattern.compile("((from|FROM)\\s[a-zA-Z1-9_.]+)");

		Matcher m = MY_PATTERN.matcher(query);
		System.out.println("source query: "+query);
		while (m.find()) {
		    String s = m.group(1);
		    table_names.add(s);
		    //System.out.println(s);
		}
		
		if (table_names.size() > 0) {
			table_name = table_names.get(random.nextInt(table_names.size())).toString().replaceAll("[fF][rR][oO][mM]", "").trim();
			//System.out.println("Selected table name: "+table_name);
		}
		
		
		return table_name;
	}
	
	private String constructJoinQuery(String source_query,int path_id, String table_name) {
		
		String s_query = null, tab_name, att_name, select_clause, from_clause = null,where_clause = null;
		String query = "select n.table_name, n.attribute_name from path_node n where (n.path_id = ?) order by n.position";
		String t = this.generateTableName();
		select_clause = "select distinct "+t+".* ";
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		String tab_prev =null, tab_curr, t_curr=null,t_prev = null,att_curr,att_prev = "";

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();

			st = conn.prepareStatement(query);
			st.setInt(1,path_id);
			rs = st.executeQuery();
			
			while (rs.next()) {
				tab_curr = rs.getString(1);
				att_curr = rs.getString(2);
				
				if (tab_curr.equals(table_name)) {
					t_curr = t;
					tab_curr = "("+source_query+")";
				}
				if (tab_curr.equals(tab_prev)) {
					t_curr = t_prev;
				}
				if (t_curr == null)
					t_curr = this.generateTableName();

				if (tab_prev == null) {
					from_clause = tab_curr+" "+t_curr+",";
					where_clause = "";
				}
				if ((tab_prev != null)&&(!tab_prev.equals(tab_curr))) {
					//from_clause = "("+from_clause+" "+tab_curr+" "+t_curr+" where ("+t_prev+"."+att_prev+" = "+t_curr+"."+att_curr+"))";
					from_clause = from_clause+" "+tab_curr+" "+t_curr+",";
					if (!where_clause.equals(""))
						where_clause = where_clause+" and ";
					where_clause = where_clause +" ("+t_prev+"."+att_prev+" = "+t_curr+"."+att_curr+") ";
				}
				tab_prev = tab_curr;
				att_prev = att_curr;
				t_prev = t_curr;
				t_curr = null;
				tab_curr = null;
			}
			
			if (from_clause.endsWith(","))
				from_clause = from_clause.substring(0, from_clause.length()-1);
			s_query = select_clause + " from " + from_clause+ " where " + where_clause;
			
			st.close();
			rs.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return s_query;
		
	}
	
	private int getPath(String table_name) {
		
		int path_id = -1;
		String query = "select p.id from path p where ('"+table_name+"' in (select n.table_name from path_node n where n.path_id = p.id)) order by rand() limit 1";
		java.sql.Statement st = null;
		ResultSet rs = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			if (rs.next())
				path_id = rs.getInt(1);
			
			st.close();
			rs.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return path_id;
	}

	
	public String getRandomTableByColumn(String schema_name, String column_name) {
		
		String table_name = null;
		java.sql.PreparedStatement st_1 = null;
		ResultSet rs = null;
		String query_1 = "SELECT table_name FROM information_schema.`COLUMNS` C where (c.table_schema = ?) and (c.column_name = ?) Order by Rand() limit 1 ";	
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st_1 = conn.prepareStatement(query_1);
			st_1.setString(1, schema_name);
			st_1.setString(2, column_name);
			rs = st_1.executeQuery();
			
			if (rs.next())
				table_name = rs.getString(1);
			
			st_1.close();
			rs.close();
			conn.close();

		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query_1);
			s.printStackTrace();	
		}
		
		return table_name;
		
	}
	
/*	
	private ForeignKey GetForeignKey(int map) {

		
		return null;
	}
*/
	public Vector mutateMappings(String integration_query,
			Vector topK_mappings) {
		Vector mutated_mappings = new Vector();
		int map; 
		
		for (int k=0;k< mutation_loop;k++) {
			for (int i =0; i< topK_mappings.size();i++) {
				map = (Integer) topK_mappings.get(i);
				int map_j = this.createJoinMapping(integration_query,map);
				System.out.println("JOIN MAPPING: "+map_j);
				if (map_j != -1) {
					this.executeMapping(map_j, integration_query);
					mutated_mappings.add(map_j);
					//System.out.println("Join mapping: "+map_j);
				}
			}
		}
		
		return mutated_mappings;
	}

	
	public Vector recombineMappings(String integration_query,Vector topK_mappings) {
		
		Vector candidate_mappings_1 = new Vector();
		
		//System.out.println("Top k mappings ");
		//for (int i=0;i<topK_mappings.size();i++)
			//System.out.println((Integer) topK_mappings.get(i));
		
		for (int i =0; i< topK_mappings.size();i++) {
			int map = (Integer) topK_mappings.get(i);
			//candidate_mappings_1.add(map);
			System.out.println("- mapping: "+map);
			
			int map_intersect =  this.getNeighbour(map,"intersection", integration_query);
			if (map_intersect != -1) {
				int m1 = this.createMapping(map,map_intersect,"intersection", integration_query);
				if (m1 != -1) {
					candidate_mappings_1.add(m1);
					this.executeMapping(m1, integration_query);
					System.out.println("Intersection neighbour is: "+map_intersect);
				}
			}
			
			int map_union =  this.getNeighbour(map,"union", integration_query);
			if (map_union != -1) {
				int m2 = this.createMapping(map,map_union,"union", integration_query);
				if (m2 != -1) {
					candidate_mappings_1.add(m2);
					this.executeMapping(m2, integration_query);
					System.out.println("Union neighbour is: "+map_union);
				}
			}
	
			
			int map_diff =  this.getNeighbour(map,"difference", integration_query);
			if (map_diff != -1) {
				int m3 =this.createMapping(map,map_diff,"difference", integration_query);
				if (m3 != -1) {
					candidate_mappings_1.add(m3);
					this.executeMapping(m3, integration_query);
					System.out.println("Difference neighbour is: "+map_diff);
				}
			}
			
			
		}
		
		return candidate_mappings_1;
	}

	public Vector recombineMappings_v0(String integration_query,Vector topK_mappings) {
		
		Vector candidate_mappings_1 = new Vector();
		
		System.out.println("Top k mappings ");
		for (int i=0;i<topK_mappings.size();i++)
			System.out.println((Integer) topK_mappings.get(i));
		
		for (int i =0; i< topK_mappings.size();i++) {
			int map = (Integer) topK_mappings.get(i);
			//candidate_mappings_1.add(map);
			
			Vector maps_intersect =  this.getNeighbours(map,"intersection", integration_query);
			for (int j=0;j<maps_intersect.size();i++) {
				int m1 = this.createMapping(map,(Integer) maps_intersect.get(j),"intersection", integration_query);
				if (m1 != -1) {
					candidate_mappings_1.add(m1);
					this.executeMapping(m1, integration_query);
					//System.out.println("Intersection neighbour is: "+(Integer) maps_intersect.get(j));
				}
			}
			
			Vector maps_union =  this.getNeighbours(map,"union", integration_query);
			for (int j=0;j<maps_union.size();i++) {
				int m2 = this.createMapping(map,(Integer) maps_union.get(j),"union", integration_query);
				if (m2 != -1) {
					candidate_mappings_1.add(m2);
					this.executeMapping(m2, integration_query);
					//System.out.println("Union neighbour is: "+(Integer) maps_union.get(j));
				}
			}
	
			
			Vector maps_diff =  this.getNeighbours(map,"difference", integration_query);
			for (int j=0;j<maps_diff.size();i++) {
				int m3 =this.createMapping(map,(Integer) maps_diff.get(j),"difference", integration_query);
				if (m3 != -1) {
					candidate_mappings_1.add(m3);
					this.executeMapping(m3, integration_query);
					//System.out.println("Difference neighbour is: "+(Integer) maps_diff.get(j));
				}
			}
			
		}
		
		return candidate_mappings_1;
	}

	
	public void selectMappings(String integration_query) {

		Vector selected_mappings = this.pm.selectTopKMappings(integration_query, nb_new_generation);

		String integration_table = integration_query.replace("integration.", "integration_");

		if (selected_mappings.size() != 0) {
		

			java.sql.Statement st_1 = null;
			java.sql.Statement st_2 = null;
			java.sql.Statement st_3 = null;

			String query_1 = "delete from schemamappings where ";
			String query_2 = "delete from mapping_cardinal_annotation where mapping_id not in (select id from schemamappings)";
			String query_3 = "delete from mapping_"+integration_table+" where mapping_id not in (select m.id from schemamappings m)";
			
			
			for (int i=0;i<selected_mappings.size();i++) {
				if (i != 0)
					query_1 = query_1 + " and ";
				query_1 = query_1 + "(id != "+(Integer) selected_mappings.get(i)+")";
			}

			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st_1 = conn.createStatement();
				st_1.executeUpdate(query_1);

				st_2 = conn.createStatement();
				st_2.executeUpdate(query_2);

				st_3 = conn.createStatement();
				st_3.executeUpdate(query_3);
				
				st_1.close();
				st_2.close();
				st_3.close();
				conn.close();

			}catch (SQLException s){
				s.printStackTrace();	
			}

			
		}

	}
	

	public Vector add(Vector v1,Vector v2) {
		
		Vector v = new Vector();
		
		for (int i=0;i<v1.size();i++) 
			v.add(v1.get(i));

		for (int i=0;i<v2.size();i++) 
			v.add(v2.get(i));

		return v;
		
	}
	
	
	public Vector applyVariationOperators(String integration_query,Vector candidate_mappings) {
		
		System.out.print("Recombine mappings ... ");
		Vector candidate_mappings_1 = this.recombineMappings(integration_query,candidate_mappings);
		System.out.println("done");
		
		
		System.out.print("Mutate mappings ... ");
		Vector candidate_mappings_2 = this.mutateMappings(integration_query,candidate_mappings);
		System.out.println("done");

		
		
		return this.add(candidate_mappings_1,candidate_mappings_2);
		
	}

	public void initialiseTerminationCondition() {
		
		this.number_of_iterations = 1;
		
	}

	public boolean termination_condition_met() {
		
		String query = "select m.f_measure from mapping_cardinal_annotation m order by m.f_measure DESC limit 1";
		java.sql.Statement st = null;
		ResultSet rs = null;
		
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery(query);
			double f_measure = -1;
			if (rs.next()) {
				f_measure = rs.getInt(1);
			}
			
			System.out.println("Highest F Measure: "+f_measure);
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		this.number_of_iterations--;
		
		if (this.number_of_iterations == -1)
			return true;
		else 
			return false;
		
	}

	@Override
	public Vector getNeighbours(int map_id, String criterion, String integration_query) {
		
		Vector neighbours = new Vector();
		
		
		String integration_table = integration_query.replace("integration.", "integration_");
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		String query = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			if (criterion.equals("intersection")){
			
				query = "select mi.mapping_id, count(mi.result_id) as fp_number from mapping_results mi, unexpected_results fp "+
					"where (fp.base_table = '"+integration_table+"') and (mi.base_table = '"+integration_table+"') and (mi.result_id = fp.id) and (mi.mapping_id != ?) "+
					"and  (mi.result_id not in  (select mi_1.result_id from mapping_results mi_1 where (mi_1.base_table = '"+integration_table+"') and (mi_1.mapping_id = ?))) "+
					"group by mi.mapping_id "+
					"order by fp_number DESC";
		
			
				//query = "SELECT tpo.mapping_id_1 from tp_order tpo where (tpo.mapping_id_2 = ?)";
				st = conn.prepareStatement(query);
				st.setInt(1, map_id);
				st.setInt(2, map_id);
			}
			
			if (criterion.equals("union")) {

				query = "select mi.mapping_id, count(mi.result_id) as tp_number from mapping_results mi, expected_results tp "+
					"where (tp.base_table = '"+integration_table+"') and (mi.base_table = '"+integration_table+"') and  (mi.result_id = tp.id) and (mi.mapping_id != ?) "+
					"and  (mi.result_id not in  (select mi_1.result_id from mapping_results mi_1 where (mi_1.base_table = '"+integration_table+"') and (mi_1.mapping_id = ?))) "+
					"group by mi.mapping_id "+
					"order by tp_number DESC";
				//System.out.println("UNION QUERY: "+query+"\n MAP_ID: "+map_id);
				//query = "SELECT tpo.mapping_id_2 from schemamappings sm, tp_order tpo where (sm.id = tpo.mapping_id_2) and (sm.integration_query = ?) and (tpo.mapping_id_1 != ?)";
				st = conn.prepareStatement(query);
				st.setInt(1, map_id);
				st.setInt(2, map_id);
			
			}
			
			if (criterion.equals("difference")) {

				query = "select mi.mapping_id, count(mi.result_id) as fp_number from mapping_results mi, unexpected_results fp "+
						"where (tp.base_table = '"+integration_table+"') and (mi.base_table = '"+integration_table+"') and (mi.result_id = fp.id) and (mi.mapping_id != ?) "+
						"and  (mi.result_id in  (select mi_1.result_id from mapping_results mi_1 where (mi_1.base_table = '"+integration_table+"') and (mi_1.mapping_id = ?))) "+
						"group by mi.mapping_id "+
						"order by fp_number DESC";
			
				//query = "SELECT fpo.mapping_id_2 from fp_order fpo where (fpo.mapping_id_1 = ?)";
				st = conn.prepareStatement(query);
				st.setInt(1, map_id);
				st.setInt(2, map_id);
			}
		
			rs = st.executeQuery();
			while (rs.next()) {
				neighbours.add(rs.getInt(1));			
			}
			
			st.close();
			rs.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return neighbours;
	}

	public int getNeighbour(int map_id, String criterion, String integration_query) {
		
		int id = -1;
		
		String integration_table = integration_query.replace("integration.", "integration_");
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		String query = null;
		
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			if (criterion.equals("intersection")){
			
				query = "select mi.mapping_id, count(mi.result_id) as fp_number from mapping_results mi, unexpected_results fp "+
					"where (fp.base_table = '"+integration_table+"') and (mi.base_table = '"+integration_table+"') and (mi.result_id = fp.id) and (mi.mapping_id != ?) "+
					"and  (mi.result_id not in  (select mi_1.result_id from mapping_results mi_1 where (mi_1.base_table = '"+integration_table+"') and (mi_1.mapping_id = ?))) "+
					"group by mi.mapping_id "+
					"order by fp_number DESC";
		
			
				st = conn.prepareStatement(query);
				st.setInt(1, map_id);
				st.setInt(2, map_id);
			}
			
			if (criterion.equals("union")) {

				query = "select mi.mapping_id, count(mi.result_id) as tp_number from mapping_results mi, expected_results tp "+
					"where (tp.base_table = '"+integration_table+"') and (mi.base_table = '"+integration_table+"') and (mi.result_id = tp.id) and (mi.mapping_id != ?) "+
					"and  (mi.result_id not in  (select mi_1.result_id from mapping_results mi_1 where (mi_1.base_table = '"+integration_table+"') and (mi_1.mapping_id = ?))) "+
					"group by mi.mapping_id "+
					"order by tp_number DESC";
				//System.out.println("UNION QUERY: "+query+"\n MAP_ID: "+map_id);
				//query = "SELECT tpo.mapping_id_2 from schemamappings sm, tp_order tpo where (sm.id = tpo.mapping_id_2) and (sm.integration_query = ?) and (tpo.mapping_id_1 != ?)";
				st = conn.prepareStatement(query);
				st.setInt(1, map_id);
				st.setInt(2, map_id);
			
			}
			
			if (criterion.equals("difference")) {

				query = "select mi.mapping_id, count(mi.result_id) as fp_number from mapping_results mi, unexpected_results fp "+
					"where (fp.base_table = '"+integration_table+"') and (mi.base_table = '"+integration_table+"') and (mi.result_id = fp.id) and (mi.mapping_id != ?) "+
					"and  (mi.result_id in  (select mi_1.result_id from mapping_results mi_1 where (mi_1.base_table = '"+integration_table+"') and (mi_1.mapping_id = ?))) "+
					"group by mi.mapping_id "+
					"order by fp_number DESC";
			
				//query = "SELECT fpo.mapping_id_2 from fp_order fpo where (fpo.mapping_id_1 = ?)";
				st = conn.prepareStatement(query);
				st.setInt(1, map_id);
				st.setInt(2, map_id);
			}
		
			//System.out.println("Criterion: "+criterion);
			//System.out.println("Searching for a neighbour for the mapping: "+map_id);
			//System.out.println("query: \n"+query);
		
			st.setMaxRows(1);
		
			rs = st.executeQuery();
			if (rs.next()) {
				id = rs.getInt(1);
			
				st.close();
				rs.close();
				conn.close();
		}
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();	
		}
		
		return id;
	}

	public int createMapping(int map_1, int map_2,
			String criterion, String integration_query) {
		
		int created_mapping = -1;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		
		String query = "insert into schemamappings values (null,?,?)";
		String mapping_id = null;
		String s_query = null; 
		String s_query1 = this.getSourceQuery(map_1);
		String s_query2 = this.getSourceQuery(map_2);
		
		if ((s_query1 == null) || (s_query2 == null))
			return created_mapping;
		
		if (criterion.equals("union")) {
			String table_name1 = this.generateTableName();
			String table_name2 = this.generateTableName();
			
			s_query = "select "+table_name1+".* from ("+s_query1+") "+table_name1+" union select "+table_name2+".* from ("+s_query2+") "+table_name2;
			
			//System.out.println("Source query of the createted UNION mapping: \n"+s_query);
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st = conn.prepareStatement(query);
				st.setString(1, integration_query);
				st.setString(2, s_query);
				
				st.executeUpdate();
				
				rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    created_mapping = rs.getInt(1);
				}
				
				st.close();
				rs.close();
				conn.close();
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
		
		if (criterion.equals("intersection")) {
			String table_name1 = this.generateTableName();
			String table_name2 = this.generateTableName();
			
			s_query = "select "+table_name1+".* from ("+s_query1+") "+table_name1+" intersect select "+table_name2+".* from ("+s_query2+") "+table_name2;
			
			//System.out.println("Source query of the createted UNION mapping: \n"+s_query);
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st = conn.prepareStatement(query);
				st.setString(1, integration_query);
				st.setString(2, s_query);
				
				st.executeUpdate();
				
				rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    created_mapping = rs.getInt(1);
				}
				
				st.close();
				rs.close();
				conn.close();
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
		
		if (criterion.equals("difference")) {
			String table_name1 = this.generateTableName();
			String table_name2 = this.generateTableName();
			
			s_query = "select "+table_name1+".* from ("+s_query1+") "+table_name1+" except select "+table_name2+".* from ("+s_query2+") "+table_name2;
			
			//System.out.println("Source query of the createted UNION mapping: \n"+s_query);
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st = conn.prepareStatement(query);
				st.setString(1, integration_query);
				st.setString(2, s_query);
				
				st.executeUpdate();
				
				rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    created_mapping = rs.getInt(1);
				}
				
				st.close();
				rs.close();
				conn.close();
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
		
		return created_mapping;
	}

	public int createMapping_oldMySQL(int map_1, int map_2,
			String criterion, String integration_query) {
		
		int created_mapping = -1;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		
		String query = "insert into schemamappings values (null,?,?)";
		String mapping_id = null;
		String s_query = null; 
		String s_query1 = this.getSourceQuery(map_1);
		String s_query2 = this.getSourceQuery(map_2);
		
		if ((s_query1 == null) || (s_query2 == null))
			return created_mapping;
		
		if (criterion.equals("union")) {
			String table_name1 = this.generateTableName();
			String table_name2 = this.generateTableName();
			
			s_query = "select "+table_name1+".* from ("+s_query1+") "+table_name1+" union select "+table_name2+".* from ("+s_query2+") "+table_name2;
			
			//System.out.println("Source query of the createted UNION mapping: \n"+s_query);
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st = conn.prepareStatement(query);
				st.setString(1, integration_query);
				st.setString(2, s_query);
				
				st.executeUpdate();
				
				rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    created_mapping = rs.getInt(1);
				}
				
				st.close();
				rs.close();
				conn.close();
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
		
		if (criterion.equals("intersection")) {
			String table_name = this.generateTableName();
			s_query = this.intersection(s_query1, s_query2);
			
			//System.out.println("Source query of the createted INTERSECTION mapping: \n"+s_query);
			
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st.setString(1, integration_query);
				st.setString(2, s_query);
				st.executeUpdate();
				
				rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    created_mapping = rs.getInt(1);
				}
				
				st.close();
				rs.close();
				conn.close();
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}
		
		if (criterion.equals("difference")) {
			String table_name = this.generateTableName();
			s_query = this.difference(s_query1, s_query2);

			//System.out.println("Source query of the createted DIFFERENCE mapping: \n"+s_query);
			
			try  {
				if ((conn == null) || conn.isClosed() ) 	
					conn = source.getConnection();
				st = conn.prepareStatement(query);
				st.setString(1, integration_query);
				st.setString(2, s_query);
				st.executeUpdate();
				
				rs = st.getGeneratedKeys();
				if ( rs.next() ) {
				    // Retrieve the auto generated key(s).
				    created_mapping = rs.getInt(1);
				}
				
				st.close();
				rs.close();
				conn.close();
				
			}catch (SQLException s){
				
				System.err.println("Error while trying to execute the following query: "+query);
				s.printStackTrace();
			}
		}

		return created_mapping;
	}
	
	public String getSourceQuery(int map) {
		
		String s_query = null;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		String query = "select source_query from schemamappings where id = ?";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			st.setInt(1, map);

			rs = st.executeQuery();
		
			if (rs.next()) {
			
				s_query = rs.getString(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		return s_query;

	}
	
	public String generateTableName() {
		
		String table_name = "tab";
		java.sql.Statement st = null;
		ResultSet rs = null;
		String query = "insert into tablenames values (null)";
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			st.executeUpdate(query);

			rs = st.getGeneratedKeys();
			if ( rs.next() ) {
			    // Retrieve the auto generated key(s).
			    table_name = table_name+rs.getInt(1); 
			}

			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}

		return table_name;
		
	}
	
	public String intersection(String query_1, String query_2){
	
		String tab_1 = this.generateTableName();
		String tab_2 = this.generateTableName();
		
		String query = "select "+tab_1+".* from ("+query_1+") as "+tab_1+
						" inner join "+
						"("+query_2+") as "+tab_2+
						" on "+this.constructWhereClause(query_1, query_2, tab_1, tab_2);

		return query;
	}
	
	public String difference(String query_1, String query_2){
		
		String tab_1 = this.generateTableName();
		String tab_2 = this.generateTableName();
		
		String query = "select "+tab_1+".* from ("+query_1+") as "+tab_1+
						" where not exists (select "+tab_2+".* from "+
						"("+query_2+") as "+tab_2+
						" where "+this.constructWhereClause(query_1, query_2, tab_1, tab_2)+")";

		return query;
	}
	
	public String constructWhereClause(String query_1, String query_2, String tab1, String tab2) {
		
		String where_clause = null;
		String[] attributes1, attributes2;
		java.sql.PreparedStatement st1,st2 = null;
				
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st1 = conn.prepareStatement(query_1);
			st2 = conn.prepareStatement(query_2);
			
			attributes1 = new String[st1.getMetaData().getColumnCount()];
			attributes2 = new String[st2.getMetaData().getColumnCount()];
			
			for (int i=0;i<st1.getMetaData().getColumnCount();i++)
				attributes1[i] = st1.getMetaData().getColumnName(i+1);
			
			for (int i=0;i<st2.getMetaData().getColumnCount();i++)
				attributes2[i] = st2.getMetaData().getColumnName(i+1);
			
			if (attributes1.length == attributes2.length){
				where_clause = "";
				for (int i =0; i< attributes1.length;i++) {
					if (i != 0)
						where_clause = where_clause +" and ";
					where_clause = where_clause + " ("+tab1+"."+attributes1[i]+" = "+tab2+"."+attributes2[i]+") ";
				}	
			}
			
			st1.close();
			st2.close();
			conn.close();
			
		}catch (SQLException s){
			
			System.err.println("Error while trying to extaract metadata from the following queries: "+query_1 +"\n or \n"+query_2);
			s.printStackTrace();
			
		}
		
		return where_clause;
	}
	

	
	private int getMaxId(String table) {
		
		int max = -1;
		java.sql.Statement st = null;
		ResultSet rs = null;

		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery("select max(id) from "+table);

			if (rs.next()) {
				max = rs.getInt(1);
			}
			
			st.close();
			rs.close();
			conn.close();

		}catch (SQLException s){
			s.printStackTrace();	
		}

		return max;
	}

	
	public void executeMapping(int map_id, String integration_query){

		String integration_table = integration_query.replace("integration.", "integration_");
		String source_query = this.getSourceQuery(map_id);
		//System.out.println("source_query for mappings "+map_id+"\n");
		int result_id = -1;
		java.sql.PreparedStatement st = null;
		java.sql.PreparedStatement st_1 = null;
		ResultSet rs = null, rs_1 = null;
		String query_1 = null;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(source_query);
			rs = st.executeQuery();
			String[] attributes = new String[st.getMetaData().getColumnCount()];
			for (int i=0;i<attributes.length;i++)
				attributes[i] = st.getMetaData().getColumnName(i+1);
			String[] values = new String[attributes.length];
			
			String query = null; 
			
			while (rs.next()) {
			
				query = "select id";
				for (int i =0; i<attributes.length; i++) { 
					values[i] = rs.getString(i+1).toString();
				}
				
				result_id = this.exists(integration_table, values);
					
				if (result_id != -1) {
					this.addRef(integration_table, result_id, map_id);
				}
				
				else {
					result_id = this.exists("correctresults_"+integration_table, values);
					
					if (result_id == -1) {
						result_id = this.getMax(this.getMaxId(integration_table),this.getMaxId("correctresults_"+integration_table));
					}
				
					query_1 = "insert into "+integration_table+" values (?";
					for (int i=0;i<attributes.length;i++)
						query_1 = query_1+",?";
					query_1 = query_1+")";
								

					st_1 = conn.prepareStatement(query_1);
					st_1.setInt(1,++result_id);

					for (int i=0;i<values.length;i++)
						st_1.setObject(i+2,values[i]);
				
					st_1.executeUpdate();
					this.addRef(integration_table, result_id, map_id);
					
					st.close();
					st_1.close();
					rs.close();
					rs_1.close();
					conn.close();
				}
			}
		}catch (SQLException s){
			s.printStackTrace();	
		}

		
	}
	
	public int exists(String table, String[] values) {
		
		int result_id = -1;
		java.sql.PreparedStatement st = null;
		ResultSet rs = null;
		String query;
		String[] attributes = this.getAttributes(table);
		
		if (attributes == null)
			return result_id;

		if (attributes.length != values.length)
			return result_id;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
				query = "select id from "+table+ " where ";
				
				for (int i =0; i<attributes.length; i++) {
					query = query + " ("+attributes[i]+" = ?)" ;
					if (i != attributes.length - 1)
						query = query + " and ";
				}
				
				st = conn.prepareStatement(query);
				
				for (int i =0; i<values.length; i++) 
					st.setString(i+1,values[i]);
				
				rs = st.executeQuery();
			
				if (rs.next()) 
					result_id = rs.getInt(1);
				
				st.close();
				rs.close();
				conn.close();
				
			
		}catch (SQLException s){
			s.printStackTrace();	
		}

		return result_id;
		
	}
	
	private String[] getAttributes(String table) {
		
		String[] attributes = null;
		java.sql.PreparedStatement st = null;
		String query = "select * from "+table;
		
		try  {
			if ((conn == null) || conn.isClosed() ) 	
				conn = source.getConnection();
			st = conn.prepareStatement(query);
			attributes = new String[st.getMetaData().getColumnCount() - 1];
			
			for (int i=0;i<attributes.length;i++)
				attributes[i] = st.getMetaData().getColumnName(i+2);
			st.close();
			conn.close();
		
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
		}
		
		return attributes;
	}

	private void addRef(String integration_table, int result_id, int map_id) {

		java.sql.Statement st = null;
		String query = "insert into mapping_"+integration_table+" values ("+map_id+",'"+result_id+"')";
				
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
	
	private int getMax(int i1, int i2) {
		
		
		if (i1>i2) {
			return i1;
		}
		else {
			return i2;
		}
			
	}

	
	public static void main(String[] args) {
		Jdbc3PoolingDataSource source = ConnectionManager.getSource();
		RefineMappingsPostgres rm = new RefineMappingsPostgres(source);
		rm.refineMappings("integration.europeancities");
		//System.out.println(rm.createJoinMapping("integration.europeancities",1));
		//rm.getTableFromQuery("select * from table_1 hgfdhgd from table_2 gfdgdf from table__3 from table");
		//rm.createJoinMapping("integration.europeancities", 21);
	}
	
}

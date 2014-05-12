package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Vector;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.IntegrationQuery;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;

/*
 *  @author Khalid Belhajjame
 */

public class SchemaMappingRetrievalPostgres implements SchemaMappingRetrieval {

    /*
	Connection dataspaces_repository_con = null;
    String url = "jdbc:mysql://localhost:3306/";
    String dataspaces_repository_db = "dataspaces_repository";
    String driver = "com.mysql.jdbc.Driver";
    String user = "dataspaces";
    String pass = "adana";
    */
	
	Jdbc3PoolingDataSource source = null;
	Connection conn;
	
	
	public SchemaMappingRetrievalPostgres(Jdbc3PoolingDataSource _source) {
		super();
		this.source = _source;
	}

	@Override
	public Vector getCandidateMappings(String i_query) {

		
		Vector mappings = new Vector();
		
		
		java.sql.PreparedStatement st = null;
		String query = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 			
			query = "select * from schemamappings where integration_query = ?";
			st = conn.prepareStatement(query);
			st.setString(1, i_query);
			ResultSet rs = st.executeQuery();
		
			while (rs.next()){
				
				SchemaMapping map = new SchemaMapping();
				map.setId(rs.getInt(1));
				map.setIntegrationRelation(rs.getString(2));
				map.setS_query(rs.getString(3));

				mappings.add(map);
			}
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
		return mappings;
		
	}

	@Override
	public Vector getCandidateMappings() {

		Vector mappings = new Vector();
		
		java.sql.Statement st = null;
		String query = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
			query = "select * from schemamappings";
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			while (rs.next()){
				
				SchemaMapping map = new SchemaMapping();
				map.setId(rs.getInt(1));
				map.setIntegrationRelation(rs.getString(2));
				map.setS_query(rs.getString(3));

				mappings.add(map);
			}
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
		return mappings;
	}
	
	@Override
	public Vector getPruningResultsIDs(String integration_query){
		
		Vector results_ids = new Vector();
		java.sql.PreparedStatement st = null;
		String query = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
			
			query = "SELECT distinct snapshotid FROM his_mapping_cardinal_annotation h, schemamappings sm where (sm.id = h.mapping_id) and (sm.integration_query = ?) order by snapshotid ASC";
			st = conn.prepareStatement(query);
			st.setString(1,integration_query);
			ResultSet rs = st.executeQuery();
		
			while (rs.next()){

				results_ids.add(rs.getString(1));
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		return results_ids;
	}

	@Override
	public Vector getPruningResults(String id) {
	
		Vector pruning_results = new Vector();
		java.sql.PreparedStatement st = null;
		String query = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 		
			query = "SELECT h.mapping_id, h.precision, h.recall, h.f_measure, h.annotated_results  FROM his_mapping_cardinal_annotation h where (h.snapshotid = ?) order by h.f_measure DESC";
			st = conn.prepareStatement(query);
			st.setString(1,id);
			ResultSet rs = st.executeQuery();
		
			while (rs.next()){

				String[] row = {"","","","",""};
				
				row[0] = ((Integer) rs.getInt(1)).toString();
				row[1] = Double.toString(rs.getDouble(2));
				row[2] = Double.toString(rs.getDouble(3));
				row[3] = Double.toString(rs.getDouble(4));
				row[4] = Double.toString(rs.getDouble(5));
				
				pruning_results.add(row);
				
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		return pruning_results;
	}

	
	public int getMinimunMapID(){
		
		int min = 0;
		String query = null;
		java.sql.Statement st = null;
		
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			query = "select min(id) from schemamappings";
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			if (rs.next()){
				
				min = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		
		return min;
	}
	
	public int getMaxMappingId() {

		int max = 0;
		String query = null;
		java.sql.Statement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			query = "select max(mapping_id) from his_mapping_cardinal_annotation";
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
		
			if (rs.next()){
				
				max = rs.getInt(1);
				
			}
			
			st.close();
			rs.close();
			conn.close();
			
		}catch (SQLException s){
			System.err.println("Error while trying to execute the following query: "+query);
			s.printStackTrace();
			
		}
		
		return max;

		
	}
	
	public int getNumberofCorrectResults(String integration_table, String snapshot_id) {
		
		int num = 0;
		String query = null;
		java.sql.PreparedStatement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			query = "select count(*) from his_expected_results h where (h.snapshotid = ?) and (h.base_table = ?)";
			st = conn.prepareStatement(query);
			st.setString(1, snapshot_id);
			st.setString(2, integration_table);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()){
				
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
	
	public int getNumberofTP(String integration_table, String snapshot_id, int mapping_id) {
		
		int num = 0;
		String query = null;
		java.sql.PreparedStatement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			query = "select count(*) from mapping_results m, his_expected_results h where (h.snapshotid = ?) and (m.mapping_id = ?) and (m.result_id = h.id) and (h.base_table = ?) and (m.base_table = ?)";
			st = conn.prepareStatement(query);
			st.setString(1, snapshot_id);
			st.setInt(2, mapping_id);
			st.setString(3,integration_table);
			st.setString(4,integration_table);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()){
				
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
	
	public int getNumberofFN(String integration_table, String snapshot_id, int mapping_id) {
		
		int num = 0;
		String query = null;
		java.sql.PreparedStatement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			query = "select count(*) from his_expected_results h where (h.snapshotid = ?) and (h.base_table = ?) and  (h.id not in (select m.result_id from mapping_results m where (m.mapping_id =  ?) and (m.base_table = ?)))";
			st = conn.prepareStatement(query);
			st.setString(1, snapshot_id);
			st.setString(2, integration_table);
			st.setInt(3, mapping_id);
			st.setString(4, integration_table);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()){
				
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
	
	public int getNumberofFP(String integration_table, String snapshot_id, int mapping_id) {
		
		int num = 0;
		String query = null;
		java.sql.PreparedStatement st = null;
		
		try  {
			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 	
			query = "select count(*) from mapping_results m, his_unexpected_results h where (h.snapshotid = ?) and (m.mapping_id = ?) and (m.result_id = h.id) and (h.base_table = ?) and (m.base_table = ?)";
			st = conn.prepareStatement(query);
			st.setString(1, snapshot_id);
			st.setInt(2, mapping_id);
			st.setString(3, integration_table);
			st.setString(4, integration_table);
			ResultSet rs = st.executeQuery();
		
			if (rs.next()){
				
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
	
	public void loadDump(String file) {
		
		
		try {
			//Process p = Runtime.getRuntime().exec("mysql -uroot -e \"source "+file+"\"");
			Process p = Runtime.getRuntime().exec("/Library/PostgreSQL/8.4/bin/psql -Upostgres -d dsms -f "+file+"\"");
			p.waitFor();
			System.out.println("process"+p.exitValue());
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createDump(String file) {
		
		
		//String command = "C:\\Software\\mysql-5.1.32-win32\\bin\\mysqldump -uroot --databases dataspaces_repository";
		String command =  "/Library/PostgreSQL/8.4/bin/pg_dump -Upostgres  dsms";
		FileWriter fw = null;
		
		 try {
			fw = new FileWriter(file);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println(command);
		
		try {
			Process p = Runtime.getRuntime().exec(command);
			String s;
            BufferedReader stdInput = new BufferedReader(new 
                    InputStreamReader(p.getInputStream()));

               BufferedReader stdError = new BufferedReader(new 
                    InputStreamReader(p.getErrorStream()));

               // read the output from the command
               
               System.out.println("Here is the standard output of the command:\n");
               while ((s = stdInput.readLine()) != null) {
                   //System.out.println(s);
            	   fw.write(s+"\n");
               }
               
               // read any errors from the attempted command

               System.out.println("Here is the standard error of the command (if any):\n");
               while ((s = stdError.readLine()) != null) {
                   System.out.println(s);
               }
               
             

		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void exportPruningResults(String i_query) {
		
		String integration_table = i_query.replace("integration.", "integration_");
		int result_id = 0, num_tp=0, num_fp=0, num_fn=0, num_correct_results=0;
		int min_map_id = this.getMinimunMapID();
		min_map_id = 0;
		int feedback_amount = 0;
		Integer mapping_id;
		double precision, recall, f_measure, annotated_results;
		
		int max_map_id = this.getMaxMappingId();
		
		try {

	        BufferedWriter out_prec = new BufferedWriter(new FileWriter("Resources/pruning_results_precision_"+(new Date()).getTime()+".csv"));
	        BufferedWriter out_recall = new BufferedWriter(new FileWriter("Resources/pruning_results_recall_"+(new Date()).getTime()+".csv"));
	        BufferedWriter out_f_measure = new BufferedWriter(new FileWriter("Resources/pruning_results_f_measure_"+(new Date()).getTime()+".csv"));
	        BufferedWriter all = new BufferedWriter(new FileWriter("Resources/pruning_results_all_"+(new Date()).getTime()+".csv"));
	        BufferedWriter results_per_mapping = new BufferedWriter(new FileWriter("Resources/pruning_results_per_mapping_"+(new Date()).getTime()+".csv"));
	        BufferedWriter results_annotations = new BufferedWriter(new FileWriter("Resources/pruning_results_annotations_"+(new Date()).getTime()+".csv"));
	        BufferedWriter results_annotations_without_top_map = new BufferedWriter(new FileWriter("Resources/pruning_results_annotations__without_top_map_"+(new Date()).getTime()+".csv"));
	        BufferedWriter removed_mappings = new BufferedWriter(new FileWriter("Resources/removed_mappings_"+(new Date()).getTime()+".csv"));
	        BufferedWriter mappings_annotations_precision = new BufferedWriter(new FileWriter("Resources/mappings_annotations_precision_"+(new Date()).getTime()+".csv"));
	        BufferedWriter mappings_annotations_recall = new BufferedWriter(new FileWriter("Resources/mappings_annotations_recall_"+(new Date()).getTime()+".csv"));
	        BufferedWriter mappings_annotations_f_measure = new BufferedWriter(new FileWriter("Resources/mappings_annotations_f_measure_"+(new Date()).getTime()+".csv"));
	        BufferedWriter mappings_ranking = new BufferedWriter(new FileWriter("Resources/mappings_ranking_"+(new Date()).getTime()+".csv"));
	        
	        out_prec.write("C , feedback_amount, mapping, precision \n");
	        out_recall.write("result_id , feedback_amount, mapping, recall \n");
	        out_f_measure.write("result_id , feedback_amount, mapping, F_measure \n");
	        all.write("result_id , feedback_amount, mapping, precision, recall, f_measure, annotated_results \n");
	        results_annotations.write("result_id , top_mapping, feedback_amount, precision, recall, f_measure \n");
	        results_annotations_without_top_map.write("result_id , feedback_amount, precision, recall, f_measure \n");
	        removed_mappings.write("result_id , mapping \n");
			
			java.sql.Statement st,st4, st5, st6, st7, st9, st11 = null;
			java.sql.PreparedStatement st1,st2,st3=null,st8=null,st10;
			String query1, query2, query3, query4, query5, query6, query7, query8, query9, query10, query11 = null;
			ResultSet rs,rs1, rs2, rs3=null, rs4, rs5, rs6, rs7, rs8=null, rs9, rs10=null, rs11 = null;
			

			if ((conn == null) || conn.isClosed()) 	
				conn = source.getConnection(); 				
			query11 = "select mapping_id from correct_mapping_cardinal_annotation order by f_measure DESC";
			query9 = "SELECT distinct h.snapshotid, f.amount from his_mapping_cardinal_annotation h, his_feedback_amount f where (h.snapshotid = f.snapshot_id) order by snapshotid ASC";
			query10 = "SELECT mapping_id from his_mapping_cardinal_annotation where (snapshotid = ?) order by f_measure DESC";
			st9 = conn.createStatement();
			st10 = conn.prepareStatement(query10);
			st11 = conn.createStatement();
			
			rs11 = st11.executeQuery(query11);
			mappings_ranking.write(",");
			while (rs11.next()) {
				mappings_ranking.write(",m"+rs11.getInt(1));
			}
			mappings_ranking.write("\n");
			
			rs9 = st9.executeQuery(query9);
			while(rs9.next()) {
				mappings_ranking.write(rs9.getString(1)+","+rs9.getInt(2));
				st10.setString(1, rs9.getString(1));
				rs10 = st10.executeQuery();
				while (rs10.next()) {
					mappings_ranking.write(",m"+rs10.getInt(1));
				}
				mappings_ranking.write("\n");	
			}
			mappings_ranking.close();
			
			mappings_annotations_precision.write("result_id, feedback_amount");
			mappings_annotations_recall.write("result_id, feedback_amount");
			mappings_annotations_f_measure.write("result_id, feedback_amount");
			for (int i=1;i<=max_map_id;i++){
				mappings_annotations_precision.write(",m_"+i);
				mappings_annotations_recall.write(",m_"+i);
				mappings_annotations_f_measure.write(",m_"+i);
			}
			mappings_annotations_precision.write("\n");
			mappings_annotations_recall.write("\n");
			mappings_annotations_f_measure.write("\n");
			
			query7 = "select * from his_feedback_amount";
			query8 = "select h.precision, h.recall, h.f_measure from his_mapping_cardinal_annotation h where (h.snapshotid = ?) and (h.mapping_id = ?)";
			st7 = conn.createStatement();
			rs7 = st7.executeQuery(query7);
			while(rs7.next()) {
				String res_id = rs7.getString(1);
				int fd_amount = rs7.getInt(2);
				mappings_annotations_precision.write(res_id+","+fd_amount);
				mappings_annotations_recall.write(res_id+","+fd_amount);
				mappings_annotations_f_measure.write(res_id+","+fd_amount);
				
				for (int i=1;i<=max_map_id;i++) {
					
					st8 = conn.prepareStatement(query8);
					st8.setString(1,res_id);
				    st8.setInt(2,i);
				    
				    rs8 = st8.executeQuery();
				    
			    if (rs8.next()) {
				    	mappings_annotations_precision.write(","+rs8.getDouble(1));
				    	mappings_annotations_recall.write(","+rs8.getDouble(2));
				    	mappings_annotations_f_measure.write(","+rs8.getDouble(3));
				    }
				    else  {
				    	mappings_annotations_precision.write(",");
			    	mappings_annotations_recall.write(",");
				    	mappings_annotations_f_measure.write(",");
				    }
				}
				mappings_annotations_precision.write("\n");
				mappings_annotations_recall.write("\n");
				mappings_annotations_f_measure.write("\n");
				
		}
			mappings_annotations_precision.close();
			mappings_annotations_recall.close();
			mappings_annotations_f_measure.close();
			
			query5 = "select * from his_ruled_out_mappings order by snapshot_id";
			st5 = conn.createStatement();
			rs5 = st5.executeQuery(query5);
			while (rs5.next()) {
				removed_mappings.write(rs5.getString(1)+","+rs5.getInt(2)+"\n");
			}
			removed_mappings.close();
			
			query4 = "select f.amount, r.*, t.mapping_id from his_feedback_amount f, his_results_annotations r, his_top_mappings t where (r.snapshot_id = f.snapshot_id) and (r.snapshot_id = t.snapshot_id) order by r.snapshot_id";
			st4 = conn.createStatement();
			rs4 = st4.executeQuery(query4);
			while (rs4.next()) {
		        results_annotations.write(rs4.getLong(2)+" ,"+rs4.getInt(6)+" ,"+rs4.getInt(1)+","+rs4.getDouble(3)+","+rs4.getDouble(4)+","+rs4.getDouble(5)+"\n");					
			}
			results_annotations.close();
			
			query6 = "select f.amount, r.* from his_feedback_amount f, his_results_annotations r where (r.snapshot_id = f.snapshot_id)  order by r.snapshot_id";
			st6 = conn.createStatement();
			rs6 = st6.executeQuery(query6);
			while (rs6.next()) {
		        results_annotations_without_top_map.write(rs6.getLong(2)+" ,"+rs6.getInt(1)+","+rs6.getDouble(3)+","+rs6.getDouble(4)+","+rs6.getDouble(5)+"\n");					
			}
			results_annotations_without_top_map.close();
				
			query1 = "SELECT distinct h.snapshotid FROM his_mapping_cardinal_annotation h, schemamappings sm where (h.mapping_id = sm.id) and (sm.integration_query = ?) order by h.snapshotid ASC";
			st1 = conn.prepareStatement(query1);
			st1.setString(1,i_query);
			
			rs1 = st1.executeQuery();
			
			while (rs1.next()) {
				query2 = "SELECT f.amount, h.snapshotid, h.mapping_id, h.precision, h.recall, h.f_measure, h.annotated_results   FROM his_mapping_cardinal_annotation h, his_feedback_amount f where (h.snapshotid = f.snapshot_id) and (h.snapshotid = ?) order by h.f_measure DESC";
				st2 = conn.prepareStatement(query2);
				st2.setString(1,rs1.getString(1));
				rs2 = st2.executeQuery();
				result_id++;
			while (rs2.next()){
				feedback_amount = rs2.getInt(1);
				mapping_id = rs2.getInt(3);
				precision = rs2.getDouble(4);
				recall = rs2.getDouble(5);
				f_measure = rs2.getDouble(6);
				annotated_results = rs2.getDouble(7);
				
		        out_prec.write(""+result_id+" , "+ feedback_amount+" , "+ (mapping_id - min_map_id) + " , "+precision +" \n");
		        out_recall.write(""+result_id+" , "+ feedback_amount+" , "+ (mapping_id - min_map_id) + " , "+recall +" \n");
		        out_f_measure.write(""+result_id+" , "+ feedback_amount+" , "+ (mapping_id - min_map_id) + " , "+f_measure +" \n");
		        all.write(""+result_id+" , "+ feedback_amount+" , "+ (mapping_id - min_map_id) + " , "+precision +" , "+recall +", "+f_measure +","+annotated_results+" \n");
				
			}
			
	        out_prec.write("\n");
	        out_recall.write("\n");
	        out_f_measure.write("\n");
	        all.write("\n");
			}
		
			Vector mappings = this.getCandidateMappings(i_query);
		
			for (int i=0; i<mappings.size();i++) {
			
				int map_id = ((SchemaMapping) mappings.get(i)).getId();
				results_per_mapping.write("mapping_"+map_id+" , feedback_amount, precision, recall, f_measure, annotated_results, tp, fp, fn \n");
					query3 = "SELECT f.amount, c.* FROM his_mapping_cardinal_annotation c, his_feedback_amount f where (c.snapshotid = f.snapshot_id) and (mapping_id = ?) order by snapshotid";
				st3 = conn.prepareStatement(query3);
				st3.setInt(1,map_id);
					rs3 = st3.executeQuery();
			
			
				while (rs3.next()) {
				//for (int h=0;h<80;h++) 
				//	if (rs3.next()){
					
				//	System.out.print(" "+h+" ");	
				//	num_tp = this.getNumberofTP(integration_table, rs3.getString(2), rs3.getInt(3));
				//	num_fp = this.getNumberofFP(integration_table, rs3.getString(2), rs3.getInt(3));
				//	num_correct_results = this.getNumberofCorrectResults(integration_table, rs3.getString(2));
				//	num_fn = num_correct_results - num_tp;
				
					results_per_mapping.write(rs3.getInt(3)+" , "+rs3.getInt(1)+" , "+ rs3.getDouble(4)+" , "+ rs3.getDouble(5)+" , "+ rs3.getDouble(6)+" , "+ rs3.getDouble(7)+" , "+num_tp+" , "+num_fp+" , "+num_fn+" \n");
				
				}
				
				results_per_mapping.write("\n\n");
				System.out.println();
				
			
			}		
			
			results_per_mapping.close();
			
	        BufferedWriter out_completeknowledge = new BufferedWriter(new FileWriter("Resources/pruning_results_complete_knowledge_"+(new Date()).getTime()+".csv"));
	        out_completeknowledge.write("mapping, precision, recall, f_measure, annotated_results \n");
	        String query = "select c.* from correct_mapping_cardinal_annotation c order by c.f_measure DESC";
	        st = conn.createStatement();
	        rs = st.executeQuery(query);
	        
	        while (rs.next()) {
				mapping_id = rs.getInt(1);
				precision = rs.getDouble(2);
				recall = rs.getDouble(3);
				f_measure = rs.getDouble(4);
					out_completeknowledge.write((mapping_id - min_map_id)+ " , "+precision +" , "+recall +", "+f_measure +"\n");	
	        }
	        
	        out_completeknowledge.close();
			
	        out_prec.close();
	        out_recall.close();
	        out_f_measure.close();
	        all.close();
	        
	        st.close();st1.close();st3.close();st4.close();st5.close();st6.close();st7.close();st8.close();st9.close();st10.close();st11.close();
	        rs.close();rs1.close();rs3.close();rs4.close();rs5.close();rs6.close();rs7.close();rs8.close();rs9.close();rs10.close();rs11.close();
		}catch (SQLException s){
			s.printStackTrace();
		} catch (IOException e) {
    	e.printStackTrace();
    }

}
	
	public static void main(String[] args){
		
		Jdbc3PoolingDataSource source = ConnectionManager.getSource();
		String file_name = "C:\\Software\\mysql-5.1.32-win32\\bin\\28_may\\initial_dataset_poor_mappings.sql";
		SchemaMappingRetrieval smr = new SchemaMappingRetrievalPostgres(source);
		//smr.loadDump(file_name);
		smr.createDump("C:\\Dumps\\1.sql");
		
	}
	

}

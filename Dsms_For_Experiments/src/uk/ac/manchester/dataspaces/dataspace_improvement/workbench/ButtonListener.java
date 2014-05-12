package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Map;
import java.util.Vector;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import sun.util.calendar.BaseCalendar.Date;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluation;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluationPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagement;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagementPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrieval;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrievalPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappings;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappingsPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.RefineMappings;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.RefineMappingsPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;

public class ButtonListener implements ActionListener {
	
	Workbench wb = null;
	QueryEvaluation qe = null;
	QueryResultManagement qrm = null;
	SchemaMappingRetrieval smr = null;
	PruneMappings pm = null;
	RefineMappings rm = null;
	int pruning_loop = 100;
	double f_measure = 0.1;
	double support = 0.01;
	int annotated_tp = 0, annotated_fp =0, annotated_fn = 0;
	int num_tp, num_fp;

	
	ButtonListener(Workbench _wb){
		super();
		this.wb = _wb;
		this.qe = new QueryEvaluationPostgres(this.wb.source);
		this.pm = new PruneMappingsPostgres(this.wb.source);
		this.rm = new RefineMappingsPostgres(this.wb.source);
		this.qrm = new QueryResultManagementPostgres(this.wb.source);
		this.smr = new SchemaMappingRetrievalPostgres(this.wb.source);

	}

	public void actionPerformed(ActionEvent e) {
				
		if (e.getSource().equals(this.wb.jButton1)) {

			String i_query = this.wb.jTextArea1.getText();
			System.out.println("Integration query: "+i_query);
			ResultsSet results = this.qe.evaluateIntegrationQuery(i_query);
			this.wb.displayQueryResults(results);
	
		}
		
		if (e.getSource().equals(this.wb.generatePositiveFeedback)) {

			this.pm.generateCorrectFeedbackInstance(this.wb.integration_query);
	
		}
		
		if (e.getSource().equals(this.wb.generateNegativeTupleFeedback)) {

			this.pm.generateIncorrectTupleFeedbackInstance(this.wb.integration_query);
	
		}
		
		if (e.getSource().equals(this.wb.generateNegativeAttributeFeedback)) {

			this.pm.generateIncorrectAttributeFeedbackInstance(this.wb.integration_query);
	
		}
		
		if (e.getSource().equals(this.wb.generateAFalseNegative)) {

			this.pm.generateAFalseNegative(this.wb.integration_query);
	
		}

		if (e.getSource().equals(this.wb.refineMappings)) {
			
			System.out.println("******************** Mapping refinement started *****************");
			this.rm.refineMappings(this.wb.integration_query);
			System.out.println("******************** Mapping refinement terminated *****************");
			
		}
		
		if (e.getSource().equals(this.wb.prunmaps)) {
			
			int top_k = 3;
			int sample_size = 10;
			Map feedback_ratios;			
	
			System.out.println("Mapping pruning started");

			System.out.print("Pruning results reinitialisation ... ");
			this.qrm.reinitialisePruningResults();
			System.out.println("done");	

			System.out.print(" Retrieve top k mappings ... ");
			Vector top_mappings =this.pm.getMappings(this.wb.integration_query);

			System.out.print("Generate feedback ... ");			
			feedback_ratios = this.pm.getFeedbackRatios(this.wb.integration_query,top_mappings,annotated_tp,annotated_fp,annotated_fn);
			annotated_tp = annotated_tp + (Integer) feedback_ratios.get("tp");
			annotated_fp = annotated_fp + (Integer) feedback_ratios.get("fp");
			annotated_fn = annotated_fn + (Integer) feedback_ratios.get("fn");
			this.pm.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("tp"), top_mappings, "tp");
			this.pm.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("fp"), top_mappings, "fp_tuple");
			System.out.println("done");
			
			System.out.print("Update log ... ");
			this.qrm.updateLog(this.wb.integration_query,top_mappings);
			System.out.println("done");		
			
			System.out.print("Prune mapping given collected feedback instances ... ");
			this.pm.pruneMappings(this.wb.integration_query);
			System.out.println("done");

			System.out.println("Mapping pruning terminated");

			/*
			Map feedback_ratios;
			Vector top_mappings =this.pm.getMappings(this.wb.integration_query);
			int loop = 1;
			for (int h=0;h<loop;h++) {
				System.out.println("Pruning "+h);
				if (h != 0) {
					System.out.print("Update log ... ");
					
					this.qrm.updateLog(this.wb.integration_query,top_mappings);
					System.out.println("done");
				
					System.out.print("Pruning results reiinitialisation ... ");
					this.qrm.reinitialisePruningResults();
					System.out.println("done");
				}
			
				System.out.print("Generate feedback ... ");
			
				feedback_ratios = this.pm.getFeedbackRatios(this.wb.integration_query,top_mappings,annotated_tp,annotated_fp,annotated_fn);
				annotated_tp = annotated_tp + (Integer) feedback_ratios.get("tp");
				annotated_fp = annotated_fp + (Integer) feedback_ratios.get("fp");
				annotated_fn = annotated_fn + (Integer) feedback_ratios.get("fn");

				this.pm.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("tp"), top_mappings, "tp");
				this.pm.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("fp"), top_mappings, "fp_tuple");
				System.out.println("done");
			
				System.out.print("Prune mapping given collected feedback instances ... ");
				this.pm.pruneMappings(this.wb.integration_query);
				System.out.println("done");
				
				//System.out.println("******************** Mapping refinement started *****************");
				//this.rm.refineMappings(this.wb.integration_query);
				//System.out.println("******************** Mapping refinement terminated *****************");

			}
			
			System.exit(0);
			*/
		}
		
		if (e.getSource().equals(this.wb.generalMappingPruning)) {
			int top_k = 3;
			int sample_size = 10;
			Map feedback_ratios;
			
			for (int l = 0; l<1; l++) {
				
			System.out.println("|||||||||| Round "+l+" ||||||||||||||||");	
			//smr.loadDump("C:\\Software\\mysql-5.1.32-win32\\bin\\amalgam_exp\\initialise.sql");
			smr.loadDump("/Users/khalidbelhajjame/Dumps/dsms_postgres.sql");
				
			System.out.println("******************** Mapping pruning started *****************");

			System.out.print("Pruning results reinitialisation ... ");
			this.qrm.reinitialisePruningResults();
			System.out.println("done");
			
			for (int i=0;i<this.pruning_loop;i++) {
			//for (int i=0;i<1;i++) {
				System.out.println("TIME: "+(new java.util.Date()).getTime());
				System.out.println("Mapping pruning for the "+i+" time");
				System.out.print(" Retrieve top k mappings ... ");
				//Vector top_mappings = this.pm.selectTopKMappings(this.wb.integration_query, top_k);
				Vector top_mappings =this.pm.getMappings(this.wb.integration_query);
				//Vector top_mappings = this.pm.selectMappingsThatCoverTP(this.wb.integration_query);
				//Vector top_mappings = new Vector(); top_mappings.add(29);top_mappings.add(11);top_mappings.add(12);top_mappings.add(1);top_mappings.add(23);
				System.out.print("Generate feedback ... ");
				
				feedback_ratios = this.pm.getFeedbackRatios(this.wb.integration_query,top_mappings,annotated_tp,annotated_fp,annotated_fn);
				annotated_tp = annotated_tp + (Integer) feedback_ratios.get("tp");
				annotated_fp = annotated_fp + (Integer) feedback_ratios.get("fp");
				annotated_fn = annotated_fn + (Integer) feedback_ratios.get("fn");

				this.pm.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("tp"), top_mappings, "tp");
				//this.pm.generateFeedbackByType(this.wb.integration_query, 11, top_mappings, "tp");
				this.pm.generateFeedbackByType(this.wb.integration_query, (Integer) feedback_ratios.get("fp"), top_mappings, "fp_tuple");
				//this.pm.generateFeedbackByType(this.wb.integration_query,  89, top_mappings, "fp_tuple");
				//this.pm.generateFeedbackByType(this.wb.integration_query, 8, top_mappings, "fp_attribute");
				//this.pm.generateFeedbackByType(this.wb.integration_query,  (Integer) feedback_ratios.get("fn"), top_mappings, "fn");
				//this.pm.generateFeedback(this.wb.integration_query, 3, top_mappings);
				
				//this.pm.generateFeedbackFromSample(this.wb.integration_query, sample_size, top_mappings);
				System.out.println("done");
				System.out.print("Update log ... ");
				this.qrm.updateLog(this.wb.integration_query,top_mappings);
				System.out.println("done");		
				//System.out.print("Rule out low quality mappings ... ");
				//this.pm.ruleOutMappings(this.qrm.getSnapshotID(),this.wb.integration_query, f_measure, support);
				//System.out.println("done");			
				System.out.println("Did Pruning results changed since the last time: "+this.pm.did_pruning_results_change(this.wb.integration_query));
				System.out.print("Pruning results reiinitialisation ... ");
				this.qrm.reinitialisePruningResults();
				System.out.println("done");	
				System.out.print("Prune mapping given collected feedback instances ... ");
				this.pm.pruneMappings(this.wb.integration_query);
				System.out.println("done");
				//System.out.print("Refine mappings ... ");
				//this.rm.refineMappings(this.wb.integration_query);
				//System.out.println("done");
			}
			
			System.out.println("******************** Mapping pruning terminated *****************");
			
			System.out.println("TIME: "+(new java.util.Date()).getTime());
			smr.createDump("/Users/khalidbelhajjame/Dumps/map_annotations/"+l+".sql");
			}
			
			System.exit(0);
		
		}

		
		if (e.getSource().equals(this.wb.jButton2)) {
			final String[] attribute_names = new String[this.wb.results_table.getColumnCount() - 2];
			final String i_query = this.wb.integration_query;
			final Jdbc3PoolingDataSource _source = this.wb.source;
			for (int i = 0; i < this.wb.results_table.getColumnCount() - 2; i++)
				attribute_names[i] = this.wb.results_table.getColumnName(i);
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					JFrame frame = new JFrame();
					frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
					FNFeedbackDialog inst = new FNFeedbackDialog(frame,i_query,attribute_names,_source);
					inst.setVisible(true);
				}
			});
		}

	
	}

}

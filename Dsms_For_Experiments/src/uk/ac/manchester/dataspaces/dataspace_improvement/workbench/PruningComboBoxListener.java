package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Vector;

import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluation;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluationPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrieval;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrievalPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdate;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdatePostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;

public class PruningComboBoxListener implements ItemListener {
	
	Workbench wb = null;
	String integration_query = null;
	SchemaMappingRetrieval smr = null;


	
	PruningComboBoxListener(Workbench _wb){
		super();
		this.wb = _wb;
		smr = new SchemaMappingRetrievalPostgres(this.wb.source);
		
	}


	@Override
	public void itemStateChanged(ItemEvent evt) {
		// TODO Auto-generated method stub
		
        // Get the affected item
        Object item = evt.getItem();
        int row;

        if (evt.getSource().equals(this.wb.integrationQueries) && (evt.getStateChange() == ItemEvent.SELECTED)) {
        	
        	integration_query = evt.getItem().toString();
        	Vector results_ids = smr.getPruningResultsIDs(this.integration_query);
        	this.wb.pruningResultsID.removeAllItems();
        	for (int i=0;i<results_ids.size();i++)
        		this.wb.pruningResultsID.addItem(results_ids.get(i).toString()); 	
        }
		
        if (evt.getSource().equals(this.wb.pruningResultsID) && (evt.getStateChange() == ItemEvent.SELECTED)) {
        	 
        	String pruning_results_id = evt.getItem().toString();
        	Vector pruning_results = smr.getPruningResults(pruning_results_id);
              	
        	DefaultTableModel  model = (DefaultTableModel) this.wb.pruning_results_table.getModel();
        	   int numrows = model.getRowCount(); 
        	   for(int i = numrows - 1; i >=0; i--)
        	   {
        		model.removeRow(i); 
        	   }
        	
        	for (int i=0; i<pruning_results.size();i++) {
        		
        		model.addRow((String[]) pruning_results.get(i));
        		
        	}
        	
        }
        
	}


}

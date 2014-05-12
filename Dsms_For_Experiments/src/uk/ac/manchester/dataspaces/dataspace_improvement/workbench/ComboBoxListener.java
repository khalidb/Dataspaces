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
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluation;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluationPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdate;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdatePostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;

public class ComboBoxListener implements ItemListener {
	
	Workbench wb = null;
	String integration_query = null;
	FeedbackUpdate fu = null;

	
	ComboBoxListener(Workbench _wb, String _integration_query){
		super();
		this.wb = _wb;
		this.integration_query = _integration_query;
		this.fu = new FeedbackUpdatePostgres(this.wb.source);
	}


	@Override
	public void itemStateChanged(ItemEvent evt) {
		// TODO Auto-generated method stub
		
        // Get the affected item
        Object item = evt.getItem();
        int row;

        if (evt.getStateChange() == ItemEvent.SELECTED) {
        	String[] values = new String[this.wb.results_table.getColumnCount() - 2];
        	String[] attributes = new String[this.wb.results_table.getColumnCount() - 2];
        	System.out.println("User Feedback: "+evt.getItem().toString());
        	row = this.wb.results_table.getSelectedRow();
        	System.out.println("Row: "+row);
        	System.out.println("Tuple");
        	if (this.wb.results_table.getColumnCount() > 2)
        		for (int i=0;i<this.wb.results_table.getColumnCount()-2;i++) {
        			values[i] = this.wb.results_table.getValueAt(row, i).toString();
        			attributes[i] = this.wb.results_table.getColumnName(i);
        		}
        	String user_fd = this.wb.results_table.getCellEditor(row, this.wb.results_table.getColumnCount() - 1).getCellEditorValue().toString();
        	
        	if (user_fd.equals("yes")) {
        		this.fu.insertTP(this.integration_query,values,attributes);
        	}
        	
        	if (user_fd.equals("no")) {
            	final String i_q = this.integration_query;
            	final String mapping_id = this.wb.results_table.getValueAt(row, this.wb.results_table.getColumnCount() - 2).toString();
        		final String[] _values = values;
        		final String[] _attributes = attributes;
        		final Jdbc3PoolingDataSource _source = this.wb.source;
        		SwingUtilities.invokeLater(new Runnable() {
        			public void run() {
        				JFrame frame = new JFrame();
        				frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        				FPFeedbackDialog inst = new FPFeedbackDialog(frame,_values,_attributes,i_q,mapping_id,_source);
        				inst.setVisible(true);
        				inst.setListValues(_values);
        			}
        		});        
        	}
        }
		
	}


}

package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.ListModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.ConnectionManager;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluation;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryEvaluationPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdate;
import uk.ac.manchester.dataspaces.dataspace_improvement.feedbackmanagement.FeedbackUpdatePostgres;


/**
* This code was edited or generated using CloudGarden's Jigloo
* SWT/Swing GUI Builder, which is free for non-commercial
* use. If Jigloo is being used commercially (ie, by a corporation,
* company or business for any purpose whatever) then you
* should purchase a license for each developer using Jigloo.
* Please visit www.cloudgarden.com for details.
* Use of Jigloo implies acceptance of these licensing terms.
* A COMMERCIAL LICENSE HAS NOT BEEN PURCHASED FOR
* THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED
* LEGALLY FOR ANY CORPORATE OR COMMERCIAL PURPOSE.
*/
public class FPFeedbackDialog extends javax.swing.JDialog {
	private JPanel jPanel1;
	private JList jList1;
	private JScrollPane jScrollPane1;
	private JButton moreButton;
	private String[] values;
	private JButton doneButton;
	private JPanel jPanel3;
	private JPanel jPanel2;
	private JRadioButton incorrCombButton;
	private JRadioButton incorrAttValueButton;
	private ButtonGroup buttonGroup1;
	private String mapping_id = null; 
	private String integration_query = null;
	private FeedbackUpdate fu = null;
	private String[] attributes = null;

	/**
	* Auto-generated main method to display this JDialog
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				JFrame frame = new JFrame();
				Jdbc3PoolingDataSource source = ConnectionManager.getSource();
				frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
				FPFeedbackDialog inst = new FPFeedbackDialog(frame,null,null,null,null,source);
				inst.setVisible(true);
				inst.setListValues(new String[] {"one","two","three","four"});
				source.close();
			}
		});
	}
	
	public FPFeedbackDialog(JFrame frame, String[] _values, String[] _attributes, String _integration_query, String _mapping_id, Jdbc3PoolingDataSource source) {
		super(frame);
		initGUI();
		frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		this.mapping_id = _mapping_id;
		this.integration_query = _integration_query;
		this.values = _values;
		this.attributes = _attributes;
		this.fu = new FeedbackUpdatePostgres(source);

	}
	
	private void initGUI() {
		try {
			{
				this.setPreferredSize(new java.awt.Dimension(359, 312));
			}
			{
				jPanel1 = new JPanel();
				BorderLayout jPanel1Layout1 = new BorderLayout();
				getContentPane().add(jPanel1, BorderLayout.NORTH);
				getContentPane().add(getJPanel2(), BorderLayout.CENTER);
				getContentPane().add(getJPanel3(), BorderLayout.SOUTH);
				jPanel1.setLayout(jPanel1Layout1);
				jPanel1.setPreferredSize(new java.awt.Dimension(392, 45));
				{
					incorrAttValueButton = new JRadioButton();
					jPanel1.add(incorrAttValueButton, BorderLayout.CENTER);
					incorrAttValueButton.setText("Incorrect Attribute Value");
				}
				{
					incorrCombButton = new JRadioButton();
					jPanel1.add(incorrCombButton, BorderLayout.NORTH);
					incorrCombButton.setText("Incorrect Combination of Attribute Values");
				}
				if(buttonGroup1 == null) {
					buttonGroup1 = new ButtonGroup();
					buttonGroup1.add(incorrCombButton);
					buttonGroup1.add(incorrAttValueButton);
				}
			}
			this.setSize(359, 312);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private JPanel getJPanel2() {
		if(jPanel2 == null) {
			jPanel2 = new JPanel();
			jPanel2.add(getJScrollPane1());
		}
		return jPanel2;
	}
	
	private JPanel getJPanel3() {
		if(jPanel3 == null) {
			jPanel3 = new JPanel();
			jPanel3.setPreferredSize(new java.awt.Dimension(282, 36));
			jPanel3.add(getDone());
			jPanel3.add(getMore());
		}
		return jPanel3;
	}
	
	private JButton getDone() {
		if(doneButton == null) {
			doneButton = new JButton();
			doneButton.setText("Done");
			doneButton.setPreferredSize(new java.awt.Dimension(93, 21));
			doneButton.addActionListener(new ButtonListener(this));
		}
		return doneButton;
	}
	
	private JButton getMore() {
		if(moreButton == null) {
			moreButton = new JButton();
			moreButton.setText("More >>");
			moreButton.setPreferredSize(new java.awt.Dimension(93, 21));
			moreButton.addActionListener(new ButtonListener(this));
		}
		return moreButton;
	}
	
	public void setListValues(String[] _values){
		this.values = _values;
		ListModel jList1Model = 
			new DefaultComboBoxModel(values);
		jList1.setModel(jList1Model);
		
	}
	
	private JList getJList1() {
		if(jList1 == null) {
			ListModel jList1Model = 
				new DefaultComboBoxModel(
						new String[] { "", "" });
			jList1 = new JList();
			jList1.setModel(jList1Model);
			jList1.setPreferredSize(new java.awt.Dimension(241, 171));
		}
		return jList1;
	}
	
	private JScrollPane getJScrollPane1() {
		if(jScrollPane1 == null) {
			jScrollPane1 = new JScrollPane();
			jScrollPane1.setPreferredSize(new java.awt.Dimension(239, 171));
			jScrollPane1.setViewportView(getJList1());
		}
		return jScrollPane1;
	}

public class ButtonListener implements ActionListener {
		
	FPFeedbackDialog d = null;
	ButtonListener(FPFeedbackDialog _d){
		super();
		this.d = _d;
	}

	public void actionPerformed(ActionEvent e) {
		
		if (e.getSource().equals(d.doneButton)) {
			this.getFeedback(e);
			this.d.dispose();
		}
		
		if (e.getSource().equals(d.moreButton)) {
			System.out.println("More Button");
			this.getFeedback(e);
			this.d.jList1.clearSelection();			
		}
	
	}
	
	private void getFeedback(ActionEvent e){
			if (this.d.incorrAttValueButton.isSelected()) {
				
				int[] indices = this.d.jList1.getSelectedIndices();
				if (indices.length > 0) {
					for (int i =0;i<indices.length;i++) {
						String[] atts = new String[1];
						String[] vals = new String[1];
						atts[0] = attributes[indices[i]];
						vals[0] = ((ListModel)this.d.jList1.getModel()).getElementAt(indices[i]).toString();
						fu.insertFP(integration_query, vals, atts);
					}
				}
				
			}
			if (this.d.incorrCombButton.isSelected()) {
				
				int[] indices = this.d.jList1.getSelectedIndices();
				String[] atts = new String[indices.length];
				String[] vals = new String[indices.length];
				if (indices.length > 0) {
					for (int i =0;i<indices.length;i++) {
						atts[i] = attributes[indices[i]];
						vals[i] = ((ListModel)this.d.jList1.getModel()).getElementAt(indices[i]).toString();
					}
					fu.insertFP(integration_query, vals, atts);
				}
			} 
	}
	
}
}
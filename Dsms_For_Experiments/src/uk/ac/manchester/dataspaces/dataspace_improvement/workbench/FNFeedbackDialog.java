package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

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
public class FNFeedbackDialog extends javax.swing.JDialog {
	private JPanel jPanel1;
	private JTable fn_table;
	private JScrollPane jScrollPane1;
	private JLabel jLabel1;
	private JButton moreButton;
	private String[] columns;
	private String[] tuple;
	private JButton doneButton;
	private JPanel jPanel3;
	private JPanel jPanel2;
	private ButtonGroup buttonGroup1;
	private String[] attribute_names = null;
	private String integration_query = null;
	Jdbc3PoolingDataSource source = null;

	/**
	* Auto-generated main method to display this JDialog
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				JFrame frame = new JFrame();
				Jdbc3PoolingDataSource source = ConnectionManager.getSource();
				frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
				FNFeedbackDialog inst = new FNFeedbackDialog(frame,null,new String[] { "one", "two","three" },source);
				inst.setVisible(true);
				inst.setColumnNames(new String[] {"att1","att2","att3","att4"});
				source.close();
			}
		});
	}
	
	public FNFeedbackDialog(JFrame frame, String i_query,String[] _attribute_names,Jdbc3PoolingDataSource _source) {
		super(frame);
		initGUI();
		this.source = _source;
		frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		this.attribute_names = _attribute_names;
		this.integration_query = i_query;
		this.setColumnNames(attribute_names);
	}
	
	private void initGUI() {
		try {
			{
				this.setPreferredSize(new java.awt.Dimension(541, 172));
			}
			{
				jPanel1 = new JPanel();
				FlowLayout jPanel1Layout1 = new FlowLayout();
				getContentPane().add(getJPanel2(), BorderLayout.CENTER);
				getContentPane().add(getJPanel3(), BorderLayout.SOUTH);
				getContentPane().add(jPanel1, BorderLayout.NORTH);
				jPanel1.setLayout(jPanel1Layout1);
				jPanel1.setPreferredSize(new java.awt.Dimension(392, 45));
				jPanel1.setEnabled(false);
				jPanel1.setForeground(new java.awt.Color(0,0,255));
				jPanel1.add(getJLabel1());
				if(buttonGroup1 == null) {
					buttonGroup1 = new ButtonGroup();
				}
			}
			this.setSize(541, 172);
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
			moreButton.setText("Another Tuple");
			moreButton.setPreferredSize(new java.awt.Dimension(134, 21));
			moreButton.addActionListener(new ButtonListener(this));
		}
		return moreButton;
	}
	
	public void setColumnNames(String[] _columns){
		this.columns = _columns;
		TableModel fn_tableModel = 
				new DefaultTableModel(new String[1][1],this.columns);
		fn_table.setModel(fn_tableModel);
	}
	
	private JTable getfn_table() {
		if(fn_table == null) {
			fn_table = new JTable();
			fn_table.setPreferredSize(new java.awt.Dimension(447, 39));
		}
		return fn_table;
	}
	
	private JScrollPane getJScrollPane1() {
		if(jScrollPane1 == null) {
			jScrollPane1 = new JScrollPane();
			jScrollPane1.setPreferredSize(new java.awt.Dimension(465, 39));
			jScrollPane1.setViewportView(getfn_table());
		}
		return jScrollPane1;
	}
	
	private JLabel getJLabel1() {
		if(jLabel1 == null) {
			jLabel1 = new JLabel();
			FlowLayout jLabel1Layout = new FlowLayout();
			jLabel1.setLayout(jLabel1Layout);
			jLabel1.setText("Use the table below to specify a tuple you expect to appear in the results");
			jLabel1.setForeground(new java.awt.Color(0,64,128));
			jLabel1.setOpaque(true);
			jLabel1.setFont(new java.awt.Font("Tahoma",1,12));
			jLabel1.setPreferredSize(new java.awt.Dimension(488, 29));
		}
		return jLabel1;
	}

public class ButtonListener implements ActionListener {
	
	private FeedbackUpdate fu = null;
	FNFeedbackDialog d = null;
	
	ButtonListener(FNFeedbackDialog _d){
		super();
		this.d = _d;
		this.fu = new FeedbackUpdatePostgres(_d.source);
	}

	public void actionPerformed(ActionEvent e) {
		
		if (e.getSource().equals(d.doneButton)) {
			this.getFeedback();
			this.d.dispose();
		}
		
		if (e.getSource().equals(d.moreButton)) {
			this.getFeedback();
			for(int i=0;i<this.d.fn_table.getColumnCount();i++) 
				this.d.fn_table.setValueAt("",0, i);
		}
		
	}
	
	private void getFeedback(){
		String[] attribute_values = new String[this.d.attribute_names.length];
		for(int i=0;i<this.d.fn_table.getColumnCount();i++) {
			attribute_values[i] = this.d.fn_table.getValueAt(0, i).toString();
		}
		this.fu.insertFN(this.d.integration_query, attribute_values, attribute_names);
	}
}
}
package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.awt.BorderLayout;
import java.awt.Color;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultCellEditor;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextPane;
import javax.swing.ListModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.border.BevelBorder;
import javax.swing.border.LineBorder;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.ConnectionManager;


public class Workbench extends javax.swing.JFrame {

	{
		//Set Look & Feel
		try {
			javax.swing.UIManager.setLookAndFeel("com.jgoodies.looks.plastic.Plastic3DLookAndFeel");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}


	public Vector mappings;
	private JMenu aboutMenuItem;
	private JMenu contentsMenuItem;
	private ComboTableCellRenderer renderer; 
	private JMenu helpMenuItem;
	public JMenuItem exitMenuItem;
	private JMenu closeFileMenuItem;
	private JMenu saveFileMenuItem;
	private JMenu newFileMenuItem;
	private JMenu openFileMenuItem;
	private JMenu fileMenuItem;
	private JMenuBar menu1;
	public JButton prunmaps;
	public JButton refineMappings;
	public JButton generalMappingPruning;
	public JButton generateAFalseNegative;
	public JButton generateNegativeAttributeFeedback;
	public JButton generateNegativeTupleFeedback;
	public JButton generatePositiveFeedback;
	private JScrollPane jScrollPane1;
	public JTable pruning_results_table;
	private JPanel pruningResultsTable;
	public JComboBox pruningResultsID;
	private JLabel jLabel5;
	private JLabel jLabel4;
	public JComboBox integrationQueries;
	public TableModel pruning_results_tableModel;
	private JPanel jPanel11;
	private JPanel MappingPruningPanel;
	private JTabbedPane jTabbedPane1;
	public JMenuItem saveResults;
	public JMenuItem exportPruningResults;
	public JMenuItem reinitialiseDS;
	public JMenuItem initialiseLog;
	public JMenuItem pruneMappings;
	public JMenuItem annotateMappingsCorrectly;
	public JMenuItem updateLog;
	public JMenuItem reinitialisePruningResults;
	private JPanel jPanel7;
	public JButton jButton2;
	private JPanel jPanel10;
	private JScrollPane jPanel9;
	private JScrollPane jScrollPane5;
	private JScrollPane jScrollPane4;
	public JMenuItem generateMappings;
	public JComboBox comboBox;
	public JMenuItem retrieveMappings;
	private JScrollPane jScrollPane3;
	private JLabel jLabel3;
	private JPanel jPanel8;
	public JTextPane mappingDescriptionArea;
	public JList candidateMappingsList;
	public String[] candidateMappingsIds;
	private JPanel jScrollPane2;
	private JLabel jLabel2;
	public JTable results_table;
	private JLabel jLabel1;
	private JPanel jPanel6;
	private JPanel jPanel5;
	public JButton jButton1;
	public JTextArea jTextArea1;
	private JLabel Label1;
	private JPanel jPanel4;
	private JPanel jPanel3;
	private JPanel jPanel2;
	private JPanel jPanel1;
	public String integration_query = null;
	public ResultsSet results = null;
	String[] integration_queries = null;
	public Jdbc3PoolingDataSource source;

	public Workbench() {

		integration_queries = new String[2];
		integration_queries[0]="";
		integration_queries[1] = "integration.europeancities";
		this.source = ConnectionManager.getSource();
		initGUI();
	
	}
	
	/**
	* Initializes the GUI.
	*/
	private void initGUI() {
		try {
			BorderLayout thisLayout = new BorderLayout();
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			getContentPane().setLayout(thisLayout);
			this.setSize(963, 560);
			{
				jPanel1 = new JPanel();
				BorderLayout jPanel1Layout = new BorderLayout();
				jPanel1.setLayout(jPanel1Layout);
				getContentPane().add(jPanel1, BorderLayout.CENTER);
				jPanel1.setPreferredSize(new java.awt.Dimension(729, 404));
				{
					jPanel2 = new JPanel();
					BorderLayout jPanel2Layout = new BorderLayout();
					jPanel2.setLayout(jPanel2Layout);
					jPanel1.add(jPanel2, BorderLayout.CENTER);
					{
						jPanel4 = new JPanel();
						jPanel2.add(jPanel4, BorderLayout.NORTH);
						BorderLayout jPanel4Layout = new BorderLayout();
						jPanel4.setLayout(jPanel4Layout);
						jPanel4.setPreferredSize(new java.awt.Dimension(518, 87));
						{
							Label1 = new JLabel();
							BorderLayout Label1Layout = new BorderLayout();
							Label1.setLayout(null);
							jPanel4.add(Label1, BorderLayout.NORTH);
							Label1.setText("     Query");
							Label1.setBackground(new java.awt.Color(128,128,255));
							Label1.setOpaque(true);
							Label1.setFont(new java.awt.Font("Tahoma",1,11));
							Label1.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
							Label1.setForeground(new java.awt.Color(255,255,255));
						}
						{
							jTextArea1 = new JTextArea();
							jPanel4.add(jTextArea1, BorderLayout.CENTER);
							jTextArea1.setText("Type your query here");
							jTextArea1.setBackground(new java.awt.Color(255,255,255));
							jTextArea1.setForeground(new java.awt.Color(0,0,160));
							jTextArea1.setBorder(BorderFactory.createEtchedBorder(BevelBorder.RAISED, new java.awt.Color(128,0,128), new java.awt.Color(192,192,192)));
							jTextArea1.setFont(new java.awt.Font("Tahoma",0,12));
						}
						{
							jPanel5 = new JPanel();
							BorderLayout jPanel5Layout = new BorderLayout();
							jPanel5.setLayout(jPanel5Layout);
							jPanel4.add(jPanel5, BorderLayout.SOUTH);
							{
								jButton1 = new JButton();
								jPanel5.add(getJButton1(), BorderLayout.EAST);
								jButton1.setText(" Run Query");
								jButton1.setBackground(new java.awt.Color(164,216,219));
								jButton1.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
								jButton1.setFont(new java.awt.Font("Tahoma",1,11));
								jButton1.setPreferredSize(new java.awt.Dimension(92, 18));
								jButton1.addActionListener(new ButtonListener(this));
							}
						}
					}
					{
						jPanel6 = new JPanel();
						BorderLayout jPanel6Layout = new BorderLayout();
						jPanel6.setLayout(jPanel6Layout);
						jPanel2.add(jPanel6, BorderLayout.SOUTH);
						jPanel6.setPreferredSize(new java.awt.Dimension(595, 388));
						{
							jLabel1 = new JLabel();
							jPanel6.add(jLabel1, BorderLayout.NORTH);
							jLabel1.setText("  Query Results");
							jLabel1.setBackground(new java.awt.Color(128,128,255));
							jLabel1.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
							jLabel1.setFont(new java.awt.Font("Tahoma",1,11));
							jLabel1.setOpaque(true);
							jLabel1.setForeground(new java.awt.Color(255,255,255));
						}
						{
							jPanel10 = new JPanel();
							jPanel6.add(jPanel10, BorderLayout.CENTER);
							jPanel10.setPreferredSize(new java.awt.Dimension(592, 299));
							{
								jScrollPane3 = new JScrollPane();
								jPanel10.add(jScrollPane3);
								jScrollPane3.setPreferredSize(new java.awt.Dimension(586, 364));
								jScrollPane3.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
								{
									TableModel results_tableModel = 
										new DefaultTableModel(
												new String[][] { { "", "" }, { "", "" } },
												new String[] { "", "" });
									results_table = new JTable();
									jScrollPane3.setViewportView(results_table);
									results_table.setShowVerticalLines(false);
									//results_table.setPreferredSize(new java.awt.Dimension(566, 273));
									results_table.setModel(results_tableModel);
								    String choices[] = { "", "yes", "no"};
									renderer = new ComboTableCellRenderer();
									comboBox = new JComboBox(choices);
									comboBox.setRenderer(renderer);
								}
								jScrollPane3.setFocusCycleRoot(true);
								jScrollPane3.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
							}
						}
						{
							jPanel7 = new JPanel();
							BorderLayout jPanel7Layout = new BorderLayout();
							jPanel7.setLayout(jPanel7Layout);
							jPanel6.add(jPanel7, BorderLayout.SOUTH);
							jPanel7.setPreferredSize(new java.awt.Dimension(655, 21));
							{
								jButton2 = new JButton();
								jPanel7.add(jButton2, BorderLayout.EAST);
								jButton2.setText("Any results that you would have expected? ");
								jButton2.setPreferredSize(new java.awt.Dimension(327, 21));
								jButton2.setBackground(new java.awt.Color(0,255,255));
								jButton2.setFont(new java.awt.Font("Tahoma",1,12));
								jButton2.addActionListener(new ButtonListener(this));
							}
						}
					}
				}
				{
					jPanel3 = new JPanel();
					BorderLayout jPanel3Layout = new BorderLayout();
					jPanel3.setLayout(jPanel3Layout);
					jPanel1.add(jPanel3, BorderLayout.EAST);
					jPanel3.setPreferredSize(new java.awt.Dimension(360, 476));
					{
						jLabel2 = new JLabel();
						jPanel3.add(jLabel2, BorderLayout.NORTH);
						jLabel2.setText("    Candidate Mappings");
						jLabel2.setBackground(new java.awt.Color(128,128,255));
						jLabel2.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
						jLabel2.setFont(new java.awt.Font("Tahoma",1,11));
						jLabel2.setOpaque(true);
						jLabel2.setForeground(new java.awt.Color(255,255,255));
					}
					{
						jScrollPane2 = new JPanel();
						BorderLayout jScrollPane2Layout = new BorderLayout();
						jScrollPane2.setLayout(jScrollPane2Layout);
						jPanel3.add(jScrollPane2, BorderLayout.CENTER);
						jScrollPane2.setPreferredSize(new java.awt.Dimension(211, 398));
						{
							jScrollPane4 = new JScrollPane();
							jScrollPane2.add(jScrollPane4, BorderLayout.CENTER);
							{
								jTabbedPane1 = new JTabbedPane();
								jScrollPane4.setViewportView(jTabbedPane1);
								jTabbedPane1.setPreferredSize(new java.awt.Dimension(297, 339));
								{
									ListModel jList1Model = 
										new DefaultComboBoxModel(
												new String[] { "" });
									candidateMappingsList = new JList();
									jTabbedPane1.addTab("List of mappings", null, candidateMappingsList, null);
									candidateMappingsList.setModel(jList1Model);
									candidateMappingsList.setBorder(new LineBorder(new java.awt.Color(0,0,0), 1, false));
								}
							}
						}
						{
						jPanel8 = new JPanel();
							BorderLayout jPanel8Layout = new BorderLayout();
							jPanel8.setLayout(jPanel8Layout);
							jScrollPane2.add(jPanel8, BorderLayout.SOUTH);
							jPanel8.setPreferredSize(new java.awt.Dimension(211, 116));
							{
								jLabel3 = new JLabel();
								jPanel8.add(jLabel3, BorderLayout.NORTH);
								jLabel3.setText("  Specification of the Selected Mapping");
								jLabel3.setBackground(new java.awt.Color(128,128,255));
								jLabel3.setBorder(BorderFactory.createEtchedBorder(BevelBorder.LOWERED));
								jLabel3.setFont(new java.awt.Font("Tahoma",1,11));
								jLabel3.setOpaque(true);
								jLabel3.setForeground(new java.awt.Color(255,255,255));
							}
							{
								jPanel9 = new JScrollPane();
								jPanel8.add(jPanel9, BorderLayout.CENTER);
								{
									
										mappingDescriptionArea = new JTextPane();
										mappingDescriptionArea.setText("");
										jPanel9.setViewportView(mappingDescriptionArea);
										//candidateMappingsList.setModel(jList1Model);
										candidateMappingsList.setBorder(new LineBorder(new java.awt.Color(0,0,0), 1, false));
										{
											MappingPruningPanel = new JPanel();
											BorderLayout MappingPruningPanelLayout = new BorderLayout();
											MappingPruningPanel.setLayout(MappingPruningPanelLayout);
											jTabbedPane1.addTab("Pruning Results", null, getMappingPruningPanel(), null);
											{
												jPanel11 = new JPanel();
												MappingPruningPanel.add(jPanel11, BorderLayout.NORTH);
												jPanel11.setPreferredSize(new java.awt.Dimension(292, 53));
												{
													jLabel4 = new JLabel();
													jPanel11.add(jLabel4);
													jLabel4.setText("Integration Relation");
													jLabel4.setPreferredSize(new java.awt.Dimension(96, 14));
												}
												{
													ComboBoxModel integrationQueriesModel = 
														new DefaultComboBoxModel(integration_queries);
													integrationQueries = new JComboBox();
													integrationQueries.addItemListener(new PruningComboBoxListener(this));
													jPanel11.add(integrationQueries);
													integrationQueries.setModel(integrationQueriesModel);
													integrationQueries.setPreferredSize(new java.awt.Dimension(151, 21));
												}
												{
													jLabel5 = new JLabel();
													jPanel11.add(jLabel5);
													jLabel5.setText("Pruning Results ID");
												}
												{
													ComboBoxModel pruningResultsIDModel = 
														new DefaultComboBoxModel(
																new String[] {});
													pruningResultsID = new JComboBox();
													jPanel11.add(getPruningResultsID());
													pruningResultsID.setModel(pruningResultsIDModel);
													pruningResultsID.setPreferredSize(new java.awt.Dimension(148, 21));
												}
											}
											{
												pruningResultsTable = new JPanel();
												MappingPruningPanel.add(pruningResultsTable, BorderLayout.CENTER);
												{
													jScrollPane1 = new JScrollPane();
													pruningResultsTable.add(jScrollPane1);
													jScrollPane1.setPreferredSize(new java.awt.Dimension(274, 244));
													{
														pruning_results_tableModel = 
															new DefaultTableModel(
																	new String[][] { { "", "","" }, { "", "", "" } },
																	new String[] { "mapping", "precision", "recall", "F measure" , "annotated results" });
														pruning_results_table = new JTable();
														jScrollPane1.setViewportView(getPruning_results_table());
														pruning_results_table.setModel(pruning_results_tableModel);
														pruning_results_table.setPreferredSize(new java.awt.Dimension(274, 244));
													}
												}
											}
										}
										
								}
							}
						}
					}
				}
			}
			{
				menu1 = new JMenuBar();
				setJMenuBar(menu1);
				{
					fileMenuItem = new JMenu();
					menu1.add(fileMenuItem);
					fileMenuItem.setText("Tools");
					{
						reinitialiseDS = new JMenuItem();
						fileMenuItem.add(getReinitialiseDS());
						reinitialiseDS.setText("Reinitialise Dataspace");
					}
					{
						initialiseLog = new JMenuItem();
						fileMenuItem.add(getInitialiseLog());
						initialiseLog.setText("Reinitialise Log");
					}
					{
						saveResults = new JMenuItem();
						fileMenuItem.add(getSaveResults());
						saveResults.setText("Save Query Results");
					}
					{
						retrieveMappings = new JMenuItem();
						fileMenuItem.add(getRetrieveMappings());
						retrieveMappings.setText("Retrieve Mappings");
					}
					{
						generateMappings = new JMenuItem();
						fileMenuItem.add(getGenerateMappings());
						generateMappings.setText("Generate Mappings");
					}
					{
						pruneMappings = new JMenuItem();
						fileMenuItem.add(getPruneMappings());
						pruneMappings.setText("Prune Mappings");
					}
					{
						updateLog = new JMenuItem();
						fileMenuItem.add(getUpdateLog());
						updateLog.setText("Update Log");
					}
					{
						reinitialisePruningResults = new JMenuItem();
						fileMenuItem.add(getReinitialisePruningResults());
						reinitialisePruningResults.setText("Reinitialise Pruning Results");
					}
					{
						exportPruningResults = new JMenuItem();
						fileMenuItem.add(getExportPruningResults());
						exportPruningResults.setText("Export Pruning Results");
					}
					{
						annotateMappingsCorrectly = new JMenuItem();
						fileMenuItem.add(getAnnotateMappingsCorrectly());
						annotateMappingsCorrectly.setText("Annotate Mappings Based on Complete Knowledge");
					}
					{
						exitMenuItem = new JMenuItem();
						exitMenuItem.setText("Exit");
						exitMenuItem.addActionListener(new MenuItemListener(this));
						fileMenuItem.add(exitMenuItem);
					}

				}
				{
					helpMenuItem = new JMenu();
					menu1.add(helpMenuItem);
					helpMenuItem.setText("Help");
					{
						contentsMenuItem = new JMenu();
						helpMenuItem.add(contentsMenuItem);
						contentsMenuItem.setText("Contents");
					}
					{
						aboutMenuItem = new JMenu();
						helpMenuItem.add(aboutMenuItem);
						aboutMenuItem.setText("About");
					}
				}
				{
					generatePositiveFeedback = new JButton();
					//menu1.add(getGeneratePositiveFeedback());
					generatePositiveFeedback.setText("Generate Positive Feedback");
					generatePositiveFeedback.setPreferredSize(new java.awt.Dimension(181, 21));
					generatePositiveFeedback.addActionListener(new ButtonListener(this));
				}
				{
					generateNegativeTupleFeedback = new JButton();
					//menu1.add(getGenerateNegativeFeedback());
					generateNegativeTupleFeedback.setText("Generate Negative Tuple Feedback");
					generateNegativeTupleFeedback.addActionListener(new ButtonListener(this));
				}
				{
					generateNegativeAttributeFeedback = new JButton();
					//menu1.add(getGenerateNegativeAttributeFeedback());
					generateNegativeAttributeFeedback.setText("Generate Negative Attribute Feedback");
					generateNegativeAttributeFeedback.addActionListener(new ButtonListener(this));
				}
				{
					generateAFalseNegative = new JButton();
					//menu1.add(getGenerateAFalseNegative());
					generateAFalseNegative.setText("Generate A False Negative");
					generateAFalseNegative.addActionListener(new ButtonListener(this));
				}
				{
					generalMappingPruning = new JButton();
					menu1.add(getGeneralMappingPruning());
					generalMappingPruning.setText("generalMappingPruning");
					generalMappingPruning.addActionListener(new ButtonListener(this));
				}
				{
					refineMappings = new JButton();
					menu1.add(getRefineMappings());
					refineMappings.setText("Mapping Refinement");
					refineMappings.addActionListener(new ButtonListener(this));
				}
				{
					prunmaps = new JButton();
					menu1.add(getPrunmaps());
					prunmaps.setText("Prune Mappings");
					prunmaps.addActionListener(new ButtonListener(this));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void displayCandidateMappings() {

		 candidateMappingsIds = new String[mappings.size()];
		 for (int i = 0; i<mappings.size(); i++) {
			 candidateMappingsIds[i] = ((Integer) ((SchemaMapping) mappings.get(i)).getId()).toString();
		 }
		
		ListModel candidateMappingsListModel = 
			new DefaultComboBoxModel(candidateMappingsIds);
		candidateMappingsList.removeAll();
		candidateMappingsList.setModel(candidateMappingsListModel);
		candidateMappingsList.setFont(new java.awt.Font("Tahoma",0,12));
		candidateMappingsList.setForeground(new java.awt.Color(0,0,0));
		candidateMappingsList.setBackground(new java.awt.Color(255,255,255));
		candidateMappingsList.addListSelectionListener(new ListSelection(this));

		
	}
	
	public void displayCandidateMappings(Vector c_mappings) {
		
		this.mappings = c_mappings;
		 candidateMappingsIds = new String[mappings.size()];
		 for (int i = 0; i<mappings.size(); i++) {
			 candidateMappingsIds[i] = ((Integer) ((SchemaMapping) mappings.get(i)).getId()).toString();
		 }
		
		ListModel candidateMappingsListModel = 
			new DefaultComboBoxModel(candidateMappingsIds);
		candidateMappingsList.removeAll();
		candidateMappingsList.setModel(candidateMappingsListModel);
		candidateMappingsList.setFont(new java.awt.Font("Tahoma",0,12));
		candidateMappingsList.setForeground(new java.awt.Color(0,0,0));
		candidateMappingsList.setBackground(new java.awt.Color(255,255,255));
		candidateMappingsList.addListSelectionListener(new ListSelection(this));

		
	}	

	
	/**
	* Auto-generated main method to display this 
	* org.eclipse.swt.widgets.Composite inside a new Shell.
	*/
	/*
	public static void main(String[] args) {
		Display display = Display.getDefault();
		Shell shell = new Shell(display);
		workbench inst = new workbench(shell, SWT.NULL);
		shell.setLayout(new FillLayout());
		shell.layout();
			inst.pack();
			shell.pack();
			shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
	}
	*/
	
	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				Workbench inst = new Workbench();
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}
	
	public JButton getJButton1() {
		return jButton1;
	}
	
	public JMenuItem getGenerateMappings() {
		generateMappings.addActionListener(new MenuItemListener(this));
		return generateMappings;
	}

	public JMenuItem getRetrieveMappings() {
		retrieveMappings.addActionListener(new MenuItemListener(this));
		return retrieveMappings;
	}
	
	public void displayMappingDecription(int selectedItem) {
		
		SchemaMapping map = (SchemaMapping)  mappings.get(selectedItem);
		String description = "Integration relation: "+map.getIntegrationRelation()+"\nSource query: "+map.getS_query();
		this.mappingDescriptionArea.setText(description);
	}
	
	public void displayQueryResults(MappingResultSet rs) {
		
		

		String[] head = new String[rs.getHead().size()];
		for (int i=0;i<rs.getHead().size();i++)
			head[i] = rs.getHead().get(i).toString();
		//System.out.println("head\n"+head);
		
		String[][] body = new String[rs.getBody().size()][head.length];
		for (int i=0;i<rs.getBody().size();i++) {
			Vector row = (Vector) rs.getBody().get(i);
			for (int j=0;j<head.length;j++) {
				body[i][j] = row.get(j).toString();
			}
		}
		//System.out.println("body");
		//for (int i = 0;i< rs.getBody().size();i++)
		//	System.out.println(body[i]+"\n");
		
		TableModel results_tableModel = 
			new DefaultTableModel(body,head);
		this.results_table.setModel(results_tableModel);
		results_table.getTableHeader().setAutoscrolls(true);
		this.jScrollPane3.updateUI();
	}

	public void displayQueryResults(ResultsSet results) {
		
		this.results = results;	
		String[] head = new String[results.getHead().size()+2];
		for (int i=0;i<results.getHead().size();i++)
			head[i] = results.getHead().get(i).toString();
		head[head.length-1] = "exists";
		head[head.length-2] = "mappings";
		//System.out.println("head\n"+head);
		
		
		int number_of_tuples = 0;
		
		for (int i=0;i<results.getResults().size();i++) {
			number_of_tuples += ((MappingResultSet) results.getResults().get(i)).getBody().size();
			if (i ==0)
				integration_query = ((MappingResultSet) results.getResults().get(i)).getMap().getIntegrationRelation();
		}
	String[][] body = new String[number_of_tuples][head.length];
		
		int tuple_number = 0;
		for (int i=0;i<results.getResults().size();i++) {
			MappingResultSet map_res = (MappingResultSet) results.getResults().get(i);
			for (int j=0; j<map_res.getBody().size();j++){
				
				Vector row = (Vector) map_res.getBody().get(j);
				for (int k=0;k<head.length - 2;k++) {
					if (row.get(k) == null)
						body[tuple_number][k] = "";
					else	
						body[tuple_number][k] = row.get(k).toString();
				}
				body[tuple_number][head.length-2] = ((Integer) map_res.getMap().getId()).toString();
				tuple_number++;
			}
		}
		

		comboBox.addItemListener(new ComboBoxListener(this,integration_query));
		
		TableCellEditor editor = new DefaultCellEditor(comboBox);
		    
		TableModel results_tableModel = 
		//	new DefaultTableModel(body,head);
			new FeedbackTableModel(head,body);
		this.results_table.setModel(results_tableModel);
		//results_table.addColumn(arg0)
	    TableColumn column = results_table.getColumnModel().getColumn(head.length-1);
	    column.setCellRenderer(renderer);
	    column.setCellEditor(editor);
	    

		results_table.getTableHeader().setAutoscrolls(true);
		this.jScrollPane3.updateUI();
		
	}
	
	public JMenuItem getPruneMappings() {
		pruneMappings.addActionListener(new MenuItemListener(this));
		return pruneMappings;
	}
	
	public JMenuItem getAnnotateMappingsCorrectly() {
		annotateMappingsCorrectly.addActionListener(new MenuItemListener(this));
		return annotateMappingsCorrectly;
	}
	
	public JMenuItem getUpdateLog() {
		updateLog.addActionListener(new MenuItemListener(this));
		return updateLog;
	}
	
	public JMenuItem getReinitialiseDS() {
		reinitialiseDS.addActionListener(new MenuItemListener(this));
		return reinitialiseDS;
	}

	public JMenuItem getInitialiseLog() {
		initialiseLog.addActionListener(new MenuItemListener(this));
		return initialiseLog;
	}

	public JMenuItem getReinitialisePruningResults() {
		reinitialisePruningResults.addActionListener(new MenuItemListener(this));
		return reinitialisePruningResults;
	}


	public JMenuItem getSaveResults() {
		saveResults.addActionListener(new MenuItemListener(this));
		return saveResults;
	}
	
	public JMenuItem getExportPruningResults() {
		exportPruningResults.addActionListener(new MenuItemListener(this));
		return exportPruningResults;
	}
	
	public JPanel getMappingPruningPanel() {
		return MappingPruningPanel;
	}
	
	public JComboBox getIntegrationQueries() {
		return integrationQueries;
	}
	
	public JComboBox getPruningResultsID() {
		pruningResultsID.addItemListener(new PruningComboBoxListener(this));
		return pruningResultsID;
	}
	
	public JTable getPruning_results_table() {
		return pruning_results_table;
	}
	
	public JButton getGeneratePositiveFeedback() {
		return generatePositiveFeedback;
	}
	
	public JButton getGenerateNegativeFeedback() {
		return generateNegativeTupleFeedback;
	}
	
	public JButton getGenerateNegativeAttributeFeedback() {
		return generateNegativeAttributeFeedback;
	}
	
	public JButton getGenerateAFalseNegative() {
		return generateAFalseNegative;
	}
	
	public JButton getGeneralMappingPruning() {
		return generalMappingPruning;
	}
	
	public JButton getRefineMappings() {
		return refineMappings;
	}
	
	public JButton getPrunmaps() {
		return prunmaps;
	}

}

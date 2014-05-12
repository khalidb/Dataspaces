package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.awt.Color;

import javax.swing.table.AbstractTableModel;

class FeedbackTableModel extends AbstractTableModel {

	  Object rowData[][] = null;

	  String columnNames[] = null;

	  FeedbackTableModel(String[] head,String[][] body) {
		  
		  columnNames = head;
		  rowData = body;
	  }
	  
	  public int getColumnCount() {
	    return columnNames.length;
	  }

	  
	  public String getColumnName(int column) {
	    return columnNames[column];
	  }

	  public int getRowCount() {
	    return rowData.length;
	  }

	  public Object getValueAt(int row, int column) {
	    return rowData[row][column];
	  }

	  public Class getColumnClass(int column) {
		  Object obj = new Object();
		  if (getValueAt(0, column) == null)
			  return obj.getClass();
		  else	  
			  return (getValueAt(0, column).getClass());
	  }

	  public void setValueAt(Object value, int row, int column) {
	    rowData[row][column] = value;
	  }

	  public boolean isCellEditable(int row, int column) {
	    return (column != 0);
	  }
	}

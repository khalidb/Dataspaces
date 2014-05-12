package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;

import javax.swing.DefaultCellEditor;
import javax.swing.DefaultListCellRenderer;
import javax.swing.ImageIcon;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListCellRenderer;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;

class ComboTableCellRenderer implements ListCellRenderer, TableCellRenderer {
  DefaultListCellRenderer listRenderer = new DefaultListCellRenderer();

  DefaultTableCellRenderer tableRenderer = new DefaultTableCellRenderer();

  private void configureRenderer(JLabel renderer, Object value) {
    if ((value != null) && (value instanceof Color)) {
      renderer.setText(value.toString());
      if (value.equals("yes"))
    	  renderer.setBackground(Color.GREEN);
      if (value.equals("no"))
    	  renderer.setBackground(Color.RED);
    } else {
      renderer.setText((String) value);
    }
  }

  public Component getListCellRendererComponent(JList list, Object value, int index,
      boolean isSelected, boolean cellHasFocus) {
    listRenderer = (DefaultListCellRenderer) listRenderer.getListCellRendererComponent(list, value,
        index, isSelected, cellHasFocus);
    configureRenderer(listRenderer, value);
    return listRenderer;
  }

  public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
      boolean hasFocus, int row, int column) {
    tableRenderer = (DefaultTableCellRenderer) tableRenderer.getTableCellRendererComponent(table,
        value, isSelected, hasFocus, row, column);
    configureRenderer(tableRenderer, value);
    return tableRenderer;
  }
}

class ColorTableModel extends AbstractTableModel {

  Object rowData[][] = { { "1", Color.RED },
      { "2", Color.BLUE },
      { "3", Color.GREEN },
      { "4", Color.MAGENTA },
      { "5", Color.PINK }, };

  String columnNames[] = { "English","Color" };

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
    return (getValueAt(0, column).getClass());
  }

  public void setValueAt(Object value, int row, int column) {
    rowData[row][column] = value;
  }

  public boolean isCellEditable(int row, int column) {
    return (column != 0);
  }
}


package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import javax.swing.JFrame;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

public class ListSelection implements ListSelectionListener {

	Workbench wb;
	ListSelection(Workbench _wb){
		super();
		this.wb = _wb;
	}
	public void valueChanged(ListSelectionEvent e) {
		
		if (e.getSource().equals(this.wb.candidateMappingsList)) {
			if (!e.getValueIsAdjusting()){
				int selectedItem = wb.candidateMappingsList.getSelectedIndex();
				if (selectedItem != -1) {
					wb.displayMappingDecription(selectedItem);
				}
				else
					wb.mappingDescriptionArea.setText("");
			}
		}

		
	}

}

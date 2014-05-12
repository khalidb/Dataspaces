package uk.ac.manchester.dataspaces.dataspace_improvement.workbench;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;

import javax.swing.JMenuItem;

import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagement;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.QueryResultManagementPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrieval;
import uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation.SchemaMappingRetrievalPostgres;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappinggeneration.GenerateMappings;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappinggeneration.GenerateMappingsMySQL;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappings;
import uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement.PruneMappingsPostgres;

public class MenuItemListener implements ActionListener {

	Workbench wb = null;
	GenerateMappings gm = null;
	SchemaMappingRetrieval smr = null;
	PruneMappings pm = null;
	QueryResultManagement qrm = null;
	
	MenuItemListener(Workbench _wb){
		super();
		this.wb = _wb;
		this.gm = new GenerateMappingsMySQL();
		this.smr = new SchemaMappingRetrievalPostgres(this.wb.source);
		this.pm = new PruneMappingsPostgres(this.wb.source);
		this.qrm = new QueryResultManagementPostgres(this.wb.source);
	}
	
	public void actionPerformed(ActionEvent e) {
		
		System.out.println("event source: "+e.getSource());
		
		if (e.getSource().equals(this.wb.generateMappings)) {
			
			System.out.println("Mappings generation invoked");
			gm.generateMappingsRandomly();
			Vector mappings = gm.getMappings();
			this.wb.mappings = mappings;
			this.wb.displayCandidateMappings();
			System.out.println("Mappings generation terminated");
			
		}

		if (e.getSource().equals(this.wb.retrieveMappings)) {
			
			System.out.println("Mappings retrieval invoked");
			Vector mappings = smr.getCandidateMappings();
			this.wb.mappings = mappings;
			this.wb.displayCandidateMappings();
			System.out.println("Mappings retrieval terminated");
			
		}

		if (e.getSource().equals(this.wb.pruneMappings)) {
			
			System.out.println("Mappings pruning invoked");
			this.pm.identifyTP(this.wb.integration_query);
			this.pm.identifyFP(this.wb.integration_query);
			this.pm.identifyResultWithoutFeedback(this.wb.integration_query);
			this.pm.order_tp(this.wb.integration_query);
			this.pm.annotateMappings(this.wb.integration_query);
			System.out.println("Mappings pruning terminated");
			
		}
		
		if (e.getSource().equals(this.wb.saveResults)) {
			
			System.out.println("Save results invoked");
			this.qrm.storeResults(this.wb.integration_query, this.wb.results);
			//this.qrm.processResults(this.wb.integration_query);
			System.out.println("Save results terminated");
			
		}

		if (e.getSource().equals(this.wb.exportPruningResults)) {
			
			System.out.println("Export Pruning Results Invoked");
			this.smr.exportPruningResults(this.wb.integration_query);
			System.out.println("Export Pruning Results Terminated");
			
		}
		
		if (e.getSource().equals(this.wb.reinitialiseDS)) {
			
			System.out.println("Dataspace Reinitalisation invoked");
			this.qrm.reinitialiseDS(this.wb.integration_query);
			System.out.println("Dataspace Reinitialisation terminated");
			
		}

		if (e.getSource().equals(this.wb.initialiseLog)) {
			
			System.out.println("Log Initalisation invoked");
			this.qrm.initialiseLog(this.wb.integration_query);
			System.out.println("Log Initialisation terminated");
			
		}

		if (e.getSource().equals(this.wb.reinitialisePruningResults)) {
			
			System.out.println("Pruning Results Reinitialisation Invoked");
			this.qrm.reinitialisePruningResults();
			System.out.println("Pruning Results Reinitialisation Terminated");
			
		}

		if (e.getSource().equals(this.wb.annotateMappingsCorrectly)) {
			
			System.out.println("Start Annotating mappings based on complete knowledge ....");
			this.pm.annotateMappingsCorrectly(this.wb.integration_query);
			System.out.println("Annotation task terminated");
			
		}
		if (e.getSource().equals(this.wb.exitMenuItem)) {
			
			this.wb.source.close();
			System.exit(0);
			
		}
		
		
		/*
		if (e.getSource().equals(this.wb.updateLog)) {
			
			System.out.println("Log update invoked");
			this.qrm.updateLog(this.wb);
			System.out.println("Log update terminated");
			
		}
		*/
		
		
	}

}

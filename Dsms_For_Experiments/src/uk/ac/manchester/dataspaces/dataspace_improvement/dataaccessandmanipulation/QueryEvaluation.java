package uk.ac.manchester.dataspaces.dataspace_improvement.dataaccessandmanipulation;

import java.util.Vector;

import uk.ac.manchester.dataspaces.dataspace_improvement.util.IntegrationQuery;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.MappingResultSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.ResultsSet;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SchemaMapping;
import uk.ac.manchester.dataspaces.dataspace_improvement.util.SourceQuery;

public interface QueryEvaluation {
	
	public ResultsSet evaluateIntegrationQuery(String i_query);
	public MappingResultSet evaluateSourceQuery(String s_query);
	public Vector getCandidateMappings(String i_query);
	public Vector selectMappings(Vector c_mappings);
	public Vector getHead(String i_query);


}

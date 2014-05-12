There are items in this repository:

1- An Eclipse Java project names Dsms_For_Experiments. This project contains the source code of programs used for performing a set of dataspace experiments. Namely, schema mapping annotations, schema mapping refinements, and feedback propagation from base relations to queries and vice-versa.

The main classes to look at for the code used for schema mapping annotation and refinement can be found in the Java package: 
uk.ac.manchester.dataspaces.dataspace_improvement.mappingmanagement

The class: PruneMappingsPostgres.java implements the methods specified in PruneMappings.java, and is used for annotating schema mapping based on feedback

I have tested the methods of the above class, and it seems to be working fine.

The class: RefineMappingsPostgres.java implements the methods specified in RefineMappings.java, and is used for refining schema mappings. I did not test the methods of this class lately, so you may experience problems wit hit.

The file in the resources folder is used to set details that are used to access the database, such as the login and password

2. To manipulate and store data, I used the Postgres database. I have included a dump file of the database, dataspaces.sql, which you can play with and use to test the different programs.

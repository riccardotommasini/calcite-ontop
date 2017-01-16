package it.polimi.sr.obbda;

/*
 * #%L
 * ontop-quest-owlapi3
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import it.unibz.inf.ontop.model.OBDAModel;
import it.unibz.inf.ontop.owlrefplatform.owlapi.MappingLoader;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWL;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLConfiguration;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLConnection;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLFactory;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLResultSet;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLStatement;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

@SuppressWarnings("Since15")
public class QuestOWL_OBDA_Calcite {

    /*
     *
     * Please use the pre-bundled H2 server from the root of this repository
     *
     */
    final String owlFile = "src/main/resources/exampleBooks.owl";
    final String obdaFile = "src/main/resources/exampleBooks.obda";
    final String sparqlFile = "src/main/resources/q2.rq";

    /**
     * Main client program
     */
    public static void main(String[] args) {
        try {
            QuestOWL_OBDA_Calcite example = new QuestOWL_OBDA_Calcite();
            example.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() throws Exception {

       		/*
         * Prepare the data connection for querying.
		 */
        File f = new File(sparqlFile);
        BufferedReader br = new BufferedReader(new FileReader(f));
        String sparqlQuery = br.readLine();
        System.out.println(sparqlQuery);

        OWLOntology ontology = OWLManager.createOWLOntologyManager()
                .loadOntologyFromOntologyDocument(new File(owlFile));

        OBDAModel obdaModel = new MappingLoader().loadFromOBDAFile(obdaFile);
		/*
         * Create the instance of Quest OWL reasoner.
		 */
        QuestOWLFactory factory = new QuestOWLFactory();

        QuestOWLConfiguration config = QuestOWLConfiguration.builder().obdaModel(obdaModel).build();

        QuestOWL reasoner = factory.createReasoner(ontology, config);
        QuestOWLConnection conn = reasoner.getConnection();

        /**ADDING SOURCE**/

        System.out.println("ADDING SOURCE");

        CalciteConnection calciteConnection = conn.getConnection().unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        final DataSource ds = JdbcSchema.dataSource("jdbc:postgresql://192.168.99.100:5432/postgres", "org.postgresql.Driver", "postgres", "");
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", ds, null, null));

        DatabaseMetaData md = calciteConnection.getMetaData();
        ResultSet tables = md.getTables(null, "public", "%", null);

        /**QUERYING FROM ONTOP**/

        System.out.println("QUERYING FROM ONTOP");

        QuestOWLStatement st = conn.createStatement();
        QuestOWLResultSet rs = st.executeTuple(sparqlQuery);

        int columnSize = rs.getColumnCount();
        while (rs.nextRow()) {
            for (int idx = 1; idx <= columnSize; idx++) {
                OWLObject binding = rs.getOWLObject(idx);
                System.out.print(binding.toString() + ", ");
            }
            System.out.print("\n");
        }
        rs.close();

			/*
             * Print the query summary
			 */

        String sqlQuery = st.getUnfolding(sparqlQuery);

        System.out.println();
        System.out.println("The input SPARQL query:");
        System.out.println("=======================");
        System.out.println(sparqlQuery);
        System.out.println();

        System.out.println("The output SQL query:");
        System.out.println("=====================");
        System.out.println(sqlQuery);

        System.out.println("QUERYING FROM CALCITE");
        /**QUERYING FROM CALCITE**/

        Statement stmt = calciteConnection.createStatement();
        ResultSet rs1 = stmt.executeQuery(sqlQuery.replace("NULL AS \"xLang\"", "'' AS \"xLang\""));

        while (rs1.next()) {
            System.out.println(rs1.getString(1) + " " + rs1.getString(2) + " " + rs1.getString(3));
        }
    }
}
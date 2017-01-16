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
import it.unibz.inf.ontop.owlrefplatform.calcite.CalciteDataSource;
import it.unibz.inf.ontop.owlrefplatform.calcite.CalciteOBDAModel;
import it.unibz.inf.ontop.owlrefplatform.owlapi.MappingLoader;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWL;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLConfiguration;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLConnection;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLFactory;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLResultSet;
import it.unibz.inf.ontop.owlrefplatform.owlapi.QuestOWLStatement;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Since15")
public class QuestOWL_OBDA_Calcite_Edit {

    /*
     *
     * Please use the pre-bundled H2 server from the root of this repository
     *
     */
    final String owlFile = "src/main/resources/exampleBooks.owl";
    final String obdaFile = "src/main/resources/calcite/exampleBooks.obda";
    final String sparqlFile = "src/main/resources/q2.rq";

    /**
     * Main client program
     */
    public static void main(String[] args) {
        try {
            QuestOWL_OBDA_Calcite_Edit example = new QuestOWL_OBDA_Calcite_Edit();
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
        //final DataSource ds = JdbcSchema.dataSource("jdbc:mysql://192.168.99.100:3306/mysql", "com.mysql.jdbc.Driver", "root", "mysql");
        final DataSource ds = JdbcSchema.dataSource("jdbc:postgresql://192.168.99.100:5432/postgres", "org.postgresql.Driver", "postgres", "");
        CalciteDataSource cds = new CalciteDataSource("public", ds);

        List<CalciteDataSource> cdslist = new ArrayList<CalciteDataSource>();
        cdslist.add(cds);

        OBDAModel obdaModel = new MappingLoader().loadFromOBDAFile(obdaFile);
        CalciteOBDAModel calciteOBDAModel = new CalciteOBDAModel(cdslist, obdaModel);
        /*
         * Create the instance of Quest OWL reasoner.
		 */
        QuestOWLFactory factory = new QuestOWLFactory();

        QuestOWLConfiguration config = QuestOWLConfiguration.builder().obdaModel(calciteOBDAModel).build();

        QuestOWL reasoner = factory.createReasoner(ontology, config);
        QuestOWLConnection conn = reasoner.getConnection();

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

    }
}
package it.polimi.sr.obbda.metadata;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by Riccardo on 08/01/2017.
 *
 * This class prints out the metadata as they are seen from Calcite.
 * I need to check if they are compatible between databases so that we can provide some
 * sort of Calcite dialect for SQL into Ontop.
 */
public class CalciteMetadata {

    @SuppressWarnings("Since15")
    public static void metadata(DataSource ds) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", ds, null, null));

        DatabaseMetaData md = connection.getMetaData();
        ResultSet tables = md.getColumns(null, "public", "tb_%", null);
        while (tables.next()) {
            String string = tables.getString(2);
            String string1 = tables.getString(3);
            ResultSet t = md.getColumns(null, string, string1, "%");
            ResultSetMetaData metaData = t.getMetaData();
            int columnCount = metaData.getColumnCount();

            for (int cl = 1; cl <= columnCount; cl++) {
                System.out.print(metaData.getColumnName(cl) + ";");
            }
            System.out.println("");
            while (t.next()) {
                for (int cl = 1; cl <= columnCount; cl++) {
                    System.out.print(t.getString(cl) + ";");
                }
                System.out.println("");
            }
        }
    }
}

package it.polimi.sr.obbda.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by Riccardo on 08/01/2017.
 */
public class JDBCMetadata {

    public static void metadata(Connection conn) throws SQLException {

        DatabaseMetaData md = conn.getMetaData();
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

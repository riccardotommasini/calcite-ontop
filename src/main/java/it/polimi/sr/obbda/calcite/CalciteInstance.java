package it.polimi.sr.obbda.calcite;


import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Riccardo on 29/12/2016.
 */
@SuppressWarnings("Since15")
public class CalciteInstance {

    protected static Connection connection;
    protected static CalciteConnection calciteConnection;
    protected static SchemaPlus rootSchema;

    public static void init(DataSource ds) throws SQLException {
        connection = DriverManager.getConnection("jdbc:calcite:");
        calciteConnection = connection.unwrap(CalciteConnection.class);
        rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("public", JdbcSchema.create(rootSchema, "public", ds, null, null));
    }
}
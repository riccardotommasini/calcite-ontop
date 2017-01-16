package it.polimi.sr.obbda.metadata.mysql;


import it.polimi.sr.obbda.metadata.CalciteMetadata;
import org.apache.calcite.adapter.jdbc.JdbcSchema;

import java.sql.SQLException;

/**
 * Created by Riccardo on 29/12/2016.
 */
@SuppressWarnings("Since15")
public class MySQLCalciteMetadata extends CalciteMetadata{
    public static void main(String[] args) throws SQLException {
        metadata(JdbcSchema.dataSource("jdbc:mysql://192.168.99.100:3306/mysql", "com.mysql.jdbc.Driver", "root", "mysql"));
    }
}
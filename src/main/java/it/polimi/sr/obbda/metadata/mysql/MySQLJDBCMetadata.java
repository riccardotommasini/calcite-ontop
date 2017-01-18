package it.polimi.sr.obbda.metadata.mysql;

import it.polimi.sr.obbda.metadata.JDBCMetadata;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Riccardo on 07/01/2017.
 */
public class MySQLJDBCMetadata extends JDBCMetadata {

    public static void main(String[] args) throws SQLException {
        metadata(DriverManager.getConnection("jdbc:mysql://192.168.99.100:3306/mysql", "root", "mysql"));
    }
}

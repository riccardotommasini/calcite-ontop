package it.polimi.sr.obbda.metadata.postgres;

import it.polimi.sr.obbda.metadata.JDBCMetadata;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Riccardo on 07/01/2017.
 */
public class PostgresMetadata extends JDBCMetadata {

    public static void main(String[] args) throws SQLException {
        metadata(DriverManager.getConnection("jdbc:postgresql://192.168.99.100:5432/postgres", "postgres", ""));
    }
}

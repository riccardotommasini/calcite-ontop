package it.polimi.sr.obbda;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

/**
 * Created by Riccardo on 06/01/2017.
 */
public class DriverTest {

    public static void main(String[] args) throws SQLException {
        //      Connection postgres = DriverManager.getConnection("jdbc:postgresql://192.168.99.100:5432/postgres", "postgres", "");
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while(drivers.hasMoreElements()){
            System.out.println(drivers.nextElement().toString());
        }
    }
}

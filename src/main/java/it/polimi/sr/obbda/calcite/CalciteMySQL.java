package it.polimi.sr.obbda.calcite;


import org.apache.calcite.adapter.jdbc.JdbcSchema;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Riccardo on 29/12/2016.
 */
@SuppressWarnings("Since15")
public class CalciteMySQL extends CalciteInstance {
    public static void main(String[] args) throws SQLException {

        init(JdbcSchema.dataSource("jdbc:mysql://192.168.99.100:3306/mysql", "com.mysql.jdbc.Driver", "root", "mysql"));

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT \n" +
                " 1 AS \"xQuestType\", '' AS \"xLang\"," +
                "CAST(Qtb_booksVIEW0.\"bk_code\" AS CHAR(8000) CHARACTER SET utf8) " +
                "FROM \"public\".\"tb_books\" Qtb_booksVIEW0");


        ResultSetMetaData meta = rs.getMetaData();
        System.out.println(meta.getColumnName(1));
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}
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
public class CalcitePostgres extends CalciteInstance {
    public static void main(String[] args) throws SQLException {
        init(JdbcSchema.dataSource("jdbc:postgresql://192.168.99.100:5432/postgres", "org.postgresql.Driver", "postgres", ""));
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT \n" +
                "   1 AS \"xQuestType\", '' AS \"xLang\", ('http://meraka/moss/exampleBooks.owl#book/' || REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(qpublic_tb_booksview0.\"bk_code\" AS VARCHAR(10485760)), ' ', '%20'), '!', '%21'), '''', '%22'), '#', '%23'), '$', '%24'), '&', '%26'), '(', '%28'), ')', '%29'), '*', '%2A'), '+', '%2B'), ',', '%2C'), '/', '%2F'), ':', '%3A'), ';', '%3B'), '=', '%3D'), '?', '%3F'), '@', '%40'), '[', '%5B'), ']', '%5D') || '/') AS \"x\"\n" +
                " FROM \n" +
                "\"public\".\"tb_books\" qpublic_tb_booksview0");

        ResultSetMetaData meta = rs.getMetaData();
        System.out.println(meta.getColumnName(1) + " " + meta.getColumnName(2) + " " + meta.getColumnName(3));
        while (rs.next()) {
            System.out.println(rs.getString(1) + " " + rs.getObject(2) + " " + rs.getString(3));
        }
    }
}
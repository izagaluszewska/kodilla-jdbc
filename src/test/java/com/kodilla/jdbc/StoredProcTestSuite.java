package com.kodilla.jdbc;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class StoredProcTestSuite {
    private static String sqlInsertBookOneReaderOne = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (1, 1, CURDATE() - 10, null)";
    private static String sqlInsertBookTwoReaderOne = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (2, 1, CURDATE() - 10, null)";
    private static String sqlInsertBookFourReaderOne = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (4, 1, CURDATE() - 10, CURDATE() - 5)";
    private static String sqlInsertBookOneReaderThree = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (1, 3, CURDATE() - 8, null)";
    private static String sqlInsertBookFiveReaderThree = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (5, 3, CURDATE() - 4, CURDATE() - 2)";
    private static String sqlInsertBookFiveReaderFour = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (5, 4, CURDATE() - 10, CURDATE() - 8)";
    private static String sqlInsertBookFiveReaderFive = "INSERT INTO RENTS(BOOK_ID, READER_ID, RENT_DATE, RETURN_DATE) VALUES (5, 5, CURDATE() - 8, null)";
    private static String sqlDelete = "DELETE FROM RENTS";
    private static String sqlCheckTableHowManyBestsellers = "SELECT COUNT(*) AS HOW_MANY FROM BOOKS WHERE BESTSELLER=true";
    private static String sqlCallUpdateBestsellers = "CALL UpdateBestsellers()";

    @Before
    public void deleteRentsTable() throws SQLException {
        DbManager dbManager = DbManager.getInstance();
        Statement statement = dbManager.getConnection().createStatement();
        statement.execute(sqlDelete);
    }

    @AfterClass
    public static void originalTableRents() throws SQLException {
        DbManager dbManager = DbManager.getInstance();
        Statement statement = dbManager.getConnection().createStatement();
        statement.execute(sqlDelete);
        statement.execute(sqlInsertBookOneReaderOne);
        statement.execute(sqlInsertBookTwoReaderOne);
        statement.execute(sqlInsertBookFourReaderOne);
        statement.execute(sqlInsertBookOneReaderThree);
        statement.execute(sqlInsertBookFiveReaderThree);
        statement.execute(sqlInsertBookFiveReaderFour);
        statement.execute(sqlInsertBookFiveReaderFive);
        statement.execute(sqlCallUpdateBestsellers);
    }

    @Test
    public void testUpdateVipLevels() throws SQLException {
        //Given
        DbManager dbManager = DbManager.getInstance();
        String sqlUpdate = "UPDATE READERS SET VIP_LEVEL=\"Not set\"";
        Statement statement = dbManager.getConnection().createStatement();
        statement.executeUpdate(sqlUpdate);
        //When
        String sqlProcedureCall = "CALL UpdateVIpLevels()";
        statement.execute(sqlProcedureCall);
        //Then
        String sqlCheckTableVipLevel = "SELECT COUNT(*) AS HOW_MANY FROM READERS WHERE VIP_LEVEL=\"Not set\"";
        ResultSet rs = statement.executeQuery(sqlCheckTableVipLevel);
        int howMany = -1;
        if (rs.next()) {
            howMany = rs.getInt("HOW_MANY");
        }
        assertEquals(0, howMany);
    }

    @Test
    public void testUpdateBestsellersIfOneBestseller() throws SQLException {
        //Given
        DbManager dbManager = DbManager.getInstance();
        Statement statement = dbManager.getConnection().createStatement();
        statement.execute(sqlInsertBookOneReaderOne);
        statement.execute(sqlInsertBookOneReaderThree);
        //When
        statement.execute(sqlCallUpdateBestsellers);
        //Then
        ResultSet rs = statement.executeQuery(sqlCheckTableHowManyBestsellers);
        int howMany = -1;
        if (rs.next()) {
            howMany = rs.getInt("HOW_MANY");
        }
        assertEquals(1, howMany);
    }

    @Test
    public void testUpdateBestsellersIfNoBestsellers() throws SQLException {
        //Given
        DbManager dbManager = DbManager.getInstance();
        Statement statement = dbManager.getConnection().createStatement();
        //When
        statement.execute(sqlCallUpdateBestsellers);
        //Then
        ResultSet rs = statement.executeQuery(sqlCheckTableHowManyBestsellers);
        int howMany = -1;
        if (rs.next()) {
            howMany = rs.getInt("HOW_MANY");
        }
        assertEquals(0, howMany);
    }

    @Test
    public void testUpdateBestsellersIfTwoBestsellers() throws SQLException {
        //Given
        DbManager dbManager = DbManager.getInstance();
        Statement statement = dbManager.getConnection().createStatement();
        statement.execute(sqlInsertBookOneReaderOne);
        statement.execute(sqlInsertBookFiveReaderThree);
        //When
        statement.execute(sqlCallUpdateBestsellers);
        //Then
        ResultSet rs = statement.executeQuery(sqlCheckTableHowManyBestsellers);
        int howMany = -1;
        if (rs.next()) {
            howMany = rs.getInt("HOW_MANY");
        }
        assertEquals(2, howMany);
    }
}

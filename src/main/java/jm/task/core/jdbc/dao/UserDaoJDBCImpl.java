package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import static jm.task.core.jdbc.util.Util.getMySQLConnection;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }
    private static final String SQL_CREATE_USERS_TABLE = "CREATE TABLE IF NOT EXISTS `testkata`.`users` (" +
            "`id` BIGINT(255) NOT NULL AUTO_INCREMENT, " +
            "`name` VARCHAR(45) NULL , " +
            "`lastName` VARCHAR(45) NULL, " +
            "`age` TINYINT NULL, " +
            "PRIMARY KEY (`id`));";
    private static final String SQL_DROP_USERS_TABLE = "DROP TABLE IF EXISTS testkata.users;";
    private static final String SQL_SAVE_USER = "INSERT IGNORE INTO testkata.users (`name`, `lastName`, `age`) VALUES (?,?,?)";
    private static final String SQL_REMOVE_USER_BY_ID = "DELETE FROM `testkata`.`users` WHERE id = ?";
    private static final String SQL_GET_ALL_USERS = "SELECT * FROM testkata.users";
    private static final String SQL_CLEAN_USERS_TABLE = "DELETE FROM testkata.users";
    private static final Connection CONNECTION = getMySQLConnection();

    public void createUsersTable() {
        try (Statement statement = CONNECTION.createStatement()) {
            statement.executeUpdate(SQL_CREATE_USERS_TABLE);
            System.out.println("table created");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void dropUsersTable() {
        try (Statement statement = CONNECTION.createStatement()) {
            statement.executeUpdate(SQL_DROP_USERS_TABLE);
            System.out.println("table deleted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement statement = CONNECTION.prepareStatement(SQL_SAVE_USER)) {
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setByte(3, age);
            statement.executeUpdate();
            System.out.println("User " + name + " " + lastName + " at the age of " + age + " has been added.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void removeUserById(long id) {
        try (PreparedStatement statement = CONNECTION.prepareStatement(SQL_REMOVE_USER_BY_ID)) {
            statement.setLong(1, id);
            statement.executeUpdate();
            System.out.println("User with id=" + id + " has been deleted.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public List<User> getAllUsers() {
        ResultSet result;
        List<User> users = new LinkedList<>();
        try (Statement statement = CONNECTION.createStatement()) {
            result = statement.executeQuery(SQL_GET_ALL_USERS);
            while (result.next()) {
                users.add(new User( result.getString(2),
                                    result.getString(3),
                                    result.getByte(4)));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println(users);
        return users;
    }
    public void cleanUsersTable() {
        try (Statement statement = CONNECTION.createStatement()) {
            statement.executeUpdate(SQL_CLEAN_USERS_TABLE);
            System.out.println("Table is clean.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
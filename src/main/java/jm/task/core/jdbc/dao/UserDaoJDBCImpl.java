package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import static jm.task.core.jdbc.util.Util.getMySQLConnection;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }
    private final String sqlCreateUsersTable = "CREATE TABLE IF NOT EXISTS `testkata`.`users` (" +
            "`id` BIGINT(255) NOT NULL AUTO_INCREMENT, " +
            "`name` VARCHAR(45) NULL , " +
            "`lastName` VARCHAR(45) NULL, " +
            "`age` TINYINT NULL, " +
            "PRIMARY KEY (`id`));";
    private final String sqlDropUsersTable = "DROP TABLE IF EXISTS testkata.users;";
    private final String sqlSaveUser = "INSERT IGNORE INTO testkata.users (`name`, `lastName`, `age`) VALUES (?,?,?)";
    private final String sqlRemoveUserById = "DELETE FROM `testkata`.`users` WHERE id = ?";
    private final String sqlGetAllUsers = "SELECT * FROM testkata.users";
    private final String sqlCleanUsersTable = "DELETE FROM testkata.users";
    private static final Connection connection = getMySQLConnection();

    public void createUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery(sqlCreateUsersTable);
            System.out.println("table created");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void dropUsersTable() {
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery(sqlDropUsersTable);
            System.out.println("table deleted");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement statement = connection.prepareStatement(sqlSaveUser)) {
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
        try (PreparedStatement statement = connection.prepareStatement(sqlRemoveUserById)) {
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
        try (Statement statement = connection.createStatement()) {
            result = statement.executeQuery(sqlGetAllUsers);
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
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlCleanUsersTable);
            System.out.println("Table is clean.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
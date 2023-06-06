package jm.task.core.jdbc;

import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
import jm.task.core.jdbc.service.UserServiceImpl;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        UserServiceImpl userService = new UserServiceImpl();
        userService.createUsersTable();
        new UserServiceImpl().saveUser("Obi", "Wan", (byte)37);
        new UserServiceImpl().saveUser("Samanta", "Benet", (byte)26);
//        new UserServiceImpl().removeUserById(2);
//        new UserServiceImpl().saveUser("Krag", "Tarvin", (byte)32);
//        new UserServiceImpl().saveUser("Bethy", "Benet", (byte)23);
    //    new UserServiceImpl().getAllUsers().get(0);
    //    System.out.println(new UserServiceImpl().getAllUsers().size());
//        new UserServiceImpl().cleanUsersTable();
    //    userService.dropUsersTable();
    }
}

package com.example.ov_mm.notes.server;

import com.example.ov_mm.notes.server.model.Note;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class NotesDao {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:notes.db")) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE IF EXISTS notes");
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS notes (" +
                        "id INTEGER PRIMARY KEY," +
                        "guid TEXT NOT NULL," +
                        "date INTEGER NOT NULL," +
                        "title TEXT," +
                        "content TEXT," +
                        "deleted BOOLEAN NOT NULL," +
                        "user TEXT NOT NULL," +
                        "version INTEGER NOT NULL" +
                        ")");
            }
        } catch (SQLException | RuntimeException e) {
            Logger.getLogger(NotesDao.class.getName()).log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public List<Note> getNotes(Long fromVersion, String user, Set<String> guids) {
        try (Connection connection = getConnection()) {
            String sql = String.format("SELECT id, guid, title, content, date, deleted, version " +
                    "FROM notes WHERE user = ? AND (version > ? OR guid IN (%s))",
                    guids.stream().map(g -> "?").collect(Collectors.joining(", ")));
            try (PreparedStatement statement = connection.prepareStatement(sql)){
                statement.setObject(1, user);
                statement.setObject(2, fromVersion);
                Iterator<String> iterator = guids.iterator();
                for (int i = 3; i < 3 + guids.size(); i++) {
                    statement.setObject(i, iterator.next());
                }
                ResultSet resultSet = statement.executeQuery();
                List<Note> notes = new ArrayList<>();
                while (resultSet.next()) {
                    Note note = new Note();
                    note.setId(resultSet.getLong(1));
                    note.setGuid(resultSet.getString(2));
                    note.setTitle(resultSet.getString(3));
                    note.setContent(resultSet.getString(4));
                    note.setDate(resultSet.getLong(5));
                    note.setDeleted(resultSet.getBoolean(6));
                    note.setVersion(resultSet.getLong(7));
                    notes.add(note);
                }
                return notes;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Long getMaxVersion(String user) {
        try (Connection connection = getConnection()) {
            String sql = "SELECT MAX(version) FROM notes WHERE user = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)){
                statement.setObject(1, user);
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    Long version = resultSet.getLong(1);
                    if (resultSet.wasNull()) {
                        return null;
                    }
                    return version;
                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveNotes(List<Note> notes) {
        try (Connection connection = getConnection()) {
            String insert = "INSERT INTO notes (guid, date, title, content, deleted, version, user) VALUES (?, ?, ?, ?, ?, ?, ?)";
            String update = "UPDATE notes SET date = ?, title = ?, content = ?, deleted = ?, version = ? WHERE id = ?";
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(insert)) {
                for (Note n : notes.stream().filter(n -> n.getId() == null).collect(Collectors.toList())) {
                    statement.setObject(1, n.getGuid());
                    statement.setObject(2, n.getDate());
                    statement.setObject(3, n.getTitle());
                    statement.setObject(4, n.getContent());
                    statement.setObject(5, n.isDeleted());
                    statement.setObject(6, n.getVersion());
                    statement.setObject(7, n.getUser());
                    statement.execute();
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(update)) {
                for (Note n : notes.stream().filter(n -> n.getId() != null).collect(Collectors.toList())) {
                    statement.setObject(1, n.getDate());
                    statement.setObject(2, n.getTitle());
                    statement.setObject(3, n.getContent());
                    statement.setObject(4, n.isDeleted());
                    statement.setObject(5, n.getVersion());
                    statement.setObject(6, n.getId());
                    statement.execute();
                }
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:notes.db");
    }
}

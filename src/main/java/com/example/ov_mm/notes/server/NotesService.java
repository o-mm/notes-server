package com.example.ov_mm.notes.server;

import com.example.ov_mm.notes.server.model.Note;
import com.google.gson.Gson;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Path("/sync")
public class NotesService {

    private NotesDao dao = new NotesDao();

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response syncNotes(String syncObject) throws InterruptedException {
        //TODO synchronization by user
        Logger.getLogger(getClass().getName()).info(syncObject);
        SyncObject object = new Gson().fromJson(syncObject, SyncObject.class);
        if (object.user == null) {
            return Response.status(400).entity("Null user").build();
        }
        Long maxVersion = Optional.ofNullable(dao.getMaxVersion(object.user)).orElse(0L);
        Logger.getLogger(getClass().getName()).info("Current DB version: " + maxVersion);
        List<Note> notes = dao.getNotes(object.version == null ? 0L : object.version, object.user,
                Stream.of(object.getNotes()).map(JNote::getGuid).collect(Collectors.toSet()));
        Logger.getLogger(getClass().getName()).info("Notes from DB: " + notes.size());
        List<Note> mergedNotes = mergeNotes(Arrays.asList(object.notes), notes, maxVersion + 1, object.user);
        List<Note> notesToSave = mergedNotes.stream().filter(n -> n.getVersion() == maxVersion + 1)
                .collect(Collectors.toList());
        dao.saveNotes(notesToSave);
        Thread.sleep(new Random().nextInt(20) * 1000);
        return Response.status(200)
                .entity(createResponse(mergedNotes, notesToSave.isEmpty() ? maxVersion : maxVersion + 1))
                .build();
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response hello() {
        return Response.status(200).entity("Hello!").build();
    }

    private List<Note> mergeNotes(List<JNote> clientNotes, List<Note> serverNotes, Long version, String user) {
        Map<String, JNote> clientMap = clientNotes.stream().collect(Collectors.toMap(JNote::getGuid, n -> n));
        List<Note> result = new ArrayList<>();
        for (Note note : serverNotes) {
            JNote jNote = clientMap.remove(note.getGuid());
            if (jNote != null) {
                merge(jNote, note, version);
            }
            result.add(note);
        }
        result.addAll(clientMap.values().stream().map(n -> fromJNote(n, version, user)).collect(Collectors.toList()));
        return result;
    }

    private void merge(JNote jNote, Note note, Long version) {
        if (jNote.date > note.getDate()) {
            note.setVersion(version);
            note.setTitle(jNote.title);
            note.setContent(jNote.content);
            note.setDate(jNote.date);
            note.setDeleted(jNote.deleted);
        }
    }

    private Note fromJNote(JNote jNote, Long version, String user) {
        Note note = new Note();
        note.setGuid(jNote.guid);
        note.setVersion(version);
        note.setDeleted(jNote.deleted);
        note.setDate(jNote.date);
        note.setTitle(jNote.title);
        note.setContent(jNote.content);
        note.setUser(user);
        return note;
    }

    private JNote toJNote(Note note) {
        JNote jNote = new JNote();
        jNote.date = note.getDate();
        jNote.guid = note.getGuid();
        jNote.title = note.getTitle();
        jNote.deleted = note.isDeleted();
        jNote.content = note.getContent();
        return jNote;
    }

    private String createResponse(List<Note> notes, Long version) {
        String s = new Gson().toJson(new SyncObject(version, notes.stream().map(this::toJNote).toArray(JNote[]::new)));
        Logger.getLogger(getClass().getName()).info(s);
        return s;
    }

    public static class JNote {

        private String guid;
        private String title;
        private String content;
        private Long date;
        private Boolean deleted;

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public Long getDate() {
            return date;
        }

        public void setDate(Long date) {
            this.date = date;
        }

        public Boolean getDeleted() {
            return deleted;
        }

        public void setDeleted(Boolean deleted) {
            this.deleted = deleted;
        }
    }

    public static class SyncObject {

        private Long version;
        private String user;
        private JNote[] notes;

        public SyncObject() {
        }

        public SyncObject(Long version, JNote[] notes) {
            this.version = version;
            this.notes = notes;
        }

        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

        public JNote[] getNotes() {
            return notes;
        }

        public void setNotes(JNote[] notes) {
            this.notes = notes;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
    }
}

/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.layer2;

public enum MongoOp {
    FIND,
    FIND_ONE,
    FIND_BY_ID,
    FIND_DISTINCT,
    FIND_AND_MODIFY,
    FIND_AND_REPLACE,
    FIND_AND_REMOVE,
    STREAM,
    AGGREGATE,
    MAP_REDUCE,
    INSERT,
    INSERT_MANY,
    SAVE,
    SAVE_ALL,
    UPDATE,
    UPDATE_MANY,
    UPSERT,
    REPLACE,
    DELETE,
    DELETE_MANY,
    FIND_ALL_AND_REMOVE,
    BULK_WRITE,
    COUNT,
    EXISTS,
    GET_COLLECTION,
    EXECUTE_COMMAND,
    UNKNOWN;


    public boolean isRead() {
        return switch (this) {
            case FIND, FIND_ONE, FIND_BY_ID, FIND_DISTINCT, FIND_AND_MODIFY, FIND_AND_REPLACE, FIND_AND_REMOVE, STREAM, AGGREGATE, MAP_REDUCE, COUNT, EXISTS -> true;
            default -> false;
        };
    }

    public boolean isWrite() {
        return switch (this) {
            case INSERT, INSERT_MANY, SAVE, SAVE_ALL, UPDATE, UPDATE_MANY, UPSERT, REPLACE, DELETE, DELETE_MANY, FIND_ALL_AND_REMOVE, BULK_WRITE -> true;
            default -> false;
        };
    }

    public static MongoOp fromMethodName(String name) {
        if (name == null) {
            return UNKNOWN;
        }
        String n = name.toLowerCase();
        if (n.startsWith("findallande")) {
            return FIND_ALL_AND_REMOVE;
        }
        if (n.startsWith("findallandremove")) {
            return FIND_ALL_AND_REMOVE;
        }
        if (n.startsWith("findandreplace")) {
            return FIND_AND_REPLACE;
        }
        if (n.startsWith("findandmodify")) {
            return FIND_AND_MODIFY;
        }
        if (n.startsWith("findandremove")) {
            return FIND_AND_REMOVE;
        }
        if (n.startsWith("findoneanddel")) {
            return FIND_AND_REMOVE;
        }
        if (n.startsWith("findoneandup")) {
            return FIND_AND_MODIFY;
        }
        if (n.startsWith("findoneandreplace")) {
            return FIND_AND_REPLACE;
        }
        if (n.startsWith("findbyid")) {
            return FIND_BY_ID;
        }
        if (n.startsWith("finddistinct")) {
            return FIND_DISTINCT;
        }
        if (n.equals("findall")) {
            return FIND;
        }
        if (n.startsWith("findone")) {
            return FIND_ONE;
        }
        if (n.startsWith("find")) {
            return FIND;
        }
        if (n.startsWith("stream")) {
            return STREAM;
        }
        if (n.startsWith("aggregate")) {
            return AGGREGATE;
        }
        if (n.startsWith("mapreduce")) {
            return MAP_REDUCE;
        }
        if (n.equals("insertall") || n.startsWith("insertmany")) {
            return INSERT_MANY;
        }
        if (n.startsWith("insert")) {
            return INSERT;
        }
        if (n.equals("saveall")) {
            return SAVE_ALL;
        }
        if (n.startsWith("save")) {
            return SAVE;
        }
        if (n.startsWith("replace")) {
            return REPLACE;
        }
        if (n.startsWith("updatemulti") || n.startsWith("updatemany") || n.startsWith("updatemulti")) {
            return UPDATE_MANY;
        }
        if (n.startsWith("upsert")) {
            return UPSERT;
        }
        if (n.startsWith("updatefirst") || n.startsWith("update")) {
            return UPDATE;
        }
        if (n.equals("removeall") || n.startsWith("deleteall") || n.startsWith("deletemany") || n.startsWith("deletemultiple")) {
            return DELETE_MANY;
        }
        if (n.startsWith("delete") || n.startsWith("remove")) {
            return DELETE;
        }
        if (n.startsWith("bulkops") || n.startsWith("bulkwrite")) {
            return BULK_WRITE;
        }
        if (n.startsWith("count") || n.startsWith("estimate")) {
            return COUNT;
        }
        if (n.startsWith("exists")) {
            return EXISTS;
        }
        if (n.startsWith("getcollect")) {
            return GET_COLLECTION;
        }
        if (n.startsWith("executecommand") || n.startsWith("execute")) {
            return EXECUTE_COMMAND;
        }
        return UNKNOWN;
    }

    public boolean isMutation() {
        return switch (this) {
            case INSERT, INSERT_MANY, SAVE, SAVE_ALL, UPDATE, UPDATE_MANY, UPSERT, REPLACE, DELETE, DELETE_MANY, FIND_ALL_AND_REMOVE, BULK_WRITE -> true;
            default -> false;
        };
    }

    public boolean isFetchable() {
        return this == FIND || this == FIND_ONE || this == FIND_BY_ID || this == FIND_DISTINCT || this == AGGREGATE || this == MAP_REDUCE || this == COUNT;
    }

    public boolean isFilterWrite() {
        return switch (this) {
            case UPDATE, UPDATE_MANY, UPSERT, DELETE, DELETE_MANY, FIND_ALL_AND_REMOVE -> true;
            default -> false;
        };
    }
}


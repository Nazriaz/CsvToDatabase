package ru.siblion.csvadapter.cli;

public enum ArgumentType {
    PATH_TO_CSV_FILE("csv"),
    DB_CONNECTION_STRING("database-connection-string"),
    TABLE_NAME("table-name"),
    USERNAME("username"),
    PASSWORD("password"),
    SEPARATOR("SEPARATOR");

    String argumentName;

    ArgumentType(String argumentName) {
        this.argumentName = argumentName;
    }

    public String getArgumentName() {
        return argumentName;
    }

    public void setArgumentName(String argumentName) {
        this.argumentName = argumentName;
    }
}

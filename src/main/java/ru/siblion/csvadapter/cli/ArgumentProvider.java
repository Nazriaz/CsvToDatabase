package ru.siblion.csvadapter.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

public class ArgumentProvider {
    private String csvFilePath;
    private String dbConnectionString;
    private String tableName;
    private String username;
    private String password;
    private String separator;

    private OptionsProvider optionsProvider;
    private HelpPrinter helpPrinter;

    public ArgumentProvider(OptionsProvider optionsProvider, HelpPrinter helpPrinter) {
        this.optionsProvider = optionsProvider;
        this.helpPrinter = helpPrinter;
    }

    public void getArguments(String[] args) {
        CommandLine commandLine = parseArguments(args);

//        System.out.println(commandLine.getOptionValue(ArgumentType.PATH_TO_CSV_FILE.getArgumentName()));
        csvFilePath = commandLine.getOptionValue(ArgumentType.PATH_TO_CSV_FILE.getArgumentName());
        dbConnectionString = commandLine.getOptionValue(ArgumentType.DB_CONNECTION_STRING.getArgumentName());
        tableName = commandLine.getOptionValue(ArgumentType.TABLE_NAME.getArgumentName());
        username = commandLine.getOptionValue(ArgumentType.USERNAME.getArgumentName());
        password = commandLine.getOptionValue(ArgumentType.PASSWORD.getArgumentName());
        separator = commandLine.getOptionValue(ArgumentType.SEPARATOR.getArgumentName());
    }

    private CommandLine parseArguments(String[] args) {
        CommandLine line = null;

        CommandLineParser parser = new DefaultParser();

        try {
            line = parser.parse(optionsProvider.getOptions(), args);

        } catch (ParseException ex) {

            System.err.println("Failed to parse command line arguments");
            System.err.println(ex.toString());
            helpPrinter.printAppHelp();

            System.exit(1);
        }

        return line;
    }

    public String getCsvFilePath() {
        return csvFilePath;
    }

    public void setCsvFilePath(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    public String getDbConnectionString() {
        return dbConnectionString;
    }

    public void setDbConnectionString(String dbConnectionString) {
        this.dbConnectionString = dbConnectionString;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}

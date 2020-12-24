package ru.siblion.csvadapter;

import org.apache.commons.cli.*;
import ru.siblion.csvadapter.config.DataBaseConfig;
import ru.siblion.csvadapter.service.impl.CsvServiceImpl;
import ru.siblion.csvadapter.service.impl.DBService;

import java.sql.SQLException;
import java.util.List;


public class CsvToDatabaseApp {

    public static final String PATH_TO_CSV_FILE = "csv";
    public static final String DB_CONNECTION_STRING = "database-connection-string";
    public static final String TABLE_NAME = "table-name";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public void run(String[] args) {

        CommandLine commandLine = parseArguments(args);

        if (commandLine.hasOption(PATH_TO_CSV_FILE)) {

            System.out.println(commandLine.getOptionValue(PATH_TO_CSV_FILE));
            String csvFileName = commandLine.getOptionValue(PATH_TO_CSV_FILE);
            String dbConnectionString = commandLine.getOptionValue(DB_CONNECTION_STRING);
            String tableName = commandLine.getOptionValue(TABLE_NAME);
            String username = commandLine.getOptionValue(USERNAME);
            String password = commandLine.getOptionValue(PASSWORD);

            CsvServiceImpl csvServiceImpl = new CsvServiceImpl(csvFileName);
            List<String[]> listArr;
            long startTime = System.nanoTime();
            listArr = csvServiceImpl.getRecords();
            long stopTime = System.nanoTime();
            System.out.println((stopTime - startTime) + " Stream Arr list size " + listArr.size());
            System.out.println(listArr.get(0).length);
            System.out.println(listArr.size());
            DataBaseConfig dataBaseConfig = new DataBaseConfig(dbConnectionString,username,password);
            DBService dbService = new DBService(tableName,dataBaseConfig);
            try {
                dbService.insertIntoTable(listArr, 24);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            printAppHelp();
        }
    }

    private CommandLine parseArguments(String[] args) {

        Options options = getOptions();
        CommandLine line = null;

        CommandLineParser parser = new DefaultParser();

        try {
            line = parser.parse(options, args);

        } catch (ParseException ex) {

            System.err.println("Failed to parse command line arguments");
            System.err.println(ex.toString());
            printAppHelp();

            System.exit(1);
        }

        return line;
    }

    private void printAppHelp() {

        Options options = getOptions();

        var formatter = new HelpFormatter();
        formatter.printHelp("Parser", options, true);
    }

    private Options getOptions() {

        var options = new Options();
        Option csvFile = Option
            .builder("c")
            .argName("CSV_FILE_PATH")
            .longOpt("csv")
            .desc("csv file to load data from")
            .hasArg()
            .build();
        Option dbConnectionString = Option
            .builder("d")
            .argName("DB_CONNECTION_STRING")
            .longOpt("database-connection-string")
            .desc("database connection string")
            .hasArg()
            .build();
        Option tableName = Option
            .builder("t")
            .argName("TABLE_NAME")
            .longOpt("table-name")
            .desc("table name")
            .hasArg()
            .required()
            .build();
        Option userName = Option
            .builder("u")
            .argName("USERNAME")
            .longOpt("username")
            .desc("database username")
            .hasArg()
            .required()
            .build();
        Option password = Option
            .builder("p")
            .argName("PASSWORD")
            .longOpt("password")
            .desc("database user password")
            .hasArg()
            .required()
            .build();
        options
            .addOption(csvFile)
            .addOption(tableName)
            .addOption(userName)
            .addOption(password)
            .addOption(dbConnectionString);
        return options;
    }
}

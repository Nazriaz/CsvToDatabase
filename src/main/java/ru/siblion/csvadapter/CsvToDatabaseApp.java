package ru.siblion.csvadapter;

import org.apache.commons.cli.*;
import ru.siblion.csvadapter.cli.OptionsProvider;
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
    public static final String SEPARATOR = "SEPARATOR";

    OptionsProvider optionsProvider = new OptionsProvider();

    public void run(String[] args) {


        CommandLine commandLine = parseArguments(args);

        if (commandLine.hasOption(PATH_TO_CSV_FILE)) {

            System.out.println(commandLine.getOptionValue(PATH_TO_CSV_FILE));
            String csvFileName = commandLine.getOptionValue(PATH_TO_CSV_FILE);
            String dbConnectionString = commandLine.getOptionValue(DB_CONNECTION_STRING);
            String tableName = commandLine.getOptionValue(TABLE_NAME);
            String username = commandLine.getOptionValue(USERNAME);
            String password = commandLine.getOptionValue(PASSWORD);
            String separator = commandLine.getOptionValue(SEPARATOR);

            CsvServiceImpl csvServiceImpl = new CsvServiceImpl(csvFileName);
            boolean separatorProvidedWithArgs = separator!=null;
            if (separatorProvidedWithArgs) {
                csvServiceImpl.setSeparator(separator);
            }
            List<String[]> listArr;
            long startTime = System.nanoTime();
            listArr = csvServiceImpl.getRecords();
            long stopTime = System.nanoTime();
            System.out.println((stopTime - startTime) / 1000000000 + " seconds - Time to read csv / size:" + listArr.size());
            DataBaseConfig dataBaseConfig = new DataBaseConfig(dbConnectionString, username, password);
            DBService dbService = new DBService(tableName, dataBaseConfig);
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
        CommandLine line = null;

        CommandLineParser parser = new DefaultParser();

        try {
            line = parser.parse(optionsProvider.getOptions(), args);

        } catch (ParseException ex) {

            System.err.println("Failed to parse command line arguments");
            System.err.println(ex.toString());
            printAppHelp();

            System.exit(1);
        }

        return line;
    }

    private void printAppHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("Parser", optionsProvider.getOptions(), true);
    }


}

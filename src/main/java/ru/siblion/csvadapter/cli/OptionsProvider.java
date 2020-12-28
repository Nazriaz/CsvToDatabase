package ru.siblion.csvadapter.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OptionsProvider {
    public Options getOptions() {

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
//            .required()
            .build();
        Option separator = Option
            .builder("s")
            .argName("SEPARATOR")
            .longOpt("separator")
            .desc("separator symbol")
            .hasArg()
            .build();
        options
            .addOption(csvFile)
            .addOption(tableName)
            .addOption(userName)
            .addOption(password)
            .addOption(separator)
            .addOption(dbConnectionString);
        return options;
    }
}

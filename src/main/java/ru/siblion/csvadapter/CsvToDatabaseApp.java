package ru.siblion.csvadapter;

import ru.siblion.csvadapter.cli.ArgumentProvider;
import ru.siblion.csvadapter.cli.HelpPrinter;
import ru.siblion.csvadapter.cli.OptionsProvider;
import ru.siblion.csvadapter.config.ConnectionPool;
import ru.siblion.csvadapter.config.DataBaseConfig;
import ru.siblion.csvadapter.service.impl.CsvServiceImpl;
import ru.siblion.csvadapter.service.impl.DBService;
import ru.siblion.csvadapter.service.impl.NewDBService;
import ru.siblion.csvadapter.util.ProcessingTimer;

import java.sql.SQLException;
import java.util.List;


public class CsvToDatabaseApp {

    private OptionsProvider optionsProvider = new OptionsProvider();
    private HelpPrinter helpPrinter = new HelpPrinter(optionsProvider);
    private ArgumentProvider argumentProvider = new ArgumentProvider(optionsProvider, helpPrinter);


    public void run(String[] args) {

        argumentProvider.getArguments(args);

        if (argumentProvider.getCsvFilePath() != null) {

            CsvServiceImpl csvServiceImpl = new CsvServiceImpl(argumentProvider.getCsvFilePath());
            boolean separatorProvidedWithArgs = argumentProvider.getSeparator() != null;
            if (separatorProvidedWithArgs) {
                csvServiceImpl.setSeparator(argumentProvider.getSeparator());
            }
//            DataBaseConfig dataBaseConfig = new DataBaseConfig(
//                argumentProvider.getDbConnectionString(),
//                argumentProvider.getUsername(),
//                argumentProvider.getPassword());
            ConnectionPool.configure(argumentProvider.getDbConnectionString(),
                argumentProvider.getUsername(),
                argumentProvider.getPassword());
//            DBService dbService = new DBService(
//                argumentProvider.getTableName(),
//                dataBaseConfig);
            NewDBService newDBService = new NewDBService(argumentProvider.getTableName(), dataBaseConfig);
            newDBService.createTempTable();

            List<String[]> csvAsStringArr = readCsv(csvServiceImpl);

            newDBService.insertToNewTableQuery(csvAsStringArr);
            newDBService.prepareMergeTablesQuery();
//            writeToDb(strings);

        } else {
            helpPrinter.printAppHelp();
        }
    }

    private List<String[]> readCsv(CsvServiceImpl csvServiceImpl) {
        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        List<String[]> csvAsStringArr = csvServiceImpl.getRecords();
        System.out.println(processingTimer.stop() +
            " seconds - Time to read csv / size:" + csvAsStringArr.size());
        return csvAsStringArr;
    }

    private void writeToDb(List<String[]> csvAsStringArr) {
        ProcessingTimer processingTimer = new ProcessingTimer();
        processingTimer.start();
        DataBaseConfig dataBaseConfig = new DataBaseConfig(
            argumentProvider.getDbConnectionString(),
            argumentProvider.getUsername(),
            argumentProvider.getPassword());
        DBService dbService = new DBService(
            argumentProvider.getTableName(),
            dataBaseConfig);
        try {
            List<String[]> strings = dbService.insertIntoTable(csvAsStringArr);
            dbService.updateTable(strings);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println(processingTimer.stop() + " seconds -Time to write csv to database");
    }
}

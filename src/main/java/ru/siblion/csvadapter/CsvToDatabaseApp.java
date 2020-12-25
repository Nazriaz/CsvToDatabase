package ru.siblion.csvadapter;

import ru.siblion.csvadapter.cli.ArgumentProvider;
import ru.siblion.csvadapter.cli.HelpPrinter;
import ru.siblion.csvadapter.cli.OptionsProvider;
import ru.siblion.csvadapter.config.DataBaseConfig;
import ru.siblion.csvadapter.service.impl.CsvServiceImpl;
import ru.siblion.csvadapter.service.impl.DBService;
import ru.siblion.csvadapter.util.PricessingTimer;

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

            List<String[]> csvAsStringArr = readCsv(csvServiceImpl);

            writeToDb(csvAsStringArr);

        } else {
            helpPrinter.printAppHelp();
        }
    }

    private List<String[]> readCsv(CsvServiceImpl csvServiceImpl) {
        PricessingTimer pricessingTimer = new PricessingTimer();
        pricessingTimer.start();
        List<String[]> csvAsStringArr = csvServiceImpl.getRecords();
        System.out.println(pricessingTimer.stop() +
            " seconds - Time to read csv / size:" + csvAsStringArr.size());
        return csvAsStringArr;
    }

    private void writeToDb(List<String[]> csvAsStringArr) {
        PricessingTimer pricessingTimer = new PricessingTimer();
        pricessingTimer.start();
        DataBaseConfig dataBaseConfig = new DataBaseConfig(
            argumentProvider.getDbConnectionString(),
            argumentProvider.getUsername(),
            argumentProvider.getPassword());
        DBService dbService = new DBService(
            argumentProvider.getTableName(),
            dataBaseConfig);
//        try {
//            dbService.insertIntoTable(csvAsStringArr, 24);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        dbService.updateTable(csvAsStringArr, 24);
        System.out.println(pricessingTimer.stop() + " seconds -Time to write csv to database");
    }
}

package ru.siblion.csvadapter.cli;

import org.apache.commons.cli.HelpFormatter;

public class HelpPrinter {
    private OptionsProvider optionsProvider;

    public HelpPrinter(OptionsProvider optionsProvider) {
        this.optionsProvider = optionsProvider;
    }

    public void printAppHelp() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("Parser", optionsProvider.getOptions(), true);
    }
}

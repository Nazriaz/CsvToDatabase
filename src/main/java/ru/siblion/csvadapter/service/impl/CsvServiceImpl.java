package ru.siblion.csvadapter.service.impl;

import ru.siblion.csvadapter.service.CsvService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CsvServiceImpl implements CsvService {
    private String csvFilePath;

    public CsvServiceImpl(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    public List<String[]> getRecords() {
        List<String[]> list = new ArrayList<>();
        try (Stream<String> stringStream = Files.lines(Path.of(csvFilePath))) {
            stringStream
                .map(s -> s.split("\t", -1))
                .forEach(list::add);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public String getCsvFilePath() {
        return csvFilePath;
    }

    public void setCsvFilePath(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }
}

package ru.siblion.csvadapter.util;

public class PricessingTimer {
    private long startTime;
    public void start(){
        this.startTime=System.nanoTime();
    }
    public double stop(){
        return  ((System.nanoTime() - startTime) / 100000000)/(double)10;
    }
}

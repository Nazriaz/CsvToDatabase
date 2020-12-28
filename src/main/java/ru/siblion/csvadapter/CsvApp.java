package ru.siblion.csvadapter;

public class CsvApp {
    public static void main(String[] args) {
//        args=new String[4];
//        args[0]="-c/home/nazriaz/Downloads/Telegram Desktop/базы csv/íáºδ csv/vk2.csv";
//        args[0]="-c/home/nazriaz/Downloads/Telegram Desktop/базы csv/íáºδ csv/vk2 (3-я копия).csv";
//        args[1]="-djdbc:h2:tcp://localhost:9092/mem:osidemoportal";
//        args[2]="-tvk_user_ivan";
//        args[3]="-uOsiDemoPortal";
//        args[4]="-p";
        CsvToDatabaseApp app = new CsvToDatabaseApp();
        app.run(args);
    }
}

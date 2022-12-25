package main;

import main.interfaces.ItemState;
import main.interfaces.LogInfo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

public class Main {
    public static void main(String[] args) throws ParseException, FileNotFoundException {
        //此方法没用；可用于调试代码
        DBManipulation dbManipulation = new DBManipulation("localhost:5432/project2","postgres","123456");
        LogInfo logInfo = new LogInfo("Hua Hang", LogInfo.StaffType.SustcManager, "500622842781782190");
        dbManipulation.$import(readfile("./data/records.csv"),readfile("./data/staffs.csv"));
        //dbManipulation.getCityCount(new LogInfo("Hua Hang", LogInfo.StaffType.SustcManager, "500622842781782190"));
        //dbManipulation.getItemInfo(logInfo,"cherry-3a393");
        dbManipulation.getShipInfo(logInfo,"e63efe60");
    }

    private static String readfile(String s) throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(new FileReader(s));
        StringBuilder sb = new StringBuilder();
        reader.lines().forEach(l -> {
            sb.append(l);
            sb.append("\n");
        });
        return sb.toString();
    }


}
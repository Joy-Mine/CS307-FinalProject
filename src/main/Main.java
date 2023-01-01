package main;

import main.interfaces.ItemState;
import main.interfaces.LogInfo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws ParseException, FileNotFoundException {
        //此方法没用；可用于调试代码
        DBManipulation dbManipulation=new DBManipulation("localhost:5432/project2","postgres","POST888lbjn");
        LogInfo logInfo=new LogInfo("Zang Cong", LogInfo.StaffType.Courier,"582433470701600771");
        dbManipulation.startDB(logInfo);

        System.out.println(Arrays.toString(dbManipulation.getAllFinishItems(logInfo)));
//        System.out.println(Arrays.toString(dbManipulation.getAllItemsOnTheShip()));
    }
}
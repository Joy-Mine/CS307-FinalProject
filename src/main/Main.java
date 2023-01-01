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

        new Front();
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
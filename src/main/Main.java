package main;

import main.interfaces.ItemState;
import main.interfaces.LogInfo;

public class Main {
    public static void main(String[] args) {
        //此方法没用；可用于调试代码
        DBManipulation dbManipulation = new DBManipulation("localhost:5432/project2","postgres","123456");
    }
}
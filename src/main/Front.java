package main;

import main.interfaces.ItemInfo;
import main.interfaces.ItemState;
import main.interfaces.LogInfo;

import java.util.Objects;
import java.util.Scanner;

public class Front {

    private String username;

    private String staffType;

    private String password;

    private LogInfo logInfo = new LogInfo("test", LogInfo.StaffType.SustcManager,"123456");

    private DBManipulation dbManipulation = new DBManipulation("localhost:5432/project2","postgres","123456");

    public Front (){
        while(true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Welcome to SUSTC Logistics inquiry system");
            System.out.println("Please input your username and your password");
            System.out.println("-------------------------");
            System.out.println("Username:");
            username = scanner.nextLine();
            System.out.println("StaffType (SustcManager,\n" +
                    "        CompanyManager,\n" +
                    "        Courier,\n" +
                    "        SeaportOfficer):");
            staffType = scanner.nextLine();
            System.out.println("Password:");
            password = scanner.nextLine();
            switch (staffType) {
                case "SustcManager" -> logInfo = new LogInfo(username, LogInfo.StaffType.SustcManager, password);
                case "CompanyManager" -> logInfo = new LogInfo(username, LogInfo.StaffType.CompanyManager, password);
                case "Courier" -> logInfo = new LogInfo(username, LogInfo.StaffType.Courier, password);
                case "SeaportOfficer" -> logInfo = new LogInfo(username, LogInfo.StaffType.SeaportOfficer, password);
                default -> System.out.println("No such staffType");
            }
            ifExist(dbManipulation);
        }

    }


    public void show (){
        if (Objects.equals(staffType, "SustcManager")){
            while(true) {
                System.out.println("You are a SustcManager");
                System.out.println("The operation you can do is below:");
                System.out.println("1.get Company Count");
                System.out.println("2.get City Count");
                System.out.println("3.get Courier Count");
                System.out.println("4.get Ship Count");
                System.out.println("5.get Item Info");
                System.out.println("6.get Ship Info");
                System.out.println("7.get Container Info");
                System.out.println("8.get Staff Info");
                System.out.println("9.logout");
                Scanner scanner = new Scanner(System.in);
                String in = scanner.nextLine();
                if(Objects.equals(in, "9")){
                    break;
                }
                switch (in) {
                    case "1" -> dbManipulation.getCompanyCount(logInfo);
                    case "2" -> dbManipulation.getCityCount(logInfo);
                    case "3" -> dbManipulation.getCourierCount(logInfo);
                    case "4" -> dbManipulation.getShipCount(logInfo);
                    case "5" -> {
                        System.out.println("Please input the item_name you want to get");
                        in = scanner.nextLine();
                        dbManipulation.getItemInfo(logInfo, in);
                    }
                    case "6" -> {
                        System.out.println("Please input the ship_name you want to get");
                        in = scanner.nextLine();
                        dbManipulation.getShipInfo(logInfo, in);
                    }
                    case "7" -> {
                        System.out.println("Please input the container_code you want to get");
                        in = scanner.nextLine();
                        dbManipulation.getContainerInfo(logInfo, in);
                    }
                    case "8" -> {
                        System.out.println("Please input the staff_name you want to get");
                        in = scanner.nextLine();
                        dbManipulation.getStaffInfo(logInfo, in);
                    }
                    default -> {
                    }
                }

            }
        }
        else if (Objects.equals(staffType, "CompanyManager")){
            while (true) {
                System.out.println("You are a CompanyManager");
                System.out.println("The operation you can do is below:");
                System.out.println("1.get ImportTaxRate");
                System.out.println("2.get ExportTaxRate");
                System.out.println("3.load Item To Container");
                System.out.println("4.load Container To Ship");
                System.out.println("5.ship Start Sailing");
                System.out.println("6.unload Item");
                System.out.println("7.item Wait For Checking");
                System.out.println("8.logout");
                Scanner scanner = new Scanner(System.in);
                String in = scanner.nextLine();
                if(Objects.equals(in, "8")){
                    break;
                }
                switch (in) {
                    case "1" -> {
                        System.out.println("Please input the city and class you want to get");
                        String city = scanner.next();
                        String item_class = scanner.next();
                        dbManipulation.getImportTaxRate(logInfo, city,item_class);
                    }
                    case "2" -> {
                        System.out.println("Please input the city and class you want to get");
                        String city = scanner.next();
                        String item_class = scanner.next();
                        dbManipulation.getExportTaxRate(logInfo, city,item_class);
                    }
                    case "3" -> {
                        System.out.println("Please input the item_name and container_code you want to get");
                        String name = scanner.next();
                        String code = scanner.next();
                        dbManipulation.loadItemToContainer(logInfo, name,code);
                    }
                    case "4" -> {
                        System.out.println("Please input the ship_name and container_code you want to get");
                        String name = scanner.next();
                        String code = scanner.next();
                        dbManipulation.loadItemToContainer(logInfo, name,code);
                    }
                    case "5" -> {
                        System.out.println("Please input the ship_name you want to get");
                        in = scanner.nextLine();
                        dbManipulation.shipStartSailing(logInfo,in);
                    }
                    case "6" -> {
                        System.out.println("Please input the item_name you want to get");
                        in = scanner.nextLine();
                        dbManipulation.unloadItem(logInfo,in);
                    }
                    case "7" -> {
                        System.out.println("Please input the item_name you want to get");
                        in = scanner.nextLine();
                        dbManipulation.itemWaitForChecking(logInfo, in);
                    }
                    default -> {
                    }
                }
            }
        }
        else if (Objects.equals(staffType, "Courier")){
            while(true) {
                System.out.println("You are a Courier");
                System.out.println("The operation you can do is below:");
                System.out.println("1.newItem");
                System.out.println("2.setItemState");
                System.out.println("3.logout");
                Scanner scanner = new Scanner(System.in);
                String in = scanner.nextLine();
                if(Objects.equals(in, "3")){
                    break;
                }
                switch (in) {
                    case "1" -> {
                        System.out.println("Please input the item_info you want to new");
                        String [] info = new String[14];
                        for (int i = 0; i <14 ; i++) {
                            info[i]=scanner.next();
                        }
                        ItemInfo info1 = new ItemInfo(info[0],info[1],Double.parseDouble(info[2]),ItemState.valueOf(info[3]), new ItemInfo.RetrievalDeliveryInfo(info[4],info[5]),new ItemInfo.RetrievalDeliveryInfo(info[6],info[7]),new ItemInfo.ImportExportInfo(info[8],info[9],Double.parseDouble(info[10])),new ItemInfo.ImportExportInfo(info[11],info[12],Double.parseDouble(info[13])));
                        dbManipulation.newItem(logInfo,info1);
                    }
                    case "2" -> {
                        System.out.println("Please input the item_name and state you want to set");
                        String name = scanner.next();
                        String state = scanner.next();
                        dbManipulation.setItemState(logInfo,name, ItemState.valueOf(state));
                    }
                    default -> {
                    }
                }
            }
        }
        else if (Objects.equals(staffType, "SeaportOfficer")){
            while(true) {
                System.out.println("You are a SeaportOfficer");
                System.out.println("The operation you can do is below:");
                System.out.println("1.get All Items At Port");
                System.out.println("2.set Item Check State");
                System.out.println("3.logout");
                Scanner scanner = new Scanner(System.in);
                String in = scanner.nextLine();
                if(Objects.equals(in, "3")){
                    break;
                }
                switch (in) {
                    case "1" -> {
                        dbManipulation.getAllItemsAtPort(logInfo);
                    }
                    case "2" -> {
                        System.out.println("Please input the item_name and state you want to set");
                        String name = scanner.next();
                        boolean success = scanner.nextBoolean();
                        dbManipulation.setItemCheckState(logInfo,name,success);
                    }
                    default -> {
                    }
                }
            }
        }
    }

    public void ifExist(DBManipulation dbManipulation){
       if(dbManipulation.startDB(logInfo)){
           System.out.println("You have logged into the system!");
           System.out.println("---------------");
           show();
       }
       else {
           System.out.println("No such staff!");
           System.out.println("Please input your info again");
           System.out.println("---------------");

       }
    }

}

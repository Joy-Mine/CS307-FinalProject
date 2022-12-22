package main;

import main.interfaces.*;

import java.sql.*;
import java.util.Properties;

public class DBManipulation implements IDatabaseManipulation {

//    private String database;
    private String root,pwd;
    private String url;
    private Statement statement;
    private Connection connection;
    private ResultSet resultSet;
    private void startDB(LogInfo logInfo){
        String currentUser=logInfo.name(),currentPwd=logInfo.password();
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);
            String sql="select type from staff_type where name='"+currentUser+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || resultSet.getString(1)!=logInfo.type().toString()){
                System.out.println("No such staff.");
                closeDB();
                return;
            }
            if(logInfo.type().toString()=="Courier")
                sql="select password from courier where name='"+currentUser+"';";
            else if(logInfo.type().toString()=="CompanyManager")
                sql="select password from company_manager where name='"+currentUser+"';";
            else if(logInfo.type().toString()=="SeaportOfficer")
                sql="select password from seaport_officer where name='"+currentUser+"';";
            else if(logInfo.type().toString()=="SustcManager")
                sql="select password from department_manager where name='"+currentUser+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || resultSet.getString(1)!=currentPwd){
                System.out.println("No such staff.");
                closeDB();
                return;
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
        Properties properties=new Properties();
        if(logInfo.type()== LogInfo.StaffType.Courier){
            properties.setProperty("user","courier");
            properties.setProperty("password","123456");
        }else if(logInfo.type()== LogInfo.StaffType.CompanyManager){
            properties.setProperty("user","company_manager");
            properties.setProperty("password","123456");
        }else if(logInfo.type()== LogInfo.StaffType.SeaportOfficer){
            properties.setProperty("user","seaport_officer");
            properties.setProperty("password","123456");
        }else if(logInfo.type()== LogInfo.StaffType.SustcManager){
            properties.setProperty("user","department_manager");
            properties.setProperty("password","123456");
        }

        try {
            connection = DriverManager.getConnection(url,properties);
            connection.setAutoCommit(true);
            statement=connection.createStatement();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }
    private void closeDB(){
        try {
            statement.close();
            connection.close();
            resultSet.close();
        } catch (SQLException e) {
            System.out.println(e);
        }

    }


    public DBManipulation(String database,String root,String pwd){
        //"localhost:5432/project2"
//        this.database=database;
        this.root=root;
        this.pwd=pwd;
        this.url="jdbc:postgresql://"+database;
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);
            String sql="";//todo:这里的sql要完成建数据库，建表，建数据库用户
            statement.execute(sql);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    @Override
    public void $import(String recordsCSV, String staffsCSV) {
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);
            String sql="";//todo:这里的sql要完成导数据。 这个方法内也可以用java集合处理数据再产生sql
            statement.execute(sql);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    @Override
    public int getCompanyCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager){
            return -1;
        }
        startDB(log);
        String sql="select count(*) from company;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public int getCityCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager){
            return -1;
        }
        startDB(log);
        String sql="select count(*) from city;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public int getCourierCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager){
            return -1;
        }
        startDB(log);
        String sql="select count(*) from courier;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public int getShipCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager){
            return -1;
        }
        startDB(log);
        String sql="select count(*) from ship;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            return resultSet.getInt(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public ItemInfo getItemInfo(LogInfo log, String name) {
        if(log.type()!= LogInfo.StaffType.SustcManager){
            return null;
        }
        startDB(log);
        String sql="select * from ItemInfo where name='"+name+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return null;
            }
            ItemState state;
            ItemInfo.RetrievalDeliveryInfo retrievalInfo;
            ItemInfo.RetrievalDeliveryInfo deliveryInfo;
            ItemInfo.ImportExportInfo importInfo;
            ItemInfo.ImportExportInfo exportInfo;
            switch (resultSet.getString(4)){
                case "PickingUp":
                    state=ItemState.PickingUp;
                    break;
                case "ToExportTransporting":
                    state=ItemState.ToExportTransporting;
                    break;
                case "ExportChecking":
                    state=ItemState.ExportChecking;
                    break;
                case "ExportCheckFailed":
                    state=ItemState.ExportCheckFailed;
                    break;
                case "PackingToContainer":
                    state=ItemState.PackingToContainer;
                    break;
                case "WaitingForShipping":
                    state=ItemState.WaitingForShipping;
                    break;
                case "Shipping":
                    state=ItemState.Shipping;
                    break;
                case "UnpackingFromContainer":
                    state=ItemState.UnpackingFromContainer;
                    break;
                case "ImportChecking":
                    state=ItemState.ImportChecking;
                    break;
                case "ImportCheckFailed":
                    state=ItemState.ImportCheckFailed;
                    break;
                case "FromImportTransporting":
                    state=ItemState.FromImportTransporting;
                    break;
                case "Delivering":
                    state=ItemState.Delivering;
                    break;
                default:
                    state=null;
            }
            retrievalInfo=new ItemInfo.RetrievalDeliveryInfo(resultSet.getString(5),resultSet.getString(6));
            deliveryInfo=new ItemInfo.RetrievalDeliveryInfo(resultSet.getString(7),resultSet.getString(8));
            importInfo=new ItemInfo.ImportExportInfo(resultSet.getString(9),resultSet.getString(10),resultSet.getDouble(11));
            exportInfo=new ItemInfo.ImportExportInfo(resultSet.getString(12),resultSet.getString(13),resultSet.getDouble(14));

            ItemInfo item=new ItemInfo(resultSet.getString(1),resultSet.getString(2),resultSet.getDouble(3),
                    state,retrievalInfo,deliveryInfo,importInfo,exportInfo);
            return item;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    @Override
    public ShipInfo getShipInfo(LogInfo log, String name) {
        return null;
    }
    @Override
    public ContainerInfo getContainerInfo(LogInfo log, String code) {
        return null;
    }
    @Override
    public StaffInfo getStaffInfo(LogInfo log, String name) {
        return null;
    }

    @Override
    public double getImportTaxRate(LogInfo log, String city, String itemClass) {
        return 0;
    }

    @Override
    public double getExportTaxRate(LogInfo log, String city, String itemClass) {
        return 0;
    }

    @Override
    public boolean loadItemToContainer(LogInfo log, String itemName, String containerCode) {
        return false;
    }

    @Override
    public boolean loadContainerToShip(LogInfo log, String shipName, String containerCode) {
        return false;
    }

    @Override
    public boolean shipStartSailing(LogInfo log, String shipName) {
        return false;
    }

    @Override
    public boolean unloadItem(LogInfo log, String itemName) {
        return false;
    }

    @Override
    public boolean itemWaitForChecking(LogInfo log, String item) {
        return false;
    }

    @Override
    public boolean newItem(LogInfo log, ItemInfo item) {
        return false;
    }

    @Override
    public boolean setItemState(LogInfo log, String name, ItemState s) {
        return false;
    }


    @Override
    public String[] getAllItemsAtPort(LogInfo log) {
        return new String[0];
    }

    @Override
    public boolean setItemCheckState(LogInfo log, String itemName, boolean success) {
        return false;
    }
}

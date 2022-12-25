package main;

import main.interfaces.*;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

public class DBManipulation implements IDatabaseManipulation {

    //    private String database;
    private String root,pwd;
    private String url;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private boolean startDB(LogInfo logInfo){
        String currentUser=logInfo.name(),currentPwd=logInfo.password();
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);
            statement=connection.createStatement();
            String sql="select type from staff_type where name='"+currentUser+"';";
            resultSet=statement.executeQuery(sql);
            /*System.out.println(!resultSet.next());
            System.out.println(resultSet.getString("type"));
            System.out.println(logInfo.type());*/
            if(!resultSet.next() || !Objects.equals(resultSet.getString("type"), logInfo.type().toString())){
                System.out.println("No such staff.");
                closeDB();
                return false;
            }
            if(Objects.equals(logInfo.type().toString(), "Courier"))
                sql="select password from courier where name='"+currentUser+"';";
            else if(Objects.equals(logInfo.type().toString(), "CompanyManager"))
                sql="select password from company_manager where name='"+currentUser+"';";
            else if(Objects.equals(logInfo.type().toString(), "SeaportOfficer"))
                sql="select password from seaport_officer where name='"+currentUser+"';";
            else if(Objects.equals(logInfo.type().toString(), "SustcManager"))
                sql="select password from department_manager where name='"+currentUser+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || !Objects.equals(resultSet.getString(1), currentPwd)){
                System.out.println("Wrong password.");
                closeDB();
                return false;
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
//        closeDB();
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
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            return false;
        }
    }
    private void closeDB(){
        try {
            //statement.close();
            connection.close();
            //resultSet.close();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }


    public DBManipulation(String database,String root,String pwd){
        this.root=root;
        this.pwd=pwd;
        this.url="jdbc:postgresql://"+database;
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);
            CreateDatabase.create(connection);//todo:建数据库，建四种数据库user，建表

        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    @Override
    public void $import(String recordsCSV, String staffsCSV) throws ParseException {
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);

        } catch (SQLException e) {
            System.out.println(e);
        }
        DataImport.$import(recordsCSV,staffsCSV,connection);
        closeDB();
    }

    //SUSTC department manager
    @Override
    public int getCompanyCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return -1;
        if(!startDB(log))
            return -1;
        String sql="select count(*) from company;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            System.out.println(resultSet.getInt(1));
            return resultSet.getInt(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public int getCityCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return -1;
        if(!startDB(log))
            return -1;
        String sql="select count(*) from city;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            System.out.println(resultSet.getInt("count"));
            return resultSet.getInt("count");
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public int getCourierCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return -1;
        if(!startDB(log))
            return -1;
        String sql="select count(*) from courier;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            return resultSet.getInt(1)-1;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public int getShipCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return -1;
        if(!startDB(log))
            return -1;
        String sql="select count(*) from ship;";
        try {
            resultSet=statement.executeQuery(sql);
            resultSet.next();
            closeDB();
            return resultSet.getInt(1)-1;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public ItemInfo getItemInfo(LogInfo log, String name) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return null;
        if(!startDB(log))
            return null;
        String sql="select * from item_info where name='"+name+"';";
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
            switch (resultSet.getString("state")){
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
                case "Finish":
                    state=ItemState.Finish;
                    break;
                default:
                    state=null;
            }
            retrievalInfo=new ItemInfo.RetrievalDeliveryInfo(resultSet.getString(6),resultSet.getString(5));
            deliveryInfo=new ItemInfo.RetrievalDeliveryInfo(resultSet.getString(7),resultSet.getString(8));
            importInfo=new ItemInfo.ImportExportInfo(resultSet.getString(9),resultSet.getString(10),resultSet.getDouble(11));
            exportInfo=new ItemInfo.ImportExportInfo(resultSet.getString(12),resultSet.getString(13),resultSet.getDouble(14));

            ItemInfo item=new ItemInfo(resultSet.getString(1),resultSet.getString(2),resultSet.getDouble(3),
                    state,retrievalInfo,deliveryInfo,importInfo,exportInfo);
            closeDB();
            return item;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    @Override
    public ShipInfo getShipInfo(LogInfo log, String name) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return null;
        if(!startDB(log))
            return null;
        String sql="select * from ship where ship_name='"+name+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return null;
            }
            closeDB();
            return new ShipInfo(resultSet.getString(1),resultSet.getString(2),resultSet.getBoolean(3));
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    @Override
    public ContainerInfo getContainerInfo(LogInfo log, String code) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return null;
        if(!startDB(log))
            return null;
        String sql="select * from container where code='"+code+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return null;
            }
            ContainerInfo.Type type;
            switch (resultSet.getString(2)){
                case "Dry":
                    type= ContainerInfo.Type.Dry;
                    break;
                case "FlatRack":
                    type= ContainerInfo.Type.FlatRack;
                    break;
                case "ISOTank":
                    type= ContainerInfo.Type.ISOTank;
                    break;
                case "OpenTop":
                    type= ContainerInfo.Type.OpenTop;
                    break;
                case "Reefer":
                    type= ContainerInfo.Type.Reefer;
                    break;
                default:
                    type=null;
            }
            closeDB();
            return new ContainerInfo(type,resultSet.getString(1),resultSet.getBoolean(3));
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    @Override
    public StaffInfo getStaffInfo(LogInfo log, String name) {
        if(log.type()!= LogInfo.StaffType.SustcManager)
            return null;
        if(!startDB(log))
            return null;
        String sql="select * from staff_type where name='"+name+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return null;
            }
            String type=resultSet.getString(1);
            switch (type){
                case "Courier":
//                    table = "courier";
                    sql="select * from courier where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.Courier,resultSet.getString(7))
                            ,resultSet.getString(2),resultSet.getString(3),resultSet.getBoolean(4)
                            ,resultSet.getInt(5),resultSet.getString(6));
                case "Company Manager":
//                    table="company_manager";
                    sql="select * from company_manager where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.CompanyManager,resultSet.getString(6))
                            ,resultSet.getString(2),"",resultSet.getBoolean(3)
                            ,resultSet.getInt(4),resultSet.getString(5));
                case "Seaport Officer":
//                    table="seaport_officer";
                    sql="select * from seaport_officer where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.SeaportOfficer,resultSet.getString(6))
                            ,"",resultSet.getString(2),resultSet.getBoolean(3)
                            ,resultSet.getInt(4),resultSet.getString(5));
                case "SUSTC Department Manager":
//                    table="department_manager";
                    sql="select * from department_manager where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.SustcManager,resultSet.getString(5))
                            ,"","",resultSet.getBoolean(2)
                            ,resultSet.getInt(3),resultSet.getString(4));
            }
            closeDB();
            return null;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }

    //courier
    @Override
    public boolean newItem(LogInfo log, ItemInfo item) {//todo: 什么时候return false，还很不确定
        if(log.type()!= LogInfo.StaffType.Courier)
            return false;
        if(!startDB(log))
            return false;
        //要判断的情况：Item是否已存在；登录的这位快递员是否在这个城市，import tax和export tax跟城市有没有对上
        if(item.retrieval().city()==item.delivery().city() || item.$import().city()==item.export().city() || log.name()!=item.retrieval().courier()) {
            closeDB();
            return false;
        }
        String sql="select city from courier where name='"+item.retrieval().courier()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String courierCity=resultSet.getString(1);
            if(courierCity!=item.retrieval().city()) {
                closeDB();
                return false;
            }
            String sql1="select tax_rate from tax_rate where city_name=='"+item.$import().city()+"' and item_class=='"+item.$class()+"'; ";
            String sql2="select tax_rate from tax_rate where city_name=='"+item.export().city()+"' and item_class=='"+item.$class()+"'; ";
            resultSet=statement.executeQuery(sql1);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            Double importTaxRate=resultSet.getDouble(1);
            resultSet=statement.executeQuery(sql2);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            Double exportTaxRate=resultSet.getDouble(1);
            if(importTaxRate!=(item.$import().tax()/item.price()) || exportTaxRate!=(item.export().tax()/item.price())){
                closeDB();
                return false;
            }
            sql="select company from courier where name='"+item.retrieval().courier()+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String company=resultSet.getString(1);
            sql="insert into item_info " +
                    "(name, class, price, state, retrieval_courier, retrieval_city, delivery_city, delivery_courier, import_city, import_officer, import_tax, export_city, export_officer, export_tax, ship, container_code, company)" +
                    "values ('"+item.name()+"','"+item.$class()+"',"+item.price()+",'PickingUp','"+item.retrieval().city()+"','"+item.retrieval().courier()+"','"+item.delivery().city()+"','"+item.delivery().courier()+"','"+item.$import().city()+"','"+item.$import().officer()+"',"+item.$import().tax()+",'"+item.export().city()+"','"+item.export().officer()+"',"+item.export().tax()+",' ',' ','"+company+"');";
            statement.execute(sql);
            closeDB();
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
    @Override
    public boolean setItemState(LogInfo log, String name, ItemState s) {
        if(log.type()!=LogInfo.StaffType.Courier)
            return false;
        if(!startDB(log))
            return false;
        String sql="select * from item_info where name='"+name+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String currentState=resultSet.getString(4);
            if(log.name()==resultSet.getString(6)){//retrieval_courier, state最多是“ExportChecking”
                switch (currentState){
                    case "PickingUp":
                        if(s==ItemState.ToExportTransporting){
                            sql="update item_info set state='ToExportTransporting' where name='name';";
                            statement.execute(sql);
                            closeDB();
                            return true;
                        }else {
                            closeDB();
                            return false;
                        }
                    case "ToExportTransporting":
                        if(s==ItemState.ExportChecking){
                            sql="update item_info set state='ExportChecking' where name='name';";
                            statement.execute(sql);
                            closeDB();
                            return true;
                        }else {
                            closeDB();
                            return false;
                        }
                }
            }
            else if (log.name()==resultSet.getString(8)){//delivery_courier
                if(currentState=="FromImportTransporting")
                    if (s == ItemState.Delivering && s != ItemState.Finish) {
                        sql="update item_info set state='"+s+"' where name='name';";
                        statement.execute(sql);
                        closeDB();
                        return true;
                    }else {
                        closeDB();
                        return false;
                    }
            }
            else if(currentState.equals("FromImportTransporting") && resultSet.getString(8).equals("")){//todo:ItemState为FromImportTransporting 且 delivery courier为空的情况
                sql="select city from courier where name='"+log.name()+"';";
                ResultSet courierResultSet=statement.executeQuery(sql);
                if(!courierResultSet.next()){
                    closeDB();
                    return false;
                }
                if(courierResultSet.getString(1).equals(resultSet.getString(7))){//当前登录的courier跟item的delivery_city在同一个城市就好
                    sql="update item_info set delivery_courier='"+log.name()+"' where name='"+name+"';";
                    statement.execute(sql);
                    closeDB();
                    return true;
                }
            }
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
        closeDB();
        return false;
    }

    //company manager
    @Override
    public double getImportTaxRate(LogInfo log, String city, String itemClass) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return -1;
        if(!startDB(log))
            return -1;
        String sql="select import_tax_rate from tax_rate where city_name='"+city+"' and item_class='"+itemClass+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return -1;
            }
//            closeDB();
            return resultSet.getDouble(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public double getExportTaxRate(LogInfo log, String city, String itemClass) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return -1;
        if(!startDB(log))
            return -1;
        String sql="select export_tax_rate from tax_rate where city_name='"+city+"' and item_class='"+itemClass+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return -1;
            }
//            closeDB();
            return resultSet.getDouble(1);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return -1;
        }
    }
    @Override
    public boolean loadItemToContainer(LogInfo log, String itemName, String containerCode) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return false;
        if(!startDB(log))
            return false;
        String sql="select state from item_info where name='"+itemName+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String state=resultSet.getString(1);
            //false情况：state不是PackingToContainer；container已经满了；container已经上船了
            if(state!="PackingToContainer"){
                closeDB();
                return false;
            }
            sql="select * from container where code='"+containerCode+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getBoolean(3) || resultSet.getBoolean(4)){
                closeDB();
                return false;
            }
            String containerType=resultSet.getString(2);
            sql="update item_info set container_code='"+containerCode+"' where name='"+itemName+"';";
            statement.execute(sql);
            closeDB();
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
    @Override
    public boolean loadContainerToShip(LogInfo log, String shipName, String containerCode) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return false;
        if(!startDB(log))
            return false;
        //false情况：container已经上船了；船已经开走了
        String sql="select * from container where code='"+containerCode+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getBoolean(4)){
                closeDB();
                return false;
            }
            sql="select sailing from ship where ship_name='"+shipName+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getBoolean(3)){
                closeDB();
                return false;
            }
            sql="update item_info set state='WaitingForShipping',ship='"+shipName+"' where container_code='code';" +
                    "update container set loaded=true;";//todo:此时item的state应该是PackingToContainer吧
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
    @Override
    public boolean shipStartSailing(LogInfo log, String shipName) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return false;
        if(!startDB(log))
            return false;
        //false情况：船已经开走了
        String sql="select sailing from ship where ship_name='"+shipName+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getBoolean(1)){
                closeDB();
                return false;
            }
            sql="update item_info set state='Shipping' where ship='"+shipName+"';" +
                    "update ship set sailing=true;";
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
    @Override
    public boolean unloadItem(LogInfo log, String itemName) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return false;
        if(!startDB(log))
            return false;
        //false情况：item的state不对
        String sql="select state from item_info where name='"+itemName+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getString(1)!="Shipping"){
                closeDB();
                return false;
            }
            sql="update item_info set state='UnpackingFromContainer' where name='"+itemName+"';";//todo:暂时只修改了item的state，应该没问题吧。
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
    @Override
    public boolean itemWaitForChecking(LogInfo log, String item) {
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return false;
        if(!startDB(log))
            return false;
        //false情况：item的state不对
        String sql="select state from item_info where name='"+item+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getString(1)!="UnpackingFromContainer"){
                closeDB();
                return false;
            }
            sql="update item_info set state='ImportChecking' where name='"+item+"';";//todo:暂时只修改了item的state，应该没问题吧。
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }

    //seaport officer
    @Override
    public String[] getAllItemsAtPort(LogInfo log) {
        if(log.type()!=LogInfo.StaffType.SeaportOfficer)
            return null;
        if(!startDB(log))
            return null;
        //false情况：item的state不对
        ArrayList<String> ans=new ArrayList<>();
        String sql="select city from seaport_officer where name='"+log.name()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return null;
            }
            String city=resultSet.getString(1);
            sql="select name from item_info where state='ExportChecking' and delivery_city='"+city+"';";
            resultSet=statement.executeQuery(sql);
            while(resultSet.next())
                ans.add(resultSet.getString(1));
            sql="select name from item_info where state='ImportChecking' and delivery_city='"+city+"';";
            resultSet=statement.executeQuery(sql);
            while(resultSet.next())
                ans.add(resultSet.getString(1));
            return ans.toArray(new String[ans.size()]);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    @Override
    public boolean setItemCheckState(LogInfo log, String itemName, boolean success) {
        if(log.type()!=LogInfo.StaffType.SeaportOfficer)
            return false;
        if(!startDB(log))
            return false;
        //false情况：item的state不对或item不存在;item不在当前log的城市
        String sql="select city from seaport_officer where name='"+log.name()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String city=resultSet.getString(1);
            sql="select state,export_city,import_city from item_info where name='"+itemName+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getString(1).equals("ExportChecking") && resultSet.getString(2).equals(city)){
                if(success)
                    sql="update item_info set state='PackingToContainer',container_code='' where name='"+itemName+"';";
                else sql="update item_info set state='ExportCheckFailed' where name='"+itemName+"';";
                statement.execute(sql);
                return true;
            }
            if(resultSet.getString(1).equals("ImportChecking") && resultSet.getString(3).equals(city)){
                if(success)
                    sql="update item_info set state='FromImportTransporting',delivery_courier='' where name='"+itemName+"';";
                else sql="update item_info set state='ImportCheckFailed' where name='"+itemName+"';";
                statement.execute(sql);
                return true;
            }
            return false;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
}
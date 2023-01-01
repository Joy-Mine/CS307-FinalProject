package main;

import main.interfaces.*;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

public class DBManipulation implements IDatabaseManipulation {


    //    private String database;
    private String root,pwd;
    private String url;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    boolean startDB(LogInfo logInfo){

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
    public void $import(String recordsCSV, String staffsCSV) {
        try {
            connection = DriverManager.getConnection(url,root,pwd);
            connection.setAutoCommit(true);

        } catch (SQLException e) {
            System.out.println(e);
        }
        try {
            DataImport.$import(recordsCSV,staffsCSV,connection);
        } catch (ParseException e) {
            System.out.println(e);
        }
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
            return resultSet.getInt(1)-1;
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
            return resultSet.getInt("count")-1;
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
            retrievalInfo=new ItemInfo.RetrievalDeliveryInfo(resultSet.getString(5),resultSet.getString(6));
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
            String shipName=resultSet.getString(1),shipOwner=resultSet.getString(2);
            boolean sailing;
            sql="select count(*) from item_info where ship='"+name+"' and state='Shipping';";
//            sql="select count(*) from (select state from item_info where ship='"+name+"')s where state='Shipping';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || resultSet.getInt(1)==0)
                sailing=false;
            else sailing=true;
            closeDB();
            return new ShipInfo(shipName,shipOwner,sailing);
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
        String sql="select type from container where code='"+code+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return null;
            }
            ContainerInfo.Type type;
            switch (resultSet.getString(1)){
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
            boolean containerUsing;
            sql="select count(*) from item_info where container_code='"+code+"' and (state='PackingToContainer' or state='UnpackingFromContainer' or state='WaitingForShipping' or state='Shipping');";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || resultSet.getInt(1)==0)
                containerUsing=false;
            else containerUsing=true;
            closeDB();
            return new ContainerInfo(type,code,containerUsing);
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
            switch (resultSet.getString(2)){
                case "Courier":
//                    table = "courier";
                    sql="select * from courier where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    resultSet.next();
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.Courier,resultSet.getString(7))
                            ,resultSet.getString(2),resultSet.getString(3),resultSet.getBoolean(4)
                            ,resultSet.getInt(5),resultSet.getString(6));
                case "CompanyManager":
//                    table="company_manager";
                    sql="select * from company_manager where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    resultSet.next();
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.CompanyManager,resultSet.getString(6))
                            ,resultSet.getString(2),null,resultSet.getBoolean(3)
                            ,resultSet.getInt(4),resultSet.getString(5));
                case "SeaportOfficer":
//                    table="seaport_officer";
                    sql="select * from seaport_officer where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    resultSet.next();
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.SeaportOfficer,resultSet.getString(6))
                            ,null,resultSet.getString(2),resultSet.getBoolean(3)
                            ,resultSet.getInt(4),resultSet.getString(5));
                case "SustcManager":
//                    table="department_manager";
                    sql="select * from department_manager where name='"+name+"';";
                    resultSet= statement.executeQuery(sql);
                    resultSet.next();
                    closeDB();
                    return new StaffInfo(new LogInfo(name, LogInfo.StaffType.SustcManager,resultSet.getString(5))
                            ,null,null,resultSet.getBoolean(2)
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
    public boolean newItem(LogInfo log, ItemInfo item) {
        if(log.type()!= LogInfo.StaffType.Courier)
            return false;
        if(!startDB(log))
            return false;
        //要判断的情况：Item是否已存在; 登录的快递员所在的城市; import tax和export tax跟城市有没有对上
        if(item.retrieval().city()==item.delivery().city() || item.$import().city()==item.export().city()) {// || log.name()!=item.retrieval().courier()
            closeDB();
            return false;
        }
        String sql="select city from courier where name='"+log.name()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String courierCity=resultSet.getString(1);
            if(!courierCity.equals(item.retrieval().city())) {
                closeDB();
                return false;
            }
            String sql1="select import_tax_rate from import_tax_rate where city_name='"+item.$import().city()+"' and item_class='"+item.$class()+"'; ";
            String sql2="select export_tax_rate from export_tax_rate where city_name='"+item.export().city()+"' and item_class='"+item.$class()+"'; ";
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
            if(Math.abs(importTaxRate-(item.$import().tax()/item.price()))>0.01 || Math.abs(exportTaxRate-(item.export().tax()/item.price()))>0.01){
                closeDB();
                return false;
            }
            sql="select company from courier where name='"+log.name()+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String company=resultSet.getString(1);
            String delivery_courier=item.delivery().courier(),import_officer=item.$import().officer(),export_officer=item.export().officer();
            if(delivery_courier==null)
                delivery_courier="";
            if(item.$import().officer()==null)
                import_officer="";
            if(item.export().officer()==null)
                export_officer="";
            if(item.name()==null||item.$class()==null){
                closeDB();
                return false;
            }
            sql="insert into item_info " +
                    "(name, class, price, state, retrieval_courier, retrieval_city, delivery_city, delivery_courier, import_city, import_officer, import_tax, export_city, export_officer, export_tax, ship, container_code, company)" +
                    "values ('"+item.name()+"','"+item.$class()+"',"+item.price()+",'PickingUp','"+log.name()+"','"+item.retrieval().city()+"','"+item.delivery().city()+"','"+delivery_courier+"','"+item.$import().city()+"','"+import_officer+"',"+item.$import().tax()+",'"+item.export().city()+"','"+export_officer+"',"+item.export().tax()+",'','','"+company+"');";
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
            resultSet = statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String currentState=resultSet.getString(4);
            if(log.name().equals(resultSet.getString(6))){//retrieval_courier, state最多是“ExportChecking”
                switch (currentState){
                    case "PickingUp":
                        if(s.equals(ItemState.ToExportTransporting)){
                            sql="update item_info set state='ToExportTransporting' where name='"+name+"';";
                            statement.execute(sql);
                            closeDB();
                            return true;
                        }else {
                            closeDB();
                            return false;
                        }
                    case "ToExportTransporting":
                        if(s.equals(ItemState.ExportChecking)){
                            sql="update item_info set state='ExportChecking' where name='"+name+"';";
                            statement.execute(sql);
                            closeDB();
                            return true;
                        }else {
                            closeDB();
                            return false;
                        }
                }
            }
            else if (log.name().equals(resultSet.getString(8))){//delivery_courier
                if(currentState.equals("FromImportTransporting"))
                    if (s.equals(ItemState.Delivering) && !s.equals(ItemState.Finish) ) {
                        sql="update item_info set state='"+s+"' where name='"+name+"';";
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
        String sql="select import_tax_rate from import_tax_rate where city_name='"+city+"' and item_class='"+itemClass+"';";
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
        String sql="select export_tax_rate from export_tax_rate where city_name='"+city+"' and item_class='"+itemClass+"';";
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
            if(!state.equals("PackingToContainer")){
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
            sql="update item_info set container_code='"+containerCode+"' where name='"+itemName+"';" +
                "update container set isfull=true where code='"+containerCode+"';";
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
        System.out.println(shipName+containerCode);
        if(log.type()!=LogInfo.StaffType.CompanyManager)
            return false;
        if(!startDB(log))
            return false;
        //false情况：Item的current state不对;container已经上船了; 船已经开走了; 公司关系
        String sql="select loaded from container where code='"+containerCode+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            System.out.println(shipName+containerCode+"container loaded:"+resultSet.getBoolean(1));
            if(resultSet.getBoolean(1)){
                closeDB();
                return false;
            }
            sql="select sailing,company_name from ship where ship_name='"+shipName+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(resultSet.getBoolean(1)){
                closeDB();
                return false;
            }
            String shipCompany=resultSet.getString(2);
            sql="select state from item_info where container_code='"+containerCode+"' and company='"+shipCompany+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(!resultSet.getString(1).equals("PackingToContainer") ){
                closeDB();
                return false;
            }
            sql="select company from company_manager where name='"+log.name()+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(!shipCompany.equals(resultSet.getString(1))){
                closeDB();
                return false;
            }
            sql="update item_info set state='WaitingForShipping',ship='"+shipName+"' where container_code='"+containerCode+"' and company='"+shipCompany+"';" +
                "update container set loaded=true where code='"+containerCode+"';";
//                "update ship set sailing=true where ship_name='"+shipName+"';"
            statement.execute(sql);
            System.out.println(shipName+"fxxk"+containerCode);
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
        String sql="select sailing,company_name from ship where ship_name='"+shipName+"';";
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
            String shipCompany=resultSet.getString(2);
            sql="select company from company_manager where name='"+log.name()+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(!shipCompany.equals(resultSet.getString(1))){
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
        String sql="select state,ship,company from item_info where name='"+itemName+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(!resultSet.getString(1).equals("Shipping")){
                closeDB();
                return false;
            }
            String shipName=resultSet.getString(2),itemCompany=resultSet.getString(3);
            sql="select sailing from ship where ship_name='"+shipName+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || !resultSet.getBoolean(1)){
                closeDB();
                return false;
            }
            sql="select company from company_manager where name='"+log.name()+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || !itemCompany.equals(resultSet.getString(1))){
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
        String sql="select state,company from item_info where name='"+item+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            if(!resultSet.getString(1).equals("UnpackingFromContainer")){
                closeDB();
                return false;
            }
            String itemCompany=resultSet.getString(2);
            sql="select company from company_manager where name='"+log.name()+"';";
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next() || !itemCompany.equals(resultSet.getString(1))){
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
            sql="select name from item_info where state='ExportChecking' and export_city='"+city+"';";
            resultSet=statement.executeQuery(sql);
            while(resultSet.next())
                ans.add(resultSet.getString(1));
            sql="select name from item_info where state='ImportChecking' and import_city='"+city+"';";
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
                    sql="update item_info set state='PackingToContainer',container_code='',export_officer='"+log.name()+"' where name='"+itemName+"';";
                else sql="update item_info set state='ExportCheckFailed',export_officer='"+log.name()+"' where name='"+itemName+"';";
                statement.execute(sql);
                return true;
            }
            if(resultSet.getString(1).equals("ImportChecking") && resultSet.getString(3).equals(city)){
                if(success)
                    sql="update item_info set state='FromImportTransporting',delivery_courier='',import_officer='"+log.name()+"' where name='"+itemName+"';";
                else sql="update item_info set state='ImportCheckFailed',import_officer='"+log.name()+"' where name='"+itemName+"';";
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

    //Advanced api
    public String[] getAllFinishItems(LogInfo loginfo){
        if(!startDB(loginfo)){
            closeDB();
            return null;
        }
        String sql="select name from item_info where state='Finish';";
        try {
            resultSet=statement.executeQuery(sql);
            ArrayList<String> ans=new ArrayList<>();
            while (resultSet.next()){
                ans.add(resultSet.getString(1));
            }
            return ans.toArray(new String[ans.size()]);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    public String[] getAllItemsOnTheShip(LogInfo loginfo,ShipInfo shipInfo){
        if(!startDB(loginfo)){
            closeDB();
            return null;
        }
        String sql="select name from item_info where state='Shipping' and ship='"+shipInfo.name()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            ArrayList<String> ans=new ArrayList<>();
            while (resultSet.next()){
                ans.add(resultSet.getString(1));
            }
            return ans.toArray(new String[ans.size()]);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    public String[] getAllContainersOnTheShip(LogInfo loginfo,ShipInfo shipInfo){
        if(!startDB(loginfo)){
            closeDB();
            return null;
        }
        String sql="select container_code from item_info where state='Shipping' and ship='"+shipInfo.name()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            ArrayList<String> ans=new ArrayList<>();
            while (resultSet.next()){
                ans.add(resultSet.getString(1));
            }
            return ans.toArray(new String[ans.size()]);
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return null;
        }
    }
    public boolean changePassword(LogInfo loginfo,String newPWD){
        if(!startDB(loginfo)){
            closeDB();
            return false;
        }
        String sql="select type from staff_type where name='"+loginfo.name()+"';";
        try {
            resultSet=statement.executeQuery(sql);
            if(!resultSet.next()){
                closeDB();
                return false;
            }
            String type=resultSet.getString(1);
            switch (type){
                case "Courier":
                    type="courier";
                    break;
                case "CompanyManager":
                    type="company_manager";
                    break;
                case "SeaportOfficer":
                    type="seaport_officer";
                    break;
                case "SustcManager":
                    type="department_manager";
                    break;
            }
            sql="update "+type+" set password='"+newPWD+"' where name='"+loginfo.name()+"';";
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }
}
package main;


import main.interfaces.*;


import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.*;

public class DBManipulation2 implements IDatabaseManipulation2 {

    //    private String database;
    private String root,pwd;
    private String url;
    private Statement statement;
    private Connection connection;
    private ResultSet resultSet;

    private final int BATCH_SIZE = 1000;
    private void startDB(){
        Properties properties=new Properties();
        properties.setProperty("user",root);
        properties.setProperty("password",pwd);
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


    public DBManipulation2(String database,String root,String pwd){
        //"localhost:5432/project2","postgres","POST888lbjn"
//        this.database=database;
        this.root=root;
        this.pwd=pwd;
        this.url="jdbc:postgresql://"+database;
    }

    @Override
    public void $import(String recordsCSV, String staffsCSV) {
        String line;
        Set<String> staff_name = new HashSet<>();//primary key of staff_type
        Set<String> city = new HashSet<>();//primary key of city
        Set<String> company = new HashSet<>();//primary key of company
        Set<String> ship_name = new HashSet<>();//primary key of ship
        Set<String> container_code = new HashSet<>();//primary key of container
        Set<String> courier_name = new HashSet<>();//primary key of courier
        Set<String> company_manager_name = new HashSet<>();//primary key of company_manager
        Set<String> seaport_officer_name = new HashSet<>();//primary key of seaport_officer
        Set<String> department_manager_name = new HashSet<>();//primary key of department_manager
        Set<String> city_name_tax = new HashSet<>();//primary key of tax_rate
        Set<String> item_class = new HashSet<>();//primary key of tax_rate
        Set<String> item_name = new HashSet<>();//primary key of item_info
        ArrayList<staff_type> staff_type_list = new ArrayList<>();
        ArrayList<city> city_list = new ArrayList<>();
        ArrayList<company> company_list = new ArrayList<>();
        ArrayList<ship> ship_list = new ArrayList<>();
        ArrayList<container> container_list = new ArrayList<>();
        ArrayList<tax_rate> tax_rate_list = new ArrayList<>();
        ArrayList<courier> courier_list = new ArrayList<>();
        ArrayList<company_manager> company_manager_list = new ArrayList<>();
        ArrayList<seaport_officer> seaport_officer_list = new ArrayList<>();
        ArrayList<department_manager> department_manager_list = new ArrayList<>();
        ArrayList<item_info> item_info_list = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(recordsCSV))) {
            bufferedReader.readLine();
            while ((line = bufferedReader.readLine()) != null) {
                String[] info = line.split(",");
                if (!item_name.contains(info[0])) {
                    item_name.add(info[0]);
                    item_info_list.add(new item_info(info));
                }
                if (!city.contains(info[3]) || !city.contains(info[5]) || !city.contains(info[7]) || !city.contains(info[8])) {                //delivery courier
                    if (!city.contains(info[3])) {
                        city.add(info[3]);
                        city_list.add(new city(info[3]));
                    }
                    if (!city.contains(info[5])) {
                        city.add(info[5]);
                        city_list.add(new city(info[5]));
                    }
                    if (!city.contains(info[7])) {
                        city.add(info[7]);
                        city_list.add(new city(info[7]));
                    }
                    if (!city.contains(info[8])) {
                        city.add(info[8]);
                        city_list.add(new city(info[8]));
                    }
                }
                if (!company.contains(info[16])) {
                    company.add(info[16]);
                    company_list.add(new company(info));
                }
                if (!ship_name.contains(info[15])) {
                    ship_name.add(info[15]);
                    ship_list.add(new ship(info));
                }
                if (!container_code.contains(info[13])) {
                    container_code.add(info[13]);
                    container_list.add(new container(info));
                }
                if ((!city_name_tax.contains(info[7]))||(!city_name_tax.contains(info[8]))) {
                    if(!item_class.contains((info[1]))){
                        city_name_tax.add(info[13]);
                        item_class.add(info[1]);
                        tax_rate_list.add(new tax_rate(info));}
                }
                else if ((city_name_tax.contains(info[7]))&&(city_name_tax.contains(info[8]))){
                    if(!item_class.contains((info[1]))){
                        city_name_tax.add(info[13]);
                        item_class.add(info[1]);
                        tax_rate_list.add(new tax_rate(info));}
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(staffsCSV))) {
            bufferedReader.readLine();
            while ((line = bufferedReader.readLine()) != null) {
                String[] info = line.split(",");
                if (!staff_name.contains(info[0])) {
                    staff_name.add(info[0]);
                    staff_type_list.add(new staff_type(info));
                }
                if (!courier_name.contains(info[0])&&(Objects.equals(info[1], "Courier"))) {                //delivery courier
                    courier_name.add(info[0]);
                    courier_list.add(new courier(info));
                }
                if (!company_manager_name.contains(info[0])&&(Objects.equals(info[1], "CompanyManager"))) {                //delivery courier
                    company_manager_name.add(info[0]);
                    company_manager_list.add(new company_manager(info));
                }
                if (!seaport_officer_name.contains(info[0])&&(Objects.equals(info[1], "SeaportOfficer"))) {                //delivery courier
                    seaport_officer_name.add(info[0]);
                    seaport_officer_list.add(new seaport_officer(info));
                }
                if (!department_manager_name.contains(info[0])&&(Objects.equals(info[1], "SustcManager"))) {                //delivery courier
                    department_manager_name.add(info[0]);
                    department_manager_list.add(new department_manager(info));
                }
            }

        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }


        String sql = "insert into staff_type (name,type) values (?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (staff_type staff_type : staff_type_list) {

                preparedStatement.setString(1, staff_type.name);
                preparedStatement.setString(2, staff_type.type);
                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into city (name) values (?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (city city1 : city_list) {

                preparedStatement.setString(1, city1.name);
                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }



        sql = "insert into company (name) values (?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (company company1 : company_list) {

                preparedStatement.setString(1, company1.name);
                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into ship (ship_name,company_name,sailing) values (?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (ship ship : ship_list) {
                preparedStatement.setString(1, ship.name);
                preparedStatement.setString(2, ship.company_name);
                preparedStatement.setBoolean(3, ship.sailing);
                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into container (code,type,full,loaded) values (?,?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (container container : container_list) {
                preparedStatement.setString(1, container.code);
                preparedStatement.setString(2, container.type);
                preparedStatement.setBoolean(3, container.full);
                preparedStatement.setBoolean(4, container.loaded);
                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into tax_rate (city_name,item_class,tax_rate) values (?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (tax_rate tax_rate : tax_rate_list) {
                preparedStatement.setString(1, tax_rate.city_name);
                preparedStatement.setString(2, tax_rate.item_class);
                preparedStatement.setFloat(3, tax_rate.tax_rate);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into courier (name,company,city,gender,age,phone,password) values (?,?,?,?,?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (courier courier : courier_list) {
                preparedStatement.setString(1, courier.name);
                preparedStatement.setString(2, courier.company);
                preparedStatement.setString(3, courier.city);
                preparedStatement.setBoolean(4, courier.gender);
                preparedStatement.setInt(5, courier.age);
                preparedStatement.setString(6, courier.phone);
                preparedStatement.setString(7, courier.password);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into company_manager (name,company,gender,age,phone,password) values (?,?,?,?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (company_manager company_manager : company_manager_list) {
                preparedStatement.setString(1, company_manager.name);
                preparedStatement.setString(2,company_manager.company);
                preparedStatement.setBoolean(3, company_manager.gender);
                preparedStatement.setInt(4, company_manager.age);
                preparedStatement.setString(5, company_manager.phone);
                preparedStatement.setString(6, company_manager.password);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into seaport_officer (name,city,gender,age,phone,password) values (?,?,?,?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (seaport_officer seaport_officer : seaport_officer_list) {
                preparedStatement.setString(1, seaport_officer.name);
                preparedStatement.setString(2, seaport_officer.city);
                preparedStatement.setBoolean(3, seaport_officer.gender);
                preparedStatement.setInt(4, seaport_officer.age);
                preparedStatement.setString(5, seaport_officer.phone);
                preparedStatement.setString(6, seaport_officer.password);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into department_manager (name,gender,age,phone,password) values (?,?,?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (department_manager department_manager : department_manager_list) {
                preparedStatement.setString(1, department_manager.name);
                preparedStatement.setBoolean(2, department_manager.gender);
                preparedStatement.setInt(3, department_manager.age);
                preparedStatement.setString(4, department_manager.phone);
                preparedStatement.setString(5, department_manager.password);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into item_info (name,class,price,state,retrieval_courier,retrieval_city,delivery_city,delivery_courier,import_city,import_officer,import_tax,export_city,export_officer,export_tax,ship,container_code,company) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (item_info item_info : item_info_list) {
                preparedStatement.setString(1, item_info.name);
                preparedStatement.setString(2, item_info.item_class);
                preparedStatement.setFloat(3, item_info.price);
                preparedStatement.setString(4, item_info.state);
                preparedStatement.setString(5, item_info.retrieval_courier);
                preparedStatement.setString(6, item_info.retrieval_city);
                preparedStatement.setString(7, item_info.delivery_city);
                preparedStatement.setString(8, item_info.delivery_courier);
                preparedStatement.setString(9, item_info.import_city);
                preparedStatement.setString(10, item_info.import_officer);
                preparedStatement.setFloat(11, item_info.import_tax);
                preparedStatement.setString(12, item_info.export_city);
                preparedStatement.setString(13, item_info.export_officer);
                preparedStatement.setFloat(14, item_info.export_tax);
                preparedStatement.setString(15, item_info.ship);
                preparedStatement.setString(16, item_info.container_code);
                preparedStatement.setString(17, item_info.company);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

    }
        @Override
    public int getCompanyCount(LogInfo log) {
        if(log.type()!= LogInfo.StaffType.SustcManager){
            return -1;
        }
        startDB();
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
        startDB();
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
        startDB();
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
        startDB();
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
        startDB();
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
        if(log.type()!= LogInfo.StaffType.Courier){
            return false;
        }
        startDB();
        String sql="select * from Iteminfo where name = ?;";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, item.name());
            resultSet = preparedStatement.executeQuery();
            if(!resultSet.next()){

                closeDB();
                return true;
            }
            else {
                closeDB();
                return false;
            }

        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }
    }

    @Override
    public boolean setItemState(LogInfo log, String name, ItemState s) {
        if(log.type()!= LogInfo.StaffType.Courier){
            return false;
        }
        startDB();
        String sql="select * from Iteminfo where name = ?;";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            resultSet = preparedStatement.executeQuery();
            if(!resultSet.next()){

                closeDB();
                return true;
            }
            else {
                closeDB();
                return false;
            }

        } catch (SQLException e) {
            System.out.println(e);
            closeDB();
            return false;
        }

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


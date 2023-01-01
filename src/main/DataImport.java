package main;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class DataImport {
    private static final int BATCH_SIZE = 1000;
    public static void $import(String recordsCSV, String staffsCSV, Connection connection) throws ParseException {

        String line;
        Set<String> staff_name = new HashSet<>();//primary key of staff_type
        Set<String> city = new HashSet<>();//primary key of city
        Set<String> company = new HashSet<>();//primary key of company
        Set<String> ship_name = new HashSet<>();//primary key of ship
        Set<String> ship_sailing = new HashSet<>();//primary key of ship
        Set<String> container_code = new HashSet<>();//primary key of container
        Set<String> courier_name = new HashSet<>();//primary key of courier
        Set<String> company_manager_name = new HashSet<>();//primary key of company_manager
        Set<String> seaport_officer_name = new HashSet<>();//primary key of seaport_officer
        Set<String> department_manager_name = new HashSet<>();//primary key of department_manager
        Set<String> city_name_import_tax_and_class = new HashSet<>();//primary key of tax_rate
        Set<String> city_name_export_tax_and_class = new HashSet<>();//primary key of tax_rate
        Set<String> item_name = new HashSet<>();//primary key of item_info
        ArrayList<staff_type> staff_type_list = new ArrayList<>();
        ArrayList<city> city_list = new ArrayList<>();
        ArrayList<company> company_list = new ArrayList<>();
        ArrayList<ship> ship_list = new ArrayList<>();
        ArrayList<container> container_list = new ArrayList<>();
        ArrayList<import_tax_rate> import_tax_rate_list = new ArrayList<>();
        ArrayList<export_tax_rate> export_tax_rate_list = new ArrayList<>();
        ArrayList<courier> courier_list = new ArrayList<>();
        ArrayList<company_manager> company_manager_list = new ArrayList<>();
        ArrayList<seaport_officer> seaport_officer_list = new ArrayList<>();
        ArrayList<department_manager> department_manager_list = new ArrayList<>();
        ArrayList<item_info> item_info_list = new ArrayList<>();
        String [] line_array = recordsCSV.split("\n");

        String[] empty_info=new String[20];
        for(int i=0;i<20;++i)
            empty_info[i]=new String();

        int i = 0;
            while (line_array[i] != null) {
                i++;
                if(i==line_array.length){
                    break;
                }
                line = line_array[i];

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
                if (!city_name_import_tax_and_class.contains(info[8]+""+info[1])) {
                    city_name_import_tax_and_class.add(info[8]+""+info[1]);
                    import_tax_rate_list.add(new import_tax_rate(info,info[8]));
                }
                if(!city_name_export_tax_and_class.contains(info[7]+""+info[1])){
                    city_name_export_tax_and_class.add(info[7]+""+info[1]);
                    export_tax_rate_list.add(new export_tax_rate(info,info[7]));
                }
            }



            line_array = staffsCSV.split("\n");
            int j = 0;
            while (line_array[j] != null) {
                j++;
                if(j==line_array.length){
                    break;
                }
                line = line_array[j];

                String[] info = line.split(",");
                if (!staff_name.contains(info[0])) {
                    staff_name.add(info[0]);
                    staff_type_list.add(new staff_type(info));
                }
                if (!courier_name.contains(info[0])&&(Objects.equals(info[1], "Courier"))) {                //delivery courier
                    courier_name.add(info[0]);
                    courier_list.add(new courier(info));
                }
                if (!company_manager_name.contains(info[0])&&(Objects.equals(info[1], "Company Manager"))) {                //delivery courier
                    company_manager_name.add(info[0]);
                    company_manager_list.add(new company_manager(info));
                }
                if (!seaport_officer_name.contains(info[0])&&(Objects.equals(info[1], "Seaport Officer"))) {                //delivery courier
                    seaport_officer_name.add(info[0]);
                    seaport_officer_list.add(new seaport_officer(info));
                }
                if (!department_manager_name.contains(info[0])&&(Objects.equals(info[1], "SUSTC Department Manager"))) {                //delivery courier
                    department_manager_name.add(info[0]);
                    department_manager_list.add(new department_manager(info));
                }
            }




        staff_type_list.add(new staff_type());
        String sql = "insert into staff_type (name,type) values (?,?)";
        try {
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        city_list.add(new city(""));
        sql = "insert into city (name) values (?)";
        try {
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        company_list.add(new company(empty_info));
        sql = "insert into company (name) values (?)";
        try {
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

//        ship_list.add(new ship(empty_info));
        sql = "insert into ship (ship_name,company_name,sailing) values (?,?,?)";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (ship ship : ship_list) {
                preparedStatement.setString(1, ship.name);
                preparedStatement.setString(2, ship.company_name);
                preparedStatement.setBoolean(3, ship.sailing);
//                preparedStatement.setBoolean(3, false);
                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

//        container_list.add(new container(empty_info));
        sql = "insert into container (code,type,isfull,loaded) values (?,?,?,?)";
        try {
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into import_tax_rate (city_name,item_class,import_tax_rate) values (?,?,?)";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (import_tax_rate import_tax_rate : import_tax_rate_list) {
                preparedStatement.setString(1, import_tax_rate.city_name);
                preparedStatement.setString(2, import_tax_rate.item_class);
                preparedStatement.setFloat(3, import_tax_rate.import_tax_rate);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into export_tax_rate (city_name,item_class,export_tax_rate) values (?,?,?)";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (export_tax_rate export_tax_rate : export_tax_rate_list) {
                preparedStatement.setString(1, export_tax_rate.city_name);
                preparedStatement.setString(2, export_tax_rate.item_class);
                preparedStatement.setFloat(3, export_tax_rate.export_tax_rate);

                preparedStatement.addBatch();
                if (++cnt % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                }

            }
            if (cnt % BATCH_SIZE != 0)
                preparedStatement.executeBatch();
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        courier_list.add(new courier());
        sql = "insert into courier (name,company,city,gender,age,phone,password) values (?,?,?,?,?,?,?)";
        try {
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        seaport_officer_list.add(new seaport_officer());
        sql = "insert into seaport_officer (name,city,gender,age,phone,password) values (?,?,?,?,?,?)";
        try {
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
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
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                connection.rollback();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        sql = "insert into item_info (name,class,price,state,retrieval_city,retrieval_courier,delivery_city,delivery_courier,import_city,import_officer,import_tax,export_city,export_officer,export_tax,ship,container_code,company) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cnt=0;
            for (item_info item_info : item_info_list) {
                preparedStatement.setString(1, item_info.name);
                preparedStatement.setString(2, item_info.item_class);
                preparedStatement.setFloat(3, item_info.price);
                preparedStatement.setString(4, item_info.state);
                preparedStatement.setString(5,item_info.retrieval_city);
                preparedStatement.setString(6,item_info.retrieval_courier);
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
            connection.setAutoCommit(false);
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
}
    class staff_type{
        public String name;
        public String type;
        public staff_type(String[] info) {
            name = info[0];
            if(Objects.equals(info[1], "Courier")){
                type = "Courier";
            }
            else if (Objects.equals(info[1], "Company Manager")){
                type = "CompanyManager";
            }
            else if (Objects.equals(info[1], "Seaport Officer")){
                type = "SeaportOfficer";
            }
            else if (Objects.equals(info[1], "SUSTC Department Manager")){
                type = "SustcManager";
            }
        }
        public staff_type(){
            name = "";
            type = "";
        }
    }

    class city {
        public String name;
        public city(String info1) {   // for retrieval couriers
            name = info1;
        }
    }

    class company {
        public String name;
        public company(String []info) {
            name=info[16];
        }
    }

    class ship {
        public String name;
        public String company_name;
        public boolean sailing;
        public ship(String[] info) {
                if(info[17].equals("Shipping"))
                    sailing=true;
                else sailing=false;
            name=info[15];
            company_name = info[16];
        }
    }

    class container {
        public String code;
        public String type;
        public boolean full;
        public boolean loaded;
        public container(String[] info) {
            code=info[13];
            switch (info[14]){
                case "Dry Container":
                    type = "Dry";
                    break;
                case "Flat Rack Container":
                    type = "FlatRack";
                    break;
                case "ISO Tank Container":
                    type = "ISOTank";
                    break;
                case "Open Top Container":
                    type = "OpenTop";
                    break;
                case "Reefer Container":
                    type = "Reefer";
                    break;
                case "":
                    type = "";
                    break;
                default:
                    break;
            }
            full = false;
            loaded = false;
        }
    }

    class import_tax_rate{
        public String city_name;
        public String item_class;
        public float import_tax_rate;
        public import_tax_rate(String[] info,String info1) {
            city_name=info1;
            item_class=info[1];
            import_tax_rate = Float.parseFloat(info[10])/Float.parseFloat(info[2]);
        }
    }

    class export_tax_rate{
        public String city_name;
        public String item_class;
        public float export_tax_rate;
        public export_tax_rate(String[] info,String info1) {
        city_name=info1;
        item_class=info[1];
        export_tax_rate = Float.parseFloat(info[9])/Float.parseFloat(info[2]);
    }
}

    class courier{
        public String name;
        public String company;
        public String city;
        public boolean gender;
        public int age;
        public String phone;
        public String password;
        public courier(String[] info) {
            name = info[0];
            company = info[2];
            city = info[3];
            gender = Objects.equals(info[4], "female");
            age = Integer.parseInt(info[5]);
            phone = info[6];
            password = info[7];
        }
        public courier() {
            name = "";
            company = "";
            city = "";
            gender = false;
            age = 0;
            phone = "";
            password = "";
        }
    }

    class company_manager{
        public String name;
        public String company;
        public boolean gender;
        public int age;
        public String phone;
        public String password;
        public company_manager(String [] info){
            name = info[0];
            company = info[2];
            gender = Objects.equals(info[4], "female");
            age = Integer.parseInt(info[5]);
            phone = info[6];
            password = info[7];
        }
    }

    class seaport_officer{
        public String name;
        public String city;
        public boolean gender;
        public int age;
        public String phone;
        public String password;

        public seaport_officer(String [] info){
            name = info[0];
            city = info[3];
            gender = Objects.equals(info[4], "female");
            age = Integer.parseInt(info[5]);
            phone = info[6];
            password = info[7];
        }
        public seaport_officer(){
            name = "";
            city = "";
            gender = false;
            age = 0;
            phone = "";
            password = "";
        }
    }

    class department_manager{
        public String name;
        public boolean gender;
        public int age;
        public String phone;
        public String password;
        public department_manager(String [] info){
            name = info[0];
            gender = Objects.equals(info[4], "female");
            age = Integer.parseInt(info[5]);
            phone = info[6];
            password = info[7];
        }
    }

    class item_info{
        public String name;
        public String item_class;
        public float price;
        public String state;
        public String retrieval_courier;
        public String retrieval_city;
        public String delivery_city;
        public String delivery_courier;
        public String import_city;
        public String import_officer;
        public float import_tax;
        public String export_city;
        public String export_officer;
        public float export_tax;
        public String ship;
        public String container_code;
        public String company;

        public item_info(String [] info){
            name = info[0];
            item_class = info [1];
            price = Float.parseFloat(info[2]);

            switch (info[17]){
                case "Picking-Up":
                    state = "PickingUp";
                    break;
                case "To-Export Transporting":
                    state = "ToExportTransporting";
                    break;
                case "Export Checking":
                    state = "ExportChecking";
                    break;
                case "Export Check Fail":
                    state = "ExportCheckFailed";
                    break;
                case "Packing to Container":
                    state = "PackingToContainer";
                    break;
                case "Waiting for Shipping":
                    state = "WaitingForShipping";
                    break;
                case "Shipping":
                    state = "Shipping";
                    break;
                case "Unpacking from Container":
                    state = "UnpackingFromContainer";
                    break;
                case "Import Checking":
                    state = "ImportChecking";
                    break;
                case "Import Check Fail":
                    state = "ImportCheckFailed";
                    break;
                case "From-Import Transporting":
                    state = "FromImportTransporting";
                    break;
                case "Delivering":
                    state = "Delivering";
                    break;
                case "Finish":
                    state = "Finish";
                    break;
                default:
                    state = "";
                    break;
            }
//            state = info[17];

            retrieval_courier = info[4];
            retrieval_city = info[3];
            delivery_city = info[5];
            delivery_courier = info[6];
            import_city = info[8];
            import_officer = info[12];
            import_tax = Float.parseFloat(info[10]);
            export_city = info[7];
            export_officer = info[11];
            export_tax = Float.parseFloat(info[9]);
            ship = info[15];
            container_code = info[13];
            company = info[16];
        }
    }

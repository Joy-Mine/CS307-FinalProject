package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
    public static void $import(String recordsCSV, String staffsCSV, Connection connection){

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
}
    class staff_type{
        public String name;
        public String type;
        public staff_type(String[] info) throws ParseException {
            name = info[0];
            type = info[1];
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
            name=info[15];
            company_name = info[16];
            sailing = false;
        }
    }

    class container {
        public String code;
        public String type;
        public boolean full;
        public boolean loaded;

        public container(String[] info) {
            code=info[13];
            type=info[14];
            full = false;
            loaded = false;
        }
    }

    class tax_rate{
        public String city_name;
        public String item_class;
        public float tax_rate;

        public tax_rate(String[] info) {

            city_name=info[24];
            item_class=info[23];
            tax_rate=0;
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
            state = info[17];
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

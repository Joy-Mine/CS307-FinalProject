package main;

import java.sql.*;

public class CreateDatabase {
    public static void create(Connection connection) throws SQLException {
        //todo:建数据库，建四种数据库user，建表

        Statement statement=connection.createStatement();
        ResultSet resultSet=statement.executeQuery("select count(*) from pg_user where usename='courier';");
        resultSet.next();

        PreparedStatement ps;
        if(resultSet.getInt(1)==0){
        /*ps=connection.prepareStatement("CREATE DATABASE  project2");
        ps.executeUpdate();//执行sql语句*/
            ps=connection.prepareStatement("CREATE USER courier WITH PASSWORD '123456' superuser ");
            ps.executeUpdate();
            ps=connection.prepareStatement("CREATE USER company_manager WITH PASSWORD '123456' superuser ");
            ps.executeUpdate();
            ps=connection.prepareStatement("CREATE USER seaport_officer WITH PASSWORD '123456' superuser ");
            ps.executeUpdate();
            ps=connection.prepareStatement("CREATE USER department_manager PASSWORD '123456'" +
                    "SUPERUSER ;");
            ps.executeUpdate();
        }

        /*ps=connection.prepareStatement("SHOW TABLES LIKE \"student\"");
        rs=ps.executeQuery();
        if(rs.next())
            System.out.println("1");
        else {
            ps=connection.prepareStatement("CREATE TABLE student (id INT(11) PRIMARY KEY ,name VARCHAR(25) ) " );
            ps.executeUpdate();
            System.out.println("0");
        }*/
        ps=connection.prepareStatement("""
                create table if not exists staff_type(
                    name text primary key unique,
                    type text

                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists city(
                    name text primary key unique
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists company(
                    name text primary key unique
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists ship(
                    ship_name text primary key unique,
                    company_name text not null,
                    sailing bool not null,
                    constraint ship_FK
                        foreign key (company_name)
                            references company(Name)
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists container(
                    code text primary key unique,
                    type text not null ,
                    isfull bool not null,
                    loaded bool not null
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists import_tax_rate(
                    city_name text not null,
                    item_class text not null,
                    import_tax_rate numeric,
                    primary key (city_name,item_class),
                    constraint Import_officer_FK
                        foreign key (city_name)
                            references city(name)
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists export_tax_rate(
                      city_name text not null,
                      item_class text not null,
                      export_tax_rate numeric,
                      primary key (city_name,item_class),
                      constraint Export_officer_FK
                          foreign key (city_name)
                              references city(name)
                  );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists courier(
                    name text not null ,
                    constraint courier_FK
                        foreign key (name)
                            references staff_type(Name) ,
                    company text not null ,
                    city text not null ,
                    gender bool not null,
                    age integer not null ,
                    phone text not null,
                    password text not null,
                    primary key (Name)
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists company_manager(
                    name text not null ,
                    constraint company_manager_FK
                        foreign key (name)
                            references staff_type(Name) ,
                    company text not null ,
                    gender bool not null,
                    age integer not null ,
                    phone text not null,
                    password text not null,
                    primary key (Name)
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists seaport_officer(
                    name text not null ,
                    constraint seaport_officer_FK
                        foreign key (name)
                            references staff_type(Name) ,
                    city text not null ,
                    gender bool not null,
                    age integer not null ,
                    phone text not null,
                    password text not null,
                    primary key (Name)
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists department_manager(
                    name text not null ,
                    constraint Courier_FK
                        foreign key (name)
                            references staff_type(Name) ,
                    gender bool not null,
                    age integer not null ,
                    phone text not null,
                    password text not null,
                    primary key (Name)
                );""");
        ps.executeUpdate();
        ps=connection.prepareStatement("""
                create table if not exists item_info(
                      name text primary key ,
                      class text not null ,
                      price numeric not null ,
                      state text,
                      retrieval_city text not null ,
                      retrieval_courier text ,
                      constraint RetrievalCourier_FK
                          foreign key (retrieval_courier)
                              references courier(name) ,
                      constraint Retrievalcity_FK
                          foreign key (retrieval_city)
                              references city(name) ,
                      delivery_city text ,
                      delivery_courier text,
                      constraint DeliveryCity_FK
                          foreign key (delivery_courier)
                              references courier(Name) ,
                      constraint DeliveryCourier_FK
                          foreign key (delivery_city)
                              references city(Name) ,
                      import_city text,
                      import_officer text,
                      constraint Import_officer_FK
                          foreign key (import_officer)
                              references seaport_officer(Name) ,
                      constraint Import_city_FK
                          foreign key (import_city)
                              references city(Name) ,
                      import_tax numeric,
                      export_city text,
                      export_officer text,
                      constraint Export_officer_FK
                          foreign key (export_officer)
                              references seaport_officer(Name),
                      constraint Export_city_FK
                          foreign key (export_city)
                              references city(Name),
                      export_tax numeric,
                      ship text ,
                      constraint ship_FK
                          foreign key (ship)
                              references ship(ship_name),
                      container_code text ,
                      constraint container_code_FK
                          foreign key (container_code)
                              references container(code),
                      company text,
                      constraint company_FK
                          foreign key (company)
                              references company(name)
                  );""");
        ps.executeUpdate();
    }
}

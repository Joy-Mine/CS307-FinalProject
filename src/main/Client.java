package main;



import main.interfaces.ItemInfo;
import main.interfaces.LogInfo;
import main.interfaces.ShipInfo;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class Client {
    static DBManipulation dbManipulation=new DBManipulation("localhost:5432/project2","postgres","POST888lbjn");
    public static void main(String[] args) {
        login();
    }
    public static void login(){
        JFrame frame=new JFrame("SUSTC数据库 客户端");
        frame.setBounds(500,500,500,400);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        JPanel panel=new JPanel(new GridLayout(3,1,50,50));
//        panel.setLayout(new GridLayout(3,1,10,10));
        panel.setSize(500,500);
        panel.setLayout(null);

        JLabel userLabel = new JLabel("User:");
        userLabel.setBounds(10,80,80,25);
        panel.add(userLabel);
        JTextField userText = new JTextField(30);
        userText.setBounds(100,80,165,25);
        panel.add(userText);

        JLabel passwordLabel = new JLabel("Password:");
        passwordLabel.setBounds(10,125,80,25);
        panel.add(passwordLabel);
        JPasswordField passwordText = new JPasswordField(30);
        passwordText.setBounds(100,125,165,25);
        panel.add(passwordText);

        JButton loginButton = new JButton("login");
        loginButton.setBounds(10, 200, 80, 25);
        panel.add(loginButton);

        loginButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String user=userText.getText(),pwd=passwordText.getText();
                String type=dbManipulation.login(user,pwd);
                if(type.equals("")){
                    JOptionPane.showMessageDialog(panel,"用户名或密码错误，请重新输入","用户名或密码错误",JOptionPane.ERROR_MESSAGE);
                }
                else {
                    frame.setLayout(new FlowLayout());
                    JPanel panel3=new JPanel();
                    JLabel label=new JLabel("Hi, SustcManager!");
                    panel3.add(label);

                    JPanel panel2=new JPanel();
                    if(type.equals("SustcManager")){
                        panel2.setLayout(new GridLayout(6,2,5,5));
                        JButton getCompanyCountButton=new JButton("getCompanyCount");
                        JButton getCityCountButton=new JButton("getCityCount");
                        JButton getCourierCountButton=new JButton("getCourierCount");
                        JButton getShipCountButton=new JButton("getShipCount");
                        JButton getItemInfoButton=new JButton("getItemInfo");
                        JButton getShipInfoButton=new JButton("getShipInfo");
                        JButton getContainerInfoButton=new JButton("getContainerInfo");
                        JButton getStaffInfoButton=new JButton("getStaffInfo");
                        panel2.add(getCompanyCountButton);
                        panel2.add(getCityCountButton);
                        panel2.add(getCourierCountButton);
                        panel2.add(getShipCountButton);
                        panel2.add(getItemInfoButton);
                        panel2.add(getShipInfoButton);
                        panel2.add(getContainerInfoButton);
                        panel2.add(getStaffInfoButton);

                        getCompanyCountButton.addActionListener(new ActionListener() {
                            @Override
                            public void actionPerformed(ActionEvent e) {
                                int k=dbManipulation.getCompanyCount(new LogInfo(user, LogInfo.StaffType.SustcManager,pwd));
                                JOptionPane.showMessageDialog(panel2,"CompanyCount: "+k,"getCompanyCount",JOptionPane.INFORMATION_MESSAGE);
                            }
                        });
                        getCityCountButton.addActionListener(new ActionListener() {
                            @Override
                            public void actionPerformed(ActionEvent e) {
                                int k=dbManipulation.getCityCount(new LogInfo(user,LogInfo.StaffType.SustcManager,pwd));
                                JOptionPane.showMessageDialog(panel2,"CityCount: "+k,"getCityCount",JOptionPane.INFORMATION_MESSAGE);
                            }
                        });
                        getCourierCountButton.addActionListener(new ActionListener() {
                            @Override
                            public void actionPerformed(ActionEvent e) {
                                int k=dbManipulation.getCourierCount(new LogInfo(user, LogInfo.StaffType.SustcManager,pwd));
                                JOptionPane.showMessageDialog(panel2,"CourierCount: "+k,"getCourierCount",JOptionPane.INFORMATION_MESSAGE);
                            }
                        });
                        getShipCountButton.addActionListener(new ActionListener() {
                            @Override
                            public void actionPerformed(ActionEvent e) {
                                int k=dbManipulation.getShipCount(new LogInfo(user, LogInfo.StaffType.SustcManager,pwd));
                                JOptionPane.showMessageDialog(panel2,"ShipCount: "+k,"getShipCount",JOptionPane.INFORMATION_MESSAGE);
                            }
                        });
                        getItemInfoButton.addActionListener(new ActionListener() {
                            @Override
                            public void actionPerformed(ActionEvent e) {
                                String name=JOptionPane.showInputDialog(panel2,"请输入物品名：");
                                ItemInfo itemInfo=dbManipulation.getItemInfo(new LogInfo(user, LogInfo.StaffType.SustcManager,pwd),name);
                                String ans="name: "+itemInfo.name()+"; class: "+itemInfo.$class()+"; state: "+itemInfo.state()+"; retrieval city: "+itemInfo.retrieval().city() +
                                        "; retrieval courier: "+itemInfo.retrieval().courier()+"; delivery city: "+itemInfo.delivery().city()+"; delivery courier: "+itemInfo.delivery().courier()+
                                        "; import city: "+itemInfo.$import().city()+"; import officer: "+itemInfo.$import().officer()+"; import tax: "+itemInfo.$import().tax()+
                                        "; export city: "+itemInfo.export().city()+"; export officer: "+itemInfo.export().officer()+"; export tax: "+itemInfo.export().tax();
                                JOptionPane.showMessageDialog(panel2,ans,"getShipCount",JOptionPane.INFORMATION_MESSAGE);
                            }
                        });
                        getShipInfoButton.addActionListener(new ActionListener() {
                            @Override
                            public void actionPerformed(ActionEvent e) {
                                String shipName=JOptionPane.showInputDialog(panel2,"请输入船名：");
                                ShipInfo shipInfo=dbManipulation.getShipInfo(new LogInfo(user, LogInfo.StaffType.SustcManager,pwd),shipName);
                                String ans="name: "+shipInfo.name()+"; owner: "+shipInfo.owner()+"; sailing: "+shipInfo.sailing();
                                JOptionPane.showMessageDialog(panel2,ans,"getShipCount",JOptionPane.INFORMATION_MESSAGE);
                            }
                        });
                    }
                    else if(type.equals("Courier")){
                        panel2.setLayout(new GridLayout(2,2,5,5));
                        JButton newItemButton=new JButton("newItem");
                        JButton setItemStateButton=new JButton("setItemState");
                        panel2.add(newItemButton);
                        panel2.add(setItemStateButton);
                    }
                    else if(type.equals("CompanyManager")){
                    }
                    else if(type.equals("SeaportOfficer")){
                    }
                    JButton getAllFinishItemsButton=new JButton("getAllFinishItems");
                    JButton getAllItemsOnTheShipButton=new JButton("getAllItemsOnTheShip");
                    JButton getAllContainersOnTheShip=new JButton("getAllContainersOnTheShip");
                    JButton changePassword=new JButton("changePassword");
                    panel2.add(getAllFinishItemsButton);
                    panel2.add(getAllItemsOnTheShipButton);
                    panel2.add(getAllContainersOnTheShip);
                    panel2.add(changePassword);
                    frame.add(panel3);
                    frame.add(panel2);
                    frame.validate();
                    frame.repaint();
                }
            }
        });


        frame.add(panel);
        frame.setVisible(true);
    }
}

package main.interfaces;

import java.text.ParseException;
import java.util.Objects;

public interface IDatabaseManipulation2 extends ISustcManager, ICourier, ICompanyManager, ISeaportOfficer {
	// Your implemented class shall have constructor like:
	//		Constructor(String database, String root, String pass)
	// See project description for more details.

	void $import(String recordsCSV, String staffsCSV);

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
}

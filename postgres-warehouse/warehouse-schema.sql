-- fact-table
1.  sk_client int(11)
2.  sk_product int(11)
3.  sk_employees int(11)
4.  sk_office int(11)
5.  sk_order_time int(11)
6.  price_one double
7.  quantity int(11)

CREATE TABLE fact_table (
    sk_client INT,
    sk_product INT,
    sk_employee INT,
    sk_office INT,
    sk_order_time INT,
    price_one DOUBLE PRECISION,
    quantity INT
);

-- dim-client
1.  sk_client int(11)
2.  cliend_id int(11)
3.  client_name varchar(50)
4.  first_name varchar(50)
5.  last_name varchar(50)
6.  phone varchar(50)
7.  address_line_1 varchar(50)
8.  address_line_2 varchar(50)
9.  city varchar(50)
10.  state varchar(50)
11.  zip_code varchar(15)
12.  country varchar(50)
13.  employee_id int(11)
14.  credit_limit double
15.  check_id varchar(50)

CREATE TABLE dim_client (
    sk_client INT PRIMARY KEY,
    client_id INT,
    client_name VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone VARCHAR(50),
    address_line_1 VARCHAR(50),
    address_line_2 VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(15),
    country VARCHAR(50),
    employee_id INT,
    credit_limit DOUBLE PRECISION,
    check_id VARCHAR(50)
);

-- dim-employee
1.  sk_employee int(11)
2.  employee_id int(11)
3.  first_name varchar(50)
4.  last_name varchar(50)
5.  extension varchar(10)
6.  email varchar(100)
7.  office_id varchar(10)
8.  job_name varchar(50)
9.  report_to int(11)

CREATE TABLE dim_employee (
    sk_employee INT PRIMARY KEY,
    employee_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    extension VARCHAR(10),
    email VARCHAR(100),
    office_id VARCHAR(10),
    job_name VARCHAR(50),
    report_to INT
);

-- dim-product
1.  sk_product int(11)
2.  product_id varchar(15)
3.  prodcut_name varchar(70)
4.  product_line varchar(50)
5.  product_scale varchar(10)
6.  supplier varchar(50)

CREATE TABLE dim_product (
    sk_product INT PRIMARY KEY,
    product_id VARCHAR(15),
    product_name VARCHAR(70),
    product_line VARCHAR(50),
    product_scale VARCHAR(10),
    supplier VARCHAR(50)
);

-- dim-time
1.  sk_time int(11)
2.  `date` date
3.  date_text varchar(20)
4.  `year` int(11)
5.  year_text varchar(13)
6.  `quarter` int(11)
7.  quarter_name varchar(20)
8.  `month` int(11)
9.  month_name varchar(13)
10.  month_abbr varchar(13)
11.  day_of_month int(11)
12.  day_of_month_text varchar(13)
13.  week_of_year int(11)
14.  week_of_year_text varchar(13)
15.  day_of_week int(11)
16.  day_name varchar(13)
17.  day_abbr varchar(13)

CREATE TABLE dim_time (
    sk_time INT PRIMARY KEY,
    date DATE,
    date_text VARCHAR(20),
    year INT,
    year_text VARCHAR(13),
    quarter INT,
    quarter_name VARCHAR(20),
    month INT,
    month_name VARCHAR(13),
    month_abbr VARCHAR(13),
    day_of_month INT,
    day_of_month_text VARCHAR(13),
    week_of_year INT,
    week_of_year_text VARCHAR(13),
    day_of_week INT,
    day_name VARCHAR(13),
    day_abbr VARCHAR(13)
);

-- dim-office
1.  sk_office int(11)
2.  office_id varchar(10)
3.  phone varchar(50)
4.  address_line_1 varchar(50)
5.  address_line_2 varchar(50)
6.  state varchar(50)
8.  country varchar(50)
7.  zip_code varchar(15)
8.  territory varchar(10)
7.  city varchar(50)

CREATE TABLE dim_office (
    sk_office INT PRIMARY KEY,
    office_id VARCHAR(10),
    phone VARCHAR(50),
    address_line_1 VARCHAR(50),
    address_line_2 VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(15),
    country VARCHAR(50),
    territory VARCHAR(10)
);

-- adding foreign key
ALTER TABLE fact_table
ADD CONSTRAINT fk_client
FOREIGN KEY (sk_client) REFERENCES dim_client (sk_client);

ALTER TABLE fact_table
ADD CONSTRAINT fk_product
FOREIGN KEY (sk_product) REFERENCES dim_product (sk_product);

ALTER TABLE fact_table
ADD CONSTRAINT fk_employee
FOREIGN KEY (sk_employee) REFERENCES dim_employee (sk_employee);

ALTER TABLE fact_table
ADD CONSTRAINT fk_office
FOREIGN KEY (sk_office) REFERENCES dim_office (sk_office);

ALTER TABLE fact_table
ADD CONSTRAINT fk_order_time
FOREIGN KEY (sk_order_time) REFERENCES dim_time (sk_time);

-- add index
CREATE INDEX idx_fact_table_sk_client ON fact_table (sk_client);
CREATE INDEX idx_fact_table_sk_product ON fact_table (sk_product);
CREATE INDEX idx_fact_table_sk_employee ON fact_table (sk_employee);
CREATE INDEX idx_fact_table_sk_office ON fact_table (sk_office);
CREATE INDEX idx_fact_table_sk_order_time ON fact_table (sk_order_time);
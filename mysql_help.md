<ins>For MySQL Local</ins>
- mysql -u <username> -p #Login with MySQL credentials
- CREATE DATABASE IF NOT EXISTS <DATABASE>; #creates database without overwrite errors
- CREATE DATABASE <DATABASE>; #creates database 
- SHOW DATABASES; #Shows all created databases.
- USE <DATABASE>; #Use specific database
- SHOW TABLES; #Show tables in database
- DESCRIBE <DATABASE> #shows variables and variable types in the database
- SELECT * FROM <TABLE>; #Shows all data stored in the table
- SELECT * FROM <TABLE> LIMIT <N>; N = Number of Rows to display
- DELETE FROM <TABLE>; #Deletes all data, keeps table structure
- DROP TABLE <TABLE>; #removes entire table

<ins>Update Dockerfile and run this to rebuild to prevent dependency issues.</ins>
- docker build -t custom_airflow:latest . 

<ins>Update Image field in docker-compose.yaml for containers</ins>
- image: custom_airflow:latest

<ins>Creating a table for loading examples in MySQL for API calls.</ins> 
- keep in mind of the variable types returned
- CREATE TABLE historical_6hour (
    id INT AUTO_INCREMENT PRIMARY KEY,
    observation_time DATETIME,
    epoch_time BIGINT,
    weather_text VARCHAR(255),
    weather_icon INT,
    has_precipitation BOOLEAN,
    precipitation_type VARCHAR(255),
    is_daytime BOOLEAN,
    temperature_c FLOAT,
    temperature_f FLOAT,
    mobile_link TEXT,
    link TEXT
);

- CREATE TABLE etf_stock_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME,
    symbol VARCHAR(10),
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
);

- CREATE TABLE etf_current_price (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp DATETIME NOT NULL,
    current_price FLOAT NOT NULL
);




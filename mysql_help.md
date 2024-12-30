# MySQL and Docker Setup Guide

## <ins>MySQL Local Commands</ins>

### General MySQL Operations
- **Login**: `mysql -u <username> -p`  
  Use your MySQL credentials to log in.

- **Create a Database (safe)**:  
  `CREATE DATABASE IF NOT EXISTS <DATABASE>;`  
  Prevents overwriting an existing database.

- **Create a Database**:  
  `CREATE DATABASE <DATABASE>;`  

- **Show All Databases**:  
  `SHOW DATABASES;`  

- **Use a Specific Database**:  
  `USE <DATABASE>;`  

- **Show Tables in a Database**:  
  `SHOW TABLES;`  

- **Describe Table Structure**:  
  `DESCRIBE <TABLE>;`  

- **View Table Data**:  
  `SELECT * FROM <TABLE>;`  

- **View Limited Rows**:  
  `SELECT * FROM <TABLE> LIMIT <N>;`  
  `N` = Number of rows to display.

- **Delete All Data in a Table**:  
  `DELETE FROM <TABLE>;`  
  *Note*: This keeps the table structure intact.

- **Delete All Data and reset increment**:
  `TRUNCATE TABLE <TABLE>;`

- **Remove a Table Completely**:  
  `DROP TABLE <TABLE>;` 

- **Edit table column name**:
  `ALTER TABLE <TABLE> CHANGE <original_name> <new_name> VARTYPE;` 

---

## <ins>Updating Dockerfile</ins>

### Build a Custom Airflow Docker Image
1. Update your `Dockerfile` as needed.
2. Rebuild the image:  
   ```bash
   docker build -t custom_airflow:latest .


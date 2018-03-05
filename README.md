# csv-to-redshift-loader

#### What script does:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Inserts csv file into RedShift DB  
#### Usage:  
* a)  
Run in terminal:  
```
python csv_to_redshift_v4.1.py -f <file path> -t <table name> -s <schema name>  
```
\<file path\> - csv file path you need to insert into db.  
\<table name\> - table name that needs to be created to persist csv file data. if not passed, csv file name will be taken as table name.  
\<schema name\> - schema name where table need to be created. if not passed, default will be taken as schema name. default schema name needs to be set in 'DB credentials' section below.  

* b)  
After script is launched, it will start analyzing csv file to get column names and column data types.  
Once analysis is complete, brief columns summary can/will be printed,  
where original column names and thir types are shown in 'column names' and 'column types' respectively.  

**Ex:**  
File analysis complete ...  

column index  |column names             |column names legitimized  |column types  |column types modified  |column selected  |
--------------|-------------------------|--------------------------|--------------|-----------------------|-----------------|
1             |1DESCRIPTION_MOD1        |_1DESCRIPTION_MOD1        |Text          |Text                   |False            | 
2             |MERCHANT_NAME            |MERCHANT_NAME             |Text          |Text                   |False            | 
3             |CATEGORY_FULL_PATH_MOD1  |CATEGORY_FULL_PATH_MOD1   |Text          |Text                   |False            | 
4             |REVENUE                  |REVENUE                   |Text          |Text                   |False            | 
5             |ITEMS                    |ITEMS                     |Integer       |Integer                |False            | 
6             |0SAMPLE_ITEM_ID          |_0SAMPLE_ITEM_ID          |Integer       |Integer                |False            | 
7             |c!ol_7                   |c_ol_7                    |Text          |Text                   |False            | 


* c)  
User will be prompted to modify column types, where user needs to provide column index number  
in printed columns summary, semi-column and data type.  
Three possible data types are available:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;i - integer  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;b - big integer  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;t - text  
or if not modification is needed and all column types look good, user can press ENTER key to skip modification.  
Ex:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MODIFY COLUMN TYPES (optional):  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Enter column number:type or Press [Enter] to skip:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5:i  (this is a user input, which means column ITEMS needs to be converted into integer when inserted into db table)  
* d)  
User will be prompted to indicate columns that needs to be imported into db.  
And again columns indexes need to be indicated seperated by commas.  
or if all columns need to be inserted, user can just press ENTER key.  
Ex:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;INDICATE COLUMNS TO BE INCLUDED:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Enter column numbers numbers seperated by comma:  1,2,7    (this is a user input, which means insert only 1st, 2nd and 7th columns only)  
* e)  
After that, a couple reminders might appear in case if no table name and/or schema name were not passed at the beginning.  
And insertion process will start.  

#### Note  
Hardcode your own following credentials in # DB engine credentials  
host_     = ''  
database_ = ''  
port_     = ''  
username_ = ''  
password_ = '' 

'''
What script does:
    Inserts csv file into MySQL DB

Usage:

    a)
    Run in terminal:
        python csv_to_redshift_v4.1.py  -f <file path>  -t <table name>  -s <schema name>

        <file path>   - csv file path you need to insert into db.
        <table name>  - table name that needs to be created to persist csv file data.
                        if not passed, csv file name will be taken as table name.
        <schema name> - schema name where table need to be created.
                        if not passed, default will be taken as schema name.
                        default schema name needs to be set in 'DB credentials' section below.

    b)
    After script is launched, it will start analyzing csv file to get column names and column data types.
    Once analysis is complete, brief columns summary can/will be printed, 
    where original column names and thir types are shown in 'column names' and 'column types' respectively.

    Ex:
        File analysis complete ...
        --------------+-------------------------+--------------------------+--------------+-----------------------+-----------------+
        column index  |column names             |column names legitimized  |column types  |column types modified  |column selected  |
        --------------+-------------------------+--------------------------+--------------+-----------------------+-----------------+
        1             |1DESCRIPTION_MOD1        |_1DESCRIPTION_MOD1        |Text          |Text                   |False            |
        2             |MERCHANT_NAME            |MERCHANT_NAME             |Text          |Text                   |False            |
        3             |CATEGORY_FULL_PATH_MOD1  |CATEGORY_FULL_PATH_MOD1   |Text          |Text                   |False            |
        4             |REVENUE                  |REVENUE                   |Text          |Text                   |False            |
        5             |ITEMS                    |ITEMS                     |Integer       |Integer                |False            |
        6             |0SAMPLE_ITEM_ID          |_0SAMPLE_ITEM_ID          |Integer       |Integer                |False            |
        7             |c!ol_7                   |c_ol_7                    |Text          |Text                   |False            |

    c)
    User will be prompted to modify column types, where user needs to provide column index number
    in printed columns summary, semi-column and data type. 
    Three possible data types are available: 
        i - integer
        b - big integer
        t - text
    or if not modification is needed and all column types look good, user can press ENTER key to skip modification.

    Ex:
        MODIFY COLUMN TYPES (optional):
            Enter column number:type or Press [Enter] to skip:
            5:i  (this is a user input, which means column ITEMS needs to be converted into integer when inserted into db table)

    d)
    User will be prompted to indicate columns that needs to be imported into db.
    And again columns indexes need to be indicated seperated by commas.
    or if all columns need to be inserted, user can just press ENTER key.

    Ex:
        INDICATE COLUMNS TO BE INCLUDED:
            Enter column numbers numbers seperated by comma:  1,2,7    (this is a user input, which means insert only 1st, 2nd and 7th columns only)

    e)
    After that, a couple reminders might appear in case if no table name and/or schema name were not passed at the beginning.
    And insertion process will start.
    
'''


import pandas as pd
from IPython.display import display, HTML	
from collections import OrderedDict
from optparse import OptionParser
from pprint import pprint
from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector
from collections import defaultdict
import sys
import csv
import time
import codecs
from copy import deepcopy
import threading



# DB engine credentials (Robocop 2)
# ---------------------------------------------------------------------------------------------------------
host_     = ''
database_ = ''
port_     = ''
username_ = ''     #'infoprod_ops_admin' admin username
password_ = ''

# DB credentials (Robocop 2)
# ---------------------------------------------------------------------------------------------------------
SCHEMA_NAME = 'infoprod'
TABLE_NAME  = ''



help_text = """\n  -f, --csv_file  // csv file to be imported into Postrgres"""
parser = OptionParser(usage=help_text)
parser.add_option("--cf", "--config_file",   dest='config_file',   help="")
parser.add_option("-f",   "--csv_file",      dest='csv_file',      help="",   default='')
parser.add_option("-s",   "--schema_name",   dest='schema_name',   help="",   default='')
parser.add_option("-t",   "--table_name",    dest='table_name',    help="",   default='')
#(options, args) = parser.parse_args()

class Spinner:
    busy = False
    delay = 0.1

    @staticmethod
    def spinning_cursor():
        while 1: 
            for cursor in '|/-\\': 
            	yield ('%-20s' % (' '*2+'Analyzing file ...' + cursor))

    def __init__(self, delay=None):
        self.spinner_generator = self.spinning_cursor()
        if delay and float(delay): 
        	self.delay = delay

    def spinner_task(self):
        while self.busy:
            sys.stdout.write(next(self.spinner_generator))
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\r')    # Use '\r' to move cursor back to line beginning or use '\b' to erase the last character
            sys.stdout.flush()

    def start(self):
        self.busy = True
        threading.Thread(target=self.spinner_task).start()

    def stop(self):
        self.busy = False
        time.sleep(self.delay)
        print('%-20s' % (' '*2+'File analysis complete ...'))


class Columns_Processor:
    required_columns = ['column index',
                        'column names',
                        'column names legitimized',
                        'column types',
                        'column types modified',
                        'column selected']
    columns_df = pd.DataFrame(columns = required_columns)

    count = None
    is_column_types_modified = False


    # 0
    def __init__(self, column_names):
        self.set_column_names(column_names)
        self.create_column_indexes()
        #self.default_column_types_to_integer()
        self.create_legitimized_column_names()
        #self.columns_df['column selected'] = False
        self.set_columns_selected(select='none')
        self.count = len(self.columns_df)

    # 1
    # column index
    def create_column_indexes(self):

        self.columns_df['column index'] = self.columns_df.index + 1

    def get_column_indexes(self, select = 'all'):
        if(select == 'selected'):
            return self.columns_df.loc[self.columns_df['column selected']==True, 'column index'].tolist()
        else:
            return self.columns_df['column index'].tolist()

    # 2
    # column names
    def set_column_names(self, column_names):
        for col in column_names:
            self.columns_df = self.columns_df.append({'column names' : col}, ignore_index=True)

    def get_column_names(self, select = 'all'):
        if(select == 'selected'):
            return self.columns_df.loc[self.columns_df['column selected']==True, 'column names'].tolist()
        else:
            return self.columns_df['column names'].tolist()

    def get_column_name_by_index(self, column_index):
        #return self.columns_df.loc[self.columns_df['column index']==column_index, ['column names']]
        return self.columns_df.loc[self.columns_df['column index']==column_index]['column names'].tolist()[0]

    # 3
    # column names legitimized
    def create_legitimized_column_names(self):

        self.columns_df['column names legitimized'] = self.columns_df['column names'].apply(lambda x: self.legitimate_as_column_name(x))

    def get_legitimized_column_names(self, select = 'all'):
        if(select == 'selected'):
            return self.columns_df.loc[self.columns_df['column selected']==True, 'column names legitimized'].tolist()
        else:
            return self.columns_df['column names legitimized'].tolist()

    # 4
    # column types
    def default_column_types_to_integer(self):
        self.columns_df['column types'] = 'Integer'
        self.columns_df['column types modified'] = 'Integer'

    def set_column_type_by_name(self, column_name, column_type):
        self.columns_df.loc[self.columns_df['column names']==column_name, ['column types']] = column_type
        self.columns_df.loc[self.columns_df['column names']==column_name, ['column types modified']] = column_type
        self.is_column_types_modified = True

    def set_column_type_by_index(self, column_index, column_type):
        self.columns_df.loc[self.columns_df['column index']==column_index, ['column types']] = column_type
        self.columns_df.loc[self.columns_df['column index']==column_index, ['column types modified']] = column_type
        self.is_column_types_modified = True

    def get_column_types(self, select = 'all'):
        if(select == 'selected'):
            return self.columns_df.loc[self.columns_df['column selected']==True, 'column types'].tolist()
        else:
            return self.columns_df['column types'].tolist()

    def get_column_type(self, column_name):

        return self.columns_df.loc[self.columns_df['column names']==column_name, 'column types'].tolist()[0]

    # 5
    # column types modified
    def set_column_type_modified_by_name(self, column_name, column_type):

        self.columns_df.loc[self.columns_df['column names']==column_name, ['column types modified']] = column_type

    def set_column_type_modified_by_index(self, column_index, column_type):

        self.columns_df.loc[self.columns_df['column index']==column_index, ['column types modified']] = column_type

    def get_column_types_modified(self, select = 'all'):
        if(select == 'selected'):
            return self.columns_df.loc[self.columns_df['column selected']==True, 'column types modified'].tolist()
        else:
            return self.columns_df['column types modified'].tolist()

    # 6
    # column selected
    def set_columns_selected(self, select='All'):
        if(select.lower() == 'all'):
            self.columns_df['column selected'] = True
        elif(select.lower() == 'none'):
            self.columns_df['column selected'] = False

    def set_column_selected_by_name(self, column_name):
        #self.columns_df['column selected'] = False

        #for col_idx in columns_included_idx:
        self.columns_df.loc[self.columns_df['column name']==column_name, ['column selected']] = True

    def set_column_selected_by_index(self, column_index):
        #self.columns_df['column selected'] = False

        #for col_idx in columns_included_idx:
        self.columns_df.loc[self.columns_df['column index']==column_index, ['column selected']] = True

    # deprecated as can be replaced by get_column_names() function
    #def get_columns_selected(self):
    #    return self.columns_df.loc[self.columns_df['column selected']==True, 'column names'].tolist()

    # 7.1
    # get row record as dict
    def get_info_by_index(self, col_index):

        return self.columns_df.loc[self.columns_df['column index']==col_index].to_dict('records')[0]

    def get_info_by_name(self, col_name):

        return self.columns_df.loc[self.columns_df['column names']==col_name].to_dict('records')[0]

    # 7.2
    # iterate row records as dict
    def iterate_all(self):
        for column_name in self.get_column_names():
            yield self.columns_df.loc[self.columns_df['column names']==column_name].to_dict('records')[0]

    def iterate_selected(self):
        for column_name in self.get_column_names(select='selected'):
            yield self.columns_df.loc[self.columns_df['column names']==column_name].to_dict('records')[0]


    # helper functions
    def legitimate_as_column_name(self, column_name_str_):
        column_name_alnum = self.replace_chars_by(column_name_str_, replace_char_by_='_', keep_='alphanum')

        if(column_name_alnum == ''):
            return column_name_start
        elif(column_name_alnum[0] in '0123456789'):
            return '_' + column_name_alnum
        else:
            return column_name_alnum

    def replace_chars_by(self, str_, replace_char_by_=None, keep_=None):
        if(keep_ == 'alpha'):
            keepable_ = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
            str_alphabetic = "".join((char if char in keepable_ else replace_char_by_) for char in str_)
            return str_alphabetic

        elif(keep_ == 'alphanum'):
            keepable_ = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
            str_alphanumeric = "".join((char if char in keepable_ else replace_char_by_) for char in str_)
            return str_alphanumeric

        elif(keep_ == 'digits'):
            keepable_ = '0123456789'
            str_wo_numbers = "".join((char if char in keepable_ else replace_char_by_) for char in str_)
            return str_wo_numbers

    def print_summary(self):

        self.print_df_tabular(self.columns_df)

    def print_df_tabular(self, df, index=False):
        import os.path
        import sys
        import pandas as pd
        if(not isinstance(df, pd.DataFrame)):
            print('ERROR:')
            print('Object for display is not DataFrame\n')
            sys.exit(0)

        cols_ls = df.columns.values.tolist()
        cols_maxWidth_dict = {}
        enlarge = 2

        # calculate width for each column
        for i, col in enumerate(cols_ls):
            max_l = df[col].map(lambda x: len(str(x))).max()  # get length of longest string value
            max_l = max(len(col),max_l)                       # get length of column name if it is longer
            cols_maxWidth_dict[i] = max_l
            #print(max_l)

        # calculate index column width
        if(index):
            df_row_count = df.shape[0]
            index_width = len(str(df_row_count))
            index_col_name = 'ix'

        # ------------------------------------------------------------------------------------
        # print top line of header
        sep_ = ''
        if(index):
            sep_ = '-'*(index_width+enlarge)
            sep_ = sep_ + '+'
        for i, col in enumerate(cols_ls):   
            sep_ = sep_ + '-'*(cols_maxWidth_dict[i]+enlarge)
            sep_ = sep_ + '+'
        print(sep_)
        #file_out.write(sep_+'\n')

        # ------------------------------------------------------------------------------------
        # print header
        sep_ = ''
        if(index):
            sep_ = sep_ + index_col_name + ' '*(index_width-len(index_col_name)+enlarge)
            sep_ = sep_ + '|'
        for i, col in enumerate(cols_ls):   
            sep_ = sep_ + col + ' '*(cols_maxWidth_dict[i]-len(col)+enlarge)
            sep_ = sep_ + '|'
        print(sep_)
        #file_out.write(sep_+'\n')

        # ------------------------------------------------------------------------------------
        # print bottom line of header
        sep_ = ''
        if(index):
            sep_ = '-'*(index_width+enlarge)
            sep_ = sep_ + '+'
        for i, col in enumerate(cols_ls):   
            sep_ = sep_ + '-'*(cols_maxWidth_dict[i]+enlarge)
            sep_ = sep_ + '+'
        print(sep_)
        #file_out.write(sep_+'\n')

        # ------------------------------------------------------------------------------------
        # print row values
        for ix, row in df[cols_ls].iterrows():
            sep_ = ''
            if(index):
                ix_str = str(int(ix))
                sep_ = sep_ + ix_str + ' '*(index_width-len(ix_str)+enlarge)
                sep_ = sep_ + '|'
            for i,v in enumerate(list(row)):
                #print(i,v)
                sep_ = sep_ + str(v) + ' '*(cols_maxWidth_dict[i]-len(str(v))+enlarge)
                sep_ = sep_ + '|'
            print(sep_)
            #file_out.write(sep_+'\n')
        print()



class CSV_FILE:
    csv_file_path = None
    csv_file_name = None
    infile        = None
    csv_reader    = None

    Columns = None


    # db engine credentials
    url         = None
    engine      = None
    conn        = None

    schema_name = None  # string
    table_name  = None  # string
    table_name_legitimate = None  # string
    schema_table_name     = None  # schema_name + table_name

    # db
    column_types_mysql = None  # odict


    def __init__(self, csv_file_path):
        self.check_file_path(csv_file_path)

        self.start_csv_reader()
        #self.column_names = self.csv_reader.fieldnames
        self.Columns = Columns_Processor(self.csv_reader.fieldnames)
        #self.Columns.print_summary()
        #self.column_count = len(self.column_names)
        
        self.close_csv_reader()
        
    # think on function name
    def check_file_path(self, csv_file_path):
        if(csv_file_path == ''):
            print('ERROR 1:')
            print(' No arguments passed')
            print(' Need to pass csv file name to be imported into RedShift')
            sys.exit(0)
        else:
            self.csv_file_path = csv_file_path
            self.csv_file_name = csv_file_path.split('/')[-1:][0]
            print('csv file path:\n ', self.csv_file_path)
            print('csv file:\n  ', self.csv_file_name)

    def start_csv_reader(self):
        #infile = open(csv_file_path, encoding='utf-8-sig')
        self.infile = codecs.open(csv_file_path, "r", encoding='utf-8-sig', errors='ignore')
        #self.csv_reader = csv.reader(self.infile)
        self.csv_reader = csv.DictReader(self.infile)

    def close_csv_reader(self):
        self.infile.close()
        self.infile = None
        self.csv_reader = None

    is_file_analyzed = False
    def analyze_file(self):
        spinner = Spinner()
        spinner.start()

        # default all column types to integer
        self.Columns.default_column_types_to_integer()

        self.start_csv_reader()
        column_names_copy_1 = self.Columns.get_column_names()
        for row_idx, row in enumerate(self.csv_reader):
            column_names_copy_2 = deepcopy(column_names_copy_1)
            #print(row)
            if(len(column_names_copy_1) == 0):
                break
            for i, col_ in enumerate(column_names_copy_2):
                cell_value = row[col_]
                if not cell_value.isdigit() and cell_value != '':
                    #print(col_, ' is Text', cell_value,'')
                    self.Columns.set_column_type_by_name(col_, 'Text')
                    column_names_copy_1 = [x for x in column_names_copy_1 if x!= col_]

                elif self.Columns.get_column_type(col_) != 'Text' and int(cell_value) > 2147483647:
                    self.Columns.set_column_type_by_name(col_, 'BigInteger')
        
        self.rows_count = row_idx + 1

        column_names_copy_1 = None
        column_names_copy_2 = None
        self.close_csv_reader()
        self.is_file_analyzed = True
        spinner.stop()

    def get_rows_count(self):
        return self.rows_count

    def replace_chars_by(self, str_, replace_char_by_=None, keep_=None):
        if(keep_ == 'alpha'):
            keepable_ = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
            str_alphabetic = "".join((char if char in keepable_ else replace_char_by_) for char in str_)
            return str_alphabetic

        elif(keep_ == 'alphanum'):
            keepable_ = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
            str_alphanumeric = "".join((char if char in keepable_ else replace_char_by_) for char in str_)
            return str_alphanumeric

        elif(keep_ == 'digits'):
            keepable_ = '0123456789'
            str_wo_numbers = "".join((char if char in keepable_ else replace_char_by_) for char in str_)
            return str_wo_numbers

    def prompt_column_types_modification(self):
        column_type_str=''

        while True:
            column_type_str = input(' '*2+'Enter column number:type or Press [Enter] to skip:  ')
            column_type_str = column_type_str.replace(' ','')
            if(column_type_str.lower() in ['','d','done']):
                break
            else:
                column_index, column_type = column_type_str.split(':')
                column_index = int(column_index)

                if(column_type.lower() in ('i','int','integer') and self.Columns.get_info_by_index(column_index)['column types modified'] != 'Integer'):
                    self.Columns.set_column_type_modified_by_index(column_index, 'Integer')
                elif(column_type.lower() in ('i','int','integer')):
                    print(' '*4, self.Columns.get_column_name_by_index(column_index), 'column is already of Integer type')

                if(column_type.lower() in ('b','big','bigint','biginteger') and self.Columns.get_info_by_index(column_index)['column types modified'] != 'BigInteger'):
                    self.Columns.set_column_type_modified_by_index(column_index, 'BigInteger')
                elif(column_type.lower() in ('b','big','bigint','biginteger')):
                    print(' '*4, self.Columns.get_column_name_by_index(column_index),' column is already of BigInteger type')

                if(column_type.lower() in ('t','text') and self.Columns.get_info_by_index(column_index)['column types modified'] != 'Text'):
                    self.Columns.set_column_type_modified_by_index(column_index, 'Text')
                elif(column_type.lower() in ('t','text')):
                    print(' '*4, self.Columns.get_column_name_by_index(column_index),' column is already of Text type')

    def prompt_column_selection(self):
        columns_included = input(' '*2+'Enter column numbers numbers seperated by comma:  ')
        

        if(columns_included == ''):
            #self.column_names_selected = self.column_names
            self.Columns.set_columns_selected(select='all')
        else:
            columns_included     = columns_included.replace(' ','')
            columns_included_idx = columns_included.strip(',').split(',')
            columns_included_idx = [int(x) for x in columns_included_idx]

            #self.column_names_selected = [self.column_names[ix-1] for ix in columns_included_idx]
            for ix in columns_included_idx:
                self.Columns.set_column_selected_by_index(ix)

        #self.column_types_selected = {col: self.column_types[col] for col in self.column_names_selected if col in self.column_types}

    def prompt_schema_name(self, schema_name):
        self.schema_name = schema_name

        if(schema_name == ''):
            print('REMINDER 1:')
            print(' No arguments passed for [schema_name]')
            print(' Default schema name will be used %s \n' % SCHEMA_NAME)
            self.schema_name = SCHEMA_NAME

    def prompt_table_name(self, table_name):
        self.table_name = table_name

        if(table_name == ''):
            print('REMINDER 2:')
            print(' No arguments passed for [table_name]')
            print(' CSV FILE NAME will be used as table name : %s \n' % self.csv_file_name)
            self.table_name = self.csv_file_name.replace('.csv','')
            self.table_name = self.legitimate_as_table_name(self.table_name)

    def start_connection(self):
        self.url    = ''.join(['postgresql://', username_, ":", password_, "@", host_, ':',port_, '/', database_])
        self.engine = create_engine(self.url)
        self.conn   = self.engine.connect()

    def end_connection(self):
        if self.conn:
            self.conn.close()

    def get_mysql_types(self, column_types):
        column_types_mysql = {}
        for col, type in column_types.items():
            if(type == 'Text'):
                column_types_mysql[col] = 'varchar(4000)'
            elif(type == 'Integer'):
                column_types_mysql[col] = 'INT'
            elif(type == 'BigInteger'):
                column_types_mysql[col] = 'INT8'

        return column_types_mysql

    def create_table_in_mysql(self, column_types):
        self.start_connection()

        col_names_odict_rd = self.get_mysql_types(column_types)

        inspector = Inspector.from_engine(self.engine)
        if(self.schema_table_name.lower() in inspector.get_table_names()):
            print('SQL Error:')
            print('Table '+self.schema_table_name+' already exists\n')
            sys.exit(0)
        else:
            qry_create_table = self.qry_create_table(self.schema_table_name, col_names_odict_rd)
            self.conn.execute(text(qry_create_table))
            self.end_connection()

    def qry_create_table(self, schema_table_name, header_names_odict):
        qry_create_table = "CREATE TABLE IF NOT EXISTS "+schema_table_name+" (\nid integer PRIMARY KEY,"
        qry_create_table = "CREATE TABLE "+schema_table_name+" (\nid integer PRIMARY KEY,"
        qry_create_table = "CREATE TABLE "+schema_table_name+" ("
                                    
        for i, kv in enumerate(header_names_odict.items()):
            #print(kv)
            comma = ','
            if((i+1) == len(header_names_odict)):
                comma=''
            column_name  = kv[0]
            if(column_name in ['order']):
                column_name = '"'+column_name+'"'
            column_type  = kv[1]
            qry_create_table = qry_create_table +'\n'+ column_name + ' ' + column_type + comma

        qry_create_table = qry_create_table+"\n); "
        return qry_create_table

    def legitimate_as_table_name(self, table_name_str_):
        table_name_alphanumeric_str_ = self.replace_chars_by(table_name_str_, replace_char_by_='_', keep_='alphanum')
        
        if(table_name_alphanumeric_str_ == ''):
            return 'table_name'
        elif(table_name_alphanumeric_str_[0] in '0123456789'):         # if file name starts with digit, append _ in front
            table_name_alphanumeric_str_ = '_'+table_name_alphanumeric_str_
            return table_name_alphanumeric_str_
        else:
            return table_name_alphanumeric_str_

    def get_column_dtypes(self, column_types):
        column_dtypes = {}
        for col, type in column_types.items():
            if(column_types[col] == 'Integer'):
                column_dtypes[col] = str #np.int
            elif(column_types[col]=='Text'):
                column_dtypes[col] = str

        return column_dtypes

    def insert_csv_file_into_mysql_2(self):
        print('Insertion process ...')
        start = time.time()
        from sqlalchemy import text


        column_types_selected_legitimized = {k:v  for k,v in zip(self.Columns.get_legitimized_column_names(select='selected'), self.Columns.get_column_types_modified(select='selected'))}
        column_types_modified             = {k:v  for k,v in zip(self.Columns.get_column_names(), self.Columns.get_column_types_modified())}


        self.schema_table_name = '.'.join([self.schema_name, self.table_name])
        self.start_connection()
        self.create_table_in_mysql(column_types_selected_legitimized)


        csv_read_chunk_size = 10000
        insertion_chunk_size = 500
        row_counter = 0
        
        column_types = {k:v for k,v in zip(self.Columns.get_column_names(select='selected'), self.Columns.get_column_types_modified(select='selected'))}
        column_dtypes = self.get_column_dtypes(column_types)  # sets all column dtypes to str so that pandas could read all csv file columns as text
        df_iter = pd.read_csv(self.csv_file_path, iterator=True, chunksize=csv_read_chunk_size, header=0, dtype=column_dtypes, encoding = 'ISO-8859-1')
        for df in df_iter:
            df.reset_index(drop=True, inplace=True)

            # 1 convert columns to int
            for col, type in column_types_modified.items():
                if(type in ['Integer', 'BigInteger']):
                    df[col] = df[col].apply(lambda x: self.str_num_int_to_int_2(x))
                
                df[col] = df[col].apply(lambda x: self.process_value(x))

                df[col] = df[col].apply(lambda x: self.value_to_sql_value(x, type))

            #print(df)

            # 2 cut and legitimize columns
            df = df.loc[:, self.Columns.get_column_names(select='selected')]
            df.columns = self.Columns.get_legitimized_column_names(select='selected')
            


            conn = self.engine.connect()
            for df_chunk in self.df_chunker(df, 500):
                qry = self.df_to_query(df_chunk, self.schema_table_name)
                conn.execute(text(qry))
            conn.close()

    def df_chunker(self, df, chunk_size):

        return (df[pos : pos+chunk_size] for pos in range(0, len(df), chunk_size))

    def process_value(self, value):
        value_ = str(value)
        value_ = value_.replace("'","''")   # replace special characters
        value_ = value_.replace(":","\:")   # replace special characters

        return value_

    def value_to_sql_value(self, v, t):
        if(v!=v or (v in ['nan','NaN',''])):
            return 'NULL'
        elif(t.lower() in ['t','txt','text']):
            return "'"+v+"'"
        else:
            return v

    def df_to_query(self, df, schema_table_name):
        columns_list_  = df.columns.values.tolist()
        columns_list_s = ['"'+col+'"' if(col in ['order']) else col for col in columns_list_]

        qry = "insert into " + schema_table_name + " (" + ','.join(columns_list_s) + ") values "

        counter = 0
        for idx, row in df.iterrows():
            comma = ","
            if(counter == 0):
                comma = ""

            values_line_str = ','.join(row)
            values_line_str = comma + "(" + values_line_str + ")"
            qry = qry + values_line_str

            counter = counter + 1


        return qry

    def str_num_int_to_int_2(self, sentence_):
        if (sentence_!=sentence_):
            return sentence_

        printable_ = '0123456789'
        str_num_processed = "".join((char if char in printable_ else "") for char in str(sentence_).lower())
        #str_num_processed = str_num_processed.lstrip('0')
        if(len(str_num_processed) != 0):
            str_num_processed = str(int(str_num_processed))
            return str_num_processed
        else:
            return str_num_processed





# ===================================================================================================================
if __name__ == "__main__":
    (options, args) = parser.parse_args()
    csv_file_path = options.csv_file
    #csv_file = None

    # 0
    # Create Csv File object
    Csv_File = CSV_FILE(csv_file_path)

    # 1
    # Analyze file
    # ---------------------------------------------------------------------------------------------------------
    print()
    Csv_File.analyze_file()

    print('\nRows count: ', Csv_File.get_rows_count())
    Csv_File.Columns.print_summary()


    # 2
    # promt user to modify column types
    # ---------------------------------------------------------------------------------------------------------
    print('='*100)
    print('MODIFY COLUMN TYPES (optional):\n')

    Csv_File.prompt_column_types_modification()
    #Csv_File.Columns.print_summary()
    print()
    

    # 3
    # prompt user to enter selected column numbers
    # ---------------------------------------------------------------------------------------------------------
    print('='*100)
    print('INDICATE COLUMNS TO BE INCLUDED:\n')

    Csv_File.prompt_column_selection()
    Csv_File.Columns.print_summary()


    # 4.1
    # get schema name
    # --------------------------------------------------------------------------------------------------------- 
    schema_name = options.schema_name
    Csv_File.prompt_schema_name(schema_name)


    # 4.2
    # get table name
    # --------------------------------------------------------------------------------------------------------- 
    table_name = options.table_name
    Csv_File.prompt_table_name(table_name)

    print([Csv_File.schema_name, Csv_File.table_name])


    # 5
    # 
    # ---------------------------------------------------------------------------------------------------------
    # insert data into table
    Csv_File.insert_csv_file_into_mysql_2()



















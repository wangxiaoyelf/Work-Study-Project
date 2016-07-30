DEBUG                = False
BENCHMARK            = True
VALIDATE             = False
from dataformat import *

# record the time elapsed to run this program
if BENCHMARK:
    import time
# define the print out colors
class g_print_colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# define data model class
class DataModel:
    def __init__ (self):
        self.id_dic = {}
        if DEBUG:
            print("DataModel Initialized\n")
        if BENCHMARK:
            self.start_time = time.time()

    def getDateKeys(self, key_id, x, x_value_keys):
        date_keys = []
        # find match for id
        if key_id in self.id_dic:
            record = self.id_dic[key_id]
            for x_value_key in x_value_keys:
                # find match for x value
                if x_value_key in record:
                    x_value = record[x_value_key]
                    x_list = x_value.split("^")
                    if x in x_list and DATE_KEY in g_column_formats[x_value_key]:
                        date_keys.append(g_column_formats[x_value_key][DATE_KEY])
        return date_keys

    def hasID(self, key_id):
        result = False
        # check if key_id has already existed in id_dic
        if key_id in self.id_dic:
            result = True
        return result

    def __validateData(self, key_id, record):
        result = True
        if VALIDATE == True:
            global g_id_size_limit
            # validate if key_id is a str and within size
            if result == True:
                if isinstance(key_id, str) == False:
                    result = False
                    print(g_print_colors.WARNING + "key_id \"%s\" is not a str\n" % key_id + g_print_colors.ENDC)

            # validate if record is a dictionary
            if result == True:
                if isinstance(record, dict) == False:
                    result = False
                    print(g_print_colors.WARNING + "record is not a dictionary\n" + g_print_colors.ENDC)

            # validate if every key in record is in g_column_formats
            if result == True:
                # for each key in record
                for record_key in record:
                    if record_key not in g_sorted_col_keys:
                        # the key not found, fail
                        result = False
                        print(g_print_colors.WARNING + "record key \"%s\" is not in g_sorted_col_keys\n" % record_key + g_print_colors.ENDC)
                        break
            # validate if record key is a correct type
            if result == True:
                for record_key in record:
                    if self.__getTypeForKey(record_key) != type(record[record_key]):
                        result = False
                        print(g_print_colors.WARNING + "record key \"%s\" has a wrong type \"%s\"\n" % (record_key, str(type(record[record_key]))) + g_print_colors.ENDC)
                        break

        # calculate record value size limit
        if result == True:
            if len(key_id) > g_id_size_limit:
                g_id_size_limit = len(key_id)
            for record_key in record:
                record_value = record[record_key]
                value_limit = self.__getValueLimit(record_key)
                if len(record_value) > value_limit:
                    self.__setValueLimit(record_key,len(record_value))

        return result

    def __isValueUnique(self, record_key):
        result = False
        # check if the value in the columns can be appended twice
        if record_key in g_column_formats:
            result = g_column_formats[record_key][UNIQUE]
        return result

    def __getValueLimit(self, record_key):
        result = 0
        if record_key in g_column_formats:
            result = g_column_formats[record_key][LIMIT]
        return result

    def __setValueLimit(self, record_key, new_limit):
        if record_key in g_column_formats:
            g_column_formats[record_key][LIMIT] = new_limit

    def __getTypeForKey(self, record_key):
        result = str
        if record_key in g_column_formats:
            result = g_column_formats[record_key][TYPE]
        return result

    def setData(self, key_id, record):
        result = False
        if self.__validateData(key_id, record) == True:
            if key_id in self.id_dic:
                # update data if record is valid and key_id already exsits in id_dic
                dic_record = self.id_dic[key_id]
                for record_key in record:
                    record_value = record[record_key]
                    if record_key in dic_record:
                        if self.__isValueUnique(record_key) == False:
                            if record_value not in dic_record[record_key].split(VALUE_SEPARATOR):
                                # update record values
                                dic_record[record_key] += VALUE_SEPARATOR + record_value
                                if len(dic_record[record_key]) > self.__getValueLimit(record_key):
                                    self.__setValueLimit(record_key, len(dic_record[record_key]))
                    else:
                        # add new record_key for existing record
                        dic_record[record_key] = record_value
                        self.id_dic[key_id] = dic_record
                if DEBUG:
                    print("[%sUpdate%s %s]:\n%s\n" % (g_print_colors.OKGREEN, g_print_colors.ENDC, key_id, str(dic_record.keys())))
            else:
                # add data only if record is valid and key_id does not exsit in id_dic before
                self.id_dic[key_id] = record
                if DEBUG:
                    print("[%sAdd%s %s]:\n%s\n" % (g_print_colors.OKGREEN, g_print_colors.ENDC, key_id, str(record.keys())))
        result = True
        return result

    def outputDataModelToFile(self, file_name, file_encoding):
        with open(file_name, "w", encoding = file_encoding) as file_h:
            if DEBUG:
                print("File %s Opened\n" % file_name)
            # write each column name in the output file
            file_h.write("id")
            for record_key in g_sorted_col_keys:
                file_h.write("\t" + record_key)
            file_h.write("\n")
            # write id values in the output file
            for key_id in self.id_dic:
                record = self.id_dic[key_id]
                file_h.write(key_id)
                # write other column values in the output file
                for record_key in g_sorted_col_keys:
                    file_h.write("\t")
                    if record_key in record:
                        file_h.write(record[record_key])
                file_h.write("\n")
            print("Total %d records written to file\n" % len(self.id_dic))

    def __del__(self):
        # get the size limit of each column
        print("id max size: %d" % g_id_size_limit)
        for record_key in g_sorted_col_keys:
            print ("%s max size: %d" % (record_key, g_column_formats[record_key][LIMIT]))
        if BENCHMARK:
            print("Total time elapsed = ", time.time() - self.start_time)

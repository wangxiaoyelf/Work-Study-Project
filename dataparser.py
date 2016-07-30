import os
import re
from datamodel import *
# Behavior
VALIDATE             = False

# File
OUTPUT_FILE_NAME = "date_finder_result.txt"
INPUT_FILE_ENCODING = "cp437"
OUTPUT_FILE_ENCODING = "utf8"
DATE_FILE_PATH       = r'D:\Projects\CopyRightData\pagehitsdata'
DATE_FILE_NAME       = r'UKpageresults_'
DATE_START           = 1800
DATE_END             = 2016

# Option
OPTION_X_KEY = "x_key"
OPTION_X_KEYS = "x_keys"
OPTION_DATE = "date_value"
OPTION_VALUE_KEY = "value_key"

# Format
CR_NUM_COL = 6
JOINED_NUM_COL = 34

# Requrement
MAX_SEQUENCE_NUM = 15

# Index
ID_INDEX = 0
TRIPLE_INDEX = 1
JOINCASEB_VALUE_INDEX = 1
JOINCASEB_COL8_INDEX = 7
JOINCASEB_COL9_INDEX = 8
JOINCASEB_COL10_INDEX = 9
JOINCASEB_COL11_INDEX = 10
JOINCASEB_COL12_INDEX = 11
JOINCASEB_COL13_INDEX = 12
JOINCASEB_COL14_INDEX = 13
JOINCASEB_COL15_INDEX = 14
JOINCASEB_COL16_INDEX = 15
JOINCASEB_COL17_INDEX = 16
JOINCASEB_COL18_INDEX = 17
JOINCASEB_COL19_INDEX = 18
JOINCASEB_COL20_INDEX = 19
JOINCASEB_COL21_INDEX = 20
JOINCASEB_COL22_INDEX = 21
JOINCASEB_COL23_INDEX = 22
JOINCASEB_COL24_INDEX = 23
JOINCASEB_COL25_INDEX = 24
JOINCASEB_COL26_INDEX = 25
JOINCASEB_COL27_INDEX = 26
JOINCASEB_COL28_INDEX = 27
JOINCASEB_COL29_INDEX = 28
JOINCASEB_COL30_INDEX = 29
JOINCASEB_COL31_INDEX = 30
JOINCASEB_COL32_INDEX = 31
JOINCASEB_COL33_INDEX = 32
JOINCASEB_COL34_INDEX = 33

def parseFile(file_name, data_handler, data_model, options):
    result = True
    with open(file_name, "r", encoding = INPUT_FILE_ENCODING) as file_h:
        start_time = time.time()
        print("processing %s..." %file_name)
        # split the file into lists of values
        for line in file_h:
            data_list = line.strip("\n").split("\t")
            result = data_handler(data_list, data_model, options)
            if result == False:
                break
    return result

def crDataHandler(data_list, data_model, options):
    result = False
    if crOrDateValidate(data_list):
        result = True
        triple_list = data_list[TRIPLE_INDEX].lstrip("^").split("^")
        for triple in triple_list:
            x = triple.lstrip("(").rstrip(")").split(",")[0]
            # find the required x values
            if x.isdigit() and int(x) <= MAX_SEQUENCE_NUM:
                record = {options[OPTION_X_KEY]:x}
                data_model.setData(data_list[ID_INDEX], record)
    return result

def crOrDateValidate(data_list):
    if not VALIDATE:
        return True
    result = True
    if result == True:
        # validate if the data list generated from copyright and date files contains the right number of items
        if len(data_list) != CR_NUM_COL:
            result = False
            print(g_print_colors.WARNING + "data list length is not equal to CR_NUM_COL(%d)" % CR_NUM_COL + g_print_colors.ENDC)
    if result == True:
        # validate if the id value is correct
        if  len(data_list) <= ID_INDEX or len(data_list[ID_INDEX]) == 0:
            result = False
            print(g_print_colors.WARNING + "id column is not valid" + g_print_colors.ENDC)
    if result == True:
        # validate if the x value column has the right pattern
        if  len(data_list) <= TRIPLE_INDEX or not re.match("(\^\(\S+?,\S+?,\S+?\))+", data_list[TRIPLE_INDEX]):
            print(g_print_colors.WARNING + "x value column is not valid %s" % str(data_list) + g_print_colors.ENDC)
    return result

def joinedValidate(data_list):
    if not VALIDATE:
        return True
    result = True
    if result == True:
        # validate if the data list generated from joinedcaseb files contains the right number of items
        if len(data_list) != JOINED_NUM_COL:
            result = False
            print(g_print_colors.WARNING + "data list length is not equal to CR_NUM_COL(%d)" % CR_NUM_COL + g_print_colors.ENDC)
    return result

def dateDataHandler(data_list, data_model, options):
    result = False
    if crOrDateValidate(data_list):
        result = True
        key_id = data_list[ID_INDEX]
        triple_value = data_list[TRIPLE_INDEX]
        x_value_keys = options[OPTION_X_KEYS]
        triple_list = triple_value.lstrip("^").split("^")
        for triple in triple_list:
            x = triple.lstrip("(").rstrip(")").split(",")[0]
            # find match and record date keys
            if x.isdigit() and int(x) <= MAX_SEQUENCE_NUM:
                date_keys = data_model.getDateKeys(key_id, x, x_value_keys)
                record = {}
                # add date values into id_dic
                for date_key in date_keys:
                    record[date_key] = options[OPTION_DATE]
                data_model.setData(data_list[ID_INDEX], record)
    return result

def joinedDataHandler(data_list, data_model, options):
    result = False
    joined_id = data_list[ID_INDEX]
    if joinedValidate(data_list):
        result = True
        if data_model.hasID(joined_id):
            record = {  options[OPTION_VALUE_KEY]:data_list[JOINCASEB_VALUE_INDEX],
                        RESERVED12_KEY:data_list[JOINCASEB_COL8_INDEX],
                        RESERVED13_KEY:data_list[JOINCASEB_COL9_INDEX],
                        RESERVED14_KEY:data_list[JOINCASEB_COL10_INDEX],
                        RESERVED15_KEY:data_list[JOINCASEB_COL11_INDEX],
                        RESERVED16_KEY:data_list[JOINCASEB_COL12_INDEX],
                        RESERVED17_KEY:data_list[JOINCASEB_COL13_INDEX],
                        RESERVED18_KEY:data_list[JOINCASEB_COL14_INDEX],
                        RESERVED19_KEY:data_list[JOINCASEB_COL15_INDEX],
                        RESERVED20_KEY:data_list[JOINCASEB_COL16_INDEX],
                        RESERVED21_KEY:data_list[JOINCASEB_COL17_INDEX],
                        RESERVED22_KEY:data_list[JOINCASEB_COL18_INDEX],
                        RESERVED23_KEY:data_list[JOINCASEB_COL19_INDEX],
                        RESERVED24_KEY:data_list[JOINCASEB_COL20_INDEX],
                        RESERVED25_KEY:data_list[JOINCASEB_COL21_INDEX],
                        RESERVED26_KEY:data_list[JOINCASEB_COL22_INDEX],
                        RESERVED27_KEY:data_list[JOINCASEB_COL23_INDEX],
                        RESERVED28_KEY:data_list[JOINCASEB_COL24_INDEX],
                        RESERVED29_KEY:data_list[JOINCASEB_COL25_INDEX],
                        RESERVED30_KEY:data_list[JOINCASEB_COL26_INDEX],
                        RESERVED31_KEY:data_list[JOINCASEB_COL27_INDEX],
                        RESERVED32_KEY:data_list[JOINCASEB_COL28_INDEX],
                        RESERVED33_KEY:data_list[JOINCASEB_COL29_INDEX],
                        RESERVED34_KEY:data_list[JOINCASEB_COL30_INDEX],
                        RESERVED35_KEY:data_list[JOINCASEB_COL31_INDEX],
                        RESERVED36_KEY:data_list[JOINCASEB_COL32_INDEX],
                        RESERVED37_KEY:data_list[JOINCASEB_COL33_INDEX],
                        RESERVED38_KEY:data_list[JOINCASEB_COL34_INDEX]}
            data_model.setData(data_list[ID_INDEX], record)
    return result


if __name__ == "__main__":
    data_model = DataModel()
    result = True
    options = {}
    # CopyRight Files
    if result == True:
        options = {OPTION_X_KEY:X_0_KEY}
        result = parseFile("catresults_copyrightedSymbol.txt", crDataHandler, data_model, options)
    if result == True:
        options = {OPTION_X_KEY:X_1_KEY}
        result = parseFile("catresults_copyrighted.txt", crDataHandler, data_model, options)
    if result == True:
        options = {OPTION_X_KEY:X_2_KEY}
        result = parseFile("catresults_copyright.txt", crDataHandler, data_model, options)
    if result == True:
        options = {OPTION_X_KEY:X_3_KEY}
        result = parseFile("catresults_copr.txt", crDataHandler, data_model, options)
    # Date Files
    if result == True:
        for year in range(DATE_START, DATE_END):
            options =   {OPTION_X_KEYS:
                            [X_0_KEY, X_1_KEY, X_2_KEY, X_3_KEY],
                        OPTION_DATE:str(year)}
            result = parseFile(os.path.join(DATE_FILE_PATH,DATE_FILE_NAME)+str(year)+'.txt', dateDataHandler, data_model, options)
            if result == False:
                break
    # Joined Case B Files
    if result == True:
        options = {OPTION_VALUE_KEY:VALUE_0_KEY}
        result = parseFile("joinedcaseb_copyrightedSymbol.txt", joinedDataHandler, data_model, options)
    if result == True:
        options = {OPTION_VALUE_KEY:VALUE_1_KEY}
        result = parseFile("joinedcaseb_copyrighted.txt", joinedDataHandler, data_model, options)
    if result == True:
        options = {OPTION_VALUE_KEY:VALUE_2_KEY}
        result = parseFile("joinedcaseb_copyright.txt", joinedDataHandler, data_model, options)
    if result == True:
        options = {OPTION_VALUE_KEY:VALUE_3_KEY}
        result = parseFile("joinedcaseb_copr.txt", joinedDataHandler, data_model, options)
    # Output to File
    if result == True:
        data_model.outputDataModelToFile(OUTPUT_FILE_NAME, OUTPUT_FILE_ENCODING)
        print("Success")
    else:
        print("Expected Error")

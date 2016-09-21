import os
from xlrd import open_workbook
import json
import re
from tqdm import tqdm

json_file_name_USDA = os.path.join(os.getcwd(), 'USDA_DB', 'USDA_DB.json')
json_file_name_USDA_items = os.path.join(os.getcwd(), 'USDA_DB', 'USDA_DB_items.json')
USDA_path = os.path.join(os.getcwd(), 'USDA_DB', 'ABBREV.xlsx')
NUTTAB_path = os.path.join(os.getcwd(), 'NUTTAB_DB','AUSNUT_2013.xls')


def excel_to_json():
    book = open_workbook(USDA_path)

    sheet = book.sheet_by_index(0)

    row = sheet.row(0)
    units = re.compile(u'(\xb5g|mg|IU|RAE|g)')
    header = [
            ('NDB_no', None),
            ('Name', None),
            ('Water', 'g'),
            ('Energy', 'Kcal'),
            ('Protein', 'g'),
            ('Lipid_total', 'g'),
            ('Ash', 'g'),
            ('Carbohydrates', 'g'),
            ('Fibre', 'g'),
            ('Sugar_total', 'g'),
            ('Calcium', 'mg'),
            ('Iron', 'mg'),
            ('Magnesium', 'mg'),
            ('Phosphorus', 'mg'),
            ('Potassium', 'mg'),
            ('Sodium', 'mg'),
            ('Zinc', 'mg'),
            ('Copper', 'mg'),
            ('Manganes', 'mg'),
            ('Selenium', 'mg'),
            ('Vit_C', 'mg'),
            ('Thiamin', 'mg'),
            ('Riboflavin', 'mg'),
            ('Niacin', 'mg'),
            ('Panto Acid', 'mg'),
            ('Vit_B6', 'mg'),
            ('Folate_total', 'ug'),
            ('Folic_acid', 'ug'),
            ('Food_folate', 'ug'),
            ('Folate_FDE', 'ug'),
            ('Choline_total', 'mg'),
            ('Vit_B12', 'mg'),
            ('Vit_A', 'UI'),
            ('Vit_A', 'RAE'),
            ('Retinol', 'ug'),
            ('Alpha_carot', 'ug'),
            ('Deta_carot', 'ug'),
            ('Beta_Crypt', 'ug'),
            ('Lycopene', 'ug'),
            ('Lut_Zea', 'ug'),
            ('Vit_E', 'mg'),
            ('Vit_D', 'ug'),
            ('Vit_D', 'IU'),
            ('Vit_K', 'ug'),
            ('FA_Sat', u'g'),
            ('FA_Mono', u'g'),
            ('FA_Poly', u'g'),
            ('Cholestrl', u'mg'),
            ('GmWt_1', 'g'),
            ('GmWt_desr_1', None),
            ('GmWt_2', 'g'),
            ('GmWt_desr_2', None),

            ]


    # for index, (name, unit) in enumerate(header):
    #     print name, unit, index
    ## define headers of excel sheet
    headers = []

    DB_dict = {}
    ## open json file for dumping:
    with open(json_file_name_USDA,'w') as f:
        #iterate through rows:
        for row_idx in tqdm(range(1, sheet.nrows)):
            item_dict = {}
            # iterate through the items in the row and build dictionary
            for col_idx, (name, units) in enumerate(header):
                item_dict[name] = {}
                item_dict[name]['value'] = sheet.cell(row_idx, col_idx).value
                item_dict[name]['units'] = units

            DB_dict[item_dict['NDB_no']['value']] = item_dict
        json.dump(DB_dict, f)
        f.close()

    return 0


def get_DB_items():
    book = open_workbook(USDA_path)
    sheet = book.sheet_by_index(0)
    item_list_dict = {}
    with open(json_file_name_USDA_items, 'w') as f:
        for row_idx in tqdm(range(1, sheet.nrows)):
            item_no = sheet.cell(row_idx, 0).value
            item_name = sheet.cell(row_idx, 1).value
            item_list_dict[item_no] = item_name


        json.dump(item_list_dict, f)
        f.close()
    return item_list_dict


if __name__ == '__main__':
    # excel_to_json()
    print len(get_DB_items().values())

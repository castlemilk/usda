# purse of this script is to handle the conversion of the USDA food DB in ugly
# delimitered syntax text files into a relational postgres format and load it in
from sqlalchemy import create_engine, MetaData, select, join
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
import pandas as pd
import json
from tqdm import tqdm
import os
import sys
import codecs
from chardet.universaldetector import UniversalDetector
import models
import argparse
import time
def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print '%s function took %0.3f ms' % (f.func_name, (time2-time1)*1000.0)
        return ret
    return wrap


detector = UniversalDetector()
def get_encoding_type(current_file):
    detector.reset()
    for line in file(current_file):
        detector.feed(line)
        if detector.done: break
    detector.close()
    return detector.result['encoding']

def convert_file(file_name, target_format='utf-8'):
    """ Will create new file that has been converted to the target format"""
    filename, ext = (''.join(file_name.split('.')[0:-1]), file_name.split('.')[-1])
    new_filename = filename+target_format+'.'+ext
    encoding_type = get_encoding_type(file_name)
    source = open(file_name)
    target = open(new_filename, "wb")
    target.write(unicode(source.read(), encoding_type).encode(target_format))
    target.close()
    source.close()
    return new_filename


class USDA_DB:
    """Parse USDA Nutiriton DB"""
    def __init__(self, database_name='USDA_DB', USDA_file_path=os.path.join(os.getcwd(), 'sr28asc')):
        """initialise DB"""
        self.engine = create_engine('postgresql://localhost/%s' %database_name)
        self.base_path = USDA_file_path
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.database = Session()

    def build_table(self,file_name, model, index=False):
        if not self.engine.dialect.has_table(self.engine,model.__tablename__):
            print "buiding table %s from %s...." % (model.__tablename__, file_name),
            file_path = os.path.join(self.base_path,file_name)
            data_matrix = pd.read_csv(convert_file(file_path),
                                                        header=None,
                                                        # encoding=encoding_type,
                                                        delimiter=r'^',
                                                        quotechar=r'~',
                                                        )
            if index:
                fields = model.__table__.columns.keys()
                fields.remove('index')
                data_matrix.columns = fields
            else:
                data_matrix.columns = model.__table__.columns.keys()


            data_matrix.to_sql(name=model.__tablename__,
                               con=self.engine,if_exists='append',index=index)
            print "Done."
            return None
        else:
            print "Table exists, skipping"

    def has_data(self):
        try:

            if(self.engine.dialect.has_table(self.engine,models.FoodDescription.__tablename__)):
                return True
            else:
                return False
        except Exception, e:
            print e
            return False

    def clear_DB(self):
        self.meta = MetaData(bind=self.engine)
        self.meta.reflect()
        self.meta.drop_all()


    def generate_json_documents(self):
        '''
        Converts the Postgres DB into a flat JSON file for export purposes etc
        '''
        foods_q = select([
                    models.FoodDescription,
                    models.FoodGroupDescription.foodgroup_description
                        ]).where(
                        models.FoodGroupDescription.foodgroup_code==\
                        models.FoodDescription.foodgroup_code
                        )

        foods = self.database.execute(foods_q)
        for food in tqdm(foods):

            ndb_no = food.ndb_no
            document = {
                    'group': food.foodgroup_description,
                    'manufacturer': food.manufacturer_name,
                    'name': {
                            'long': food.long_description,
                            'common':[],
                            'sci':food.scientific_name,
                            }
            }

            # add common names
            if food.common_name:
                common_names = [com_name for \
                            com_name in food.common_name.split(',')\
                            if com_name != '']
                document['name']['common'] = document['name']['common'] + common_names

            # add langual food source description to common names
            document['name']['common'] = document['name']['common'] + \
            self.query_langual_foodsource(ndb_no)

            # add nutritional information
            document['nutrients'] = self.query_nutrients(ndb_no)
            document['portions'] = self.query_gramweight(ndb_no)


            # collect miscellanous data into a meta fields
            document['meta'] = {
                            'ndb_no': ndb_no,
                            'nitrogen_factor': str(food.n_factor),
                            'protein_factor': str(food.pro_factor),
                            'fat_factor': str(food.fat_factor),
                            'carb_factor': str(food.cho_factor),
                            'fndds_survey': food.survey,
                            'ref_desc': food.reference_description,
                            'refuse':food.refuse,
                            'footnotes': self.query_foodnote(ndb_no),
                            'langual': self.query_langual_foodsource(ndb_no),

                                }
            json.dumps(document)










    @timing
    def query_nutrients(self, ndb_no):
        nutrients = []
        j_1 = outerjoin(models.NutritionData,
            models.SourceCodeDescription,
            models.NutritionData.src_code==models.SourceCodeDescription.src_code)
        j_2 = outerjoin(j_1,
                models.DataDerivativeCodeDescription,
                models.NutritionData.derive_code==models.DataDerivativeCodeDescription.derivation_code)
        nut_table = select([
        models.NutritionData,
        models.NutritionDefinition.units,
        models.NutritionDefinition.nutrition_description,
        models.NutritionDefinition.tag_name,
        models.NutritionDefinition.num_decimal,
        models.NutritionDefinition.sr_order,
        models.SourceCodeDescription.src_cd_description,
        models.DataDerivativeCodeDescription],
                                    ).select_from(j_2).where(
                                and_(
                                    models.NutritionData.nutrition_no==\
                                    models.NutritionDefinition.nutrition_no,
                                    models.NutritionData.ndb_no==ndb_no
                                    )
                                    )

        for row in self.database.execute(nut_table):
            nutrient_filtered = {
                    'ndb_no': row.ndb_no,
                    'name': row.nutrition_description,
                    'abbr': row.tag_name,
                    'value': str(row.nutrition_value),
                    'units': row.units,
                    'meta': {
                            'imputed': row.ref_ndb_no,
                            'is_add': row.add_nutrition_mark,
                            'rounded': row.num_decimal,
                            'conf': row.confidence_code,
                            'mod_month': row.add_modification_date[0:2] if \
                            row.add_modification_date else None,
                            'mod_year': row.add_modification_date[3:] if \
                            row.add_modification_date else None,
                            'lower_error': str(row.lower_eb),
                            'upper_error': str(row.upper_eb),
                            'std_error': str(row.std_error),
                            'data_points': row.num_datapoints,
                            'minval': str(row.min_value),
                            'maxval': str(row.max_value),
                            'DoF': str(row.degrees_of_freedom),
                            'stat_comments': row.statistical_comment,
                            # 'sources': self.query_source_ids(row.ndb_no,
                            #                                 row.nutrition_no),
                            'source_type': row.src_cd_description,
                            'derivation': row.derivation_description,
                            'studies': str(row.num_studies),
                            }


            }
            nutrients.append(nutrient_filtered)


        return nutrients
    @timing
    def query_source_ids(self, ndb_no, nutrition_no):
        src_ids = []
        source_ids_q = select([
                    models.SourceOfDataLink.data_source_id
                            ]).where(
                            and_(
                    models.SourceOfDataLink.ndb_no==ndb_no,
                    models.SourceOfDataLink.nutrition_no==nutrition_no
                            )
                            )

        rows= self.database.execute(source_ids_q)
        for row in rows:
            src_ids.append(row.data_source_id)
        return src_ids
    @timing
    def query_langual_foodsource(self, ndb_no):
        '''Query the nutrition DB for the langual ....'''
        thesaurus = []
        langual_foodsource_q = select([
                        models.LanguaLFactor.factor_code,
                        models.LanguaLFactor.ndb_no,
                        models.LangualFactorDescription.description,
                            ]).where(
                            and_(
                            models.LanguaLFactor.factor_code==\
                            models.LangualFactorDescription.factor_code,
                            models.LanguaLFactor.factor_code.notlike('B'),
                            models.LanguaLFactor.ndb_no==ndb_no
                            )
                            )
        rows = self.database.execute(langual_foodsource_q)

        for row in rows:
            thesaurus.append({'code':row.factor_code, 'description':row.description})

        return thesaurus
    @timing
    def query_langual(self, ndb_no):
        '''Query the nutrition DB for the langual foodsource description for
            the specified NDB_no
        '''
        thesaurus = []
        langual_foodsource_q = select([
                        models.LanguaLFactor.factor_code,
                        models.LanguaLFactor.ndb_no,
                        models.LangualFactorDescription.description,
                            ]).where(
                            and_(
                            models.LanguaLFactor.factor_code==\
                            models.LangualFactorDescription.factor_code,
                            models.LanguaLFactor.factor_code.notlike('B'),
                            models.LanguaLFactor.ndb_no==ndb_no
                            )
                            )
        rows = self.database.execute(langual_foodsource_q)
        for row in rows:
            thesaurus.append(row.description)

        return thesaurus

    def query_foodnote(self, ndb_no):
        '''Query nutrition DB for footnote information for given NDB_no'''
        thesaurus = []
        footnote_q = select([
                        models.FootNote
                            ]).where(models.FootNote.ndb_no==ndb_no)
        rows = self.database.execute(footnote_q)
        for row in rows:
            package = {
                    'n_code': row.nutrition_no,
                    'type': row.footnote_type,
                    'text': row.footnote_text,
                        }
            thesaurus.append(package)
        return thesaurus

    def query_gramweight(self, ndb_no):
        '''Query nutrition DB for gram weight information for given NDB_no'''
        thesaurus = []
        footnote_q = select([
                        models.Weight
                            ]).where(
                            models.Weight.ndb_no==ndb_no
                            ).order_by(models.Weight.sequence_no)
        rows = self.database.execute(footnote_q)
        for row in rows:
            package = {
                    'amount': str(row.unit_amount),
                    'unit': row.measurement_description,
                    'g': str(row.gram_weight),
                        }
            thesaurus.append(package)
        return thesaurus


def main():
    # detector = UniversalDetector()
    db = USDA_DB()
    # db.query_nutrients('1111')
    # print db.has_data()

    # Setup command line parsing
    parser = argparse.ArgumentParser(description='''Parses USDA nutrient database flat files and coverts it into SQLite database.
    Also provides options for exporting the nutrient data from the SQLite database into other formats.''')

    # Add arguments
    parser.add_argument('-p', '--path', dest='path', help='The path to the nutrient data files. (default: data/sr28/)', default='data/sr28/')
    parser.add_argument('-db', '--database', dest='database', help='The name of the SQLite file to read/write nutrient info. (default: nutrients.db)', default='nutrients.db')
    parser.add_argument('-f', '--force', dest='force', action='store_true', help='Whether to force refresh of database file from flat file. If database file already exits and has some data in it we skip flat file parsing.')
    parser.add_argument('-e', '--export', dest='export', action='store_true', help='Converts nutrient data into json documents and outputs to standard out, each document is seperated by a newline.')
    args = vars(parser.parse_args())
    print args
    path = args['path']
    if args['force']:
        print "clearing DB"
        db.clear_DB()

    if not db.has_data():
        print "Building tables..."
        db.build_table('FOOD_DES.txt', models.FoodDescription)
        db.build_table('FD_GROUP.txt', models.FoodGroupDescription)
        db.build_table('LANGUAL.txt', models.LanguaLFactor)
        db.build_table('LANGDESC.txt', models.LangualFactorDescription)
        db.build_table('NUT_DATA.txt', models.NutritionData)
        db.build_table('NUTR_DEF.txt', models.NutritionDefinition)
        db.build_table('SRC_CD.txt', models.SourceCodeDescription)
        db.build_table('DERIV_CD.txt', models.DataDerivativeCodeDescription)
        db.build_table('WEIGHT.txt', models.Weight)
        db.build_table('FOOTNOTE.txt', models.FootNote, index=True)
        db.build_table('DATA_SRC.txt', models.SourceOfData)
        db.build_table('DATSRCLN.txt', models.SourceOfDataLink)
    else:
        print "Tables exist"


    if args['export']:
        print "converting to JSON document"
        db.generate_json_documents()


    # db.query_nutrients('1001')

    # print db.query_source_ids('1001','306')

    # print db.query_langual_foodsource(2001)
    # db.generate_json_file()

if __name__=='__main__':
    #build_food_group()
    # build_food_description()
    # build_nutrition_data()
    main()

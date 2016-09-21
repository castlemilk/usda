from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, NUMERIC
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
class FoodDescription(Base):
    """postgres table holding food description information"""
    __tablename__ = 'food_description'
    ndb_no = Column(String(5), nullable = False,primary_key = True)
    foodgroup_code = Column(String(4),nullable = False)
    long_description = Column(String(200), nullable = False)
    short_description = Column(String(60), nullable = False)
    common_name = Column(String(100), nullable=True)
    manufacturer_name = Column(String(100))
    survey = Column(String(1))
    reference_description = Column(String(135))
    refuse = Column(Integer)
    scientific_name = Column(String(65))
    n_factor = Column(NUMERIC(4,2))
    pro_factor = Column(NUMERIC(4,2))
    fat_factor = Column(NUMERIC(4,2))
    cho_factor = Column(NUMERIC(4,2))


class FoodGroupDescription(Base):
    __tablename__ = 'foodgroup_description'

    foodgroup_code = Column(String(4),
    # ForeignKey('food_description.foodgroup_code'),
    nullable=False, primary_key=True)

    foodgroup_description = Column(String(60), nullable=False)

class FootNote(Base):
    __tablename__ = 'footnote'
    index = Column(Integer, nullable=False, primary_key=True)
    ndb_no = Column(String(5),
    # ForeignKey('food_description.ndb_no'),
     nullable = False)
    footnote_no = Column(String(4), nullable = False)
    footnote_type = Column(String(1), nullable= False)
    nutrition_no = Column(String(6), nullable=True)
    footnote_text = Column(String(200), nullable=False)

class Weight(Base):
    __tablename__ = 'weight'
    ndb_no = Column(String(5),
    # ForeignKey('food_description.ndb_no'),
     nullable=False, primary_key = True)
    sequence_no = Column(String(2), nullable=False, primary_key=True)
    unit_amount = Column(NUMERIC(5,2),nullable=False)
    measurement_description = Column(String(84), nullable=False)
    gram_weight = Column(NUMERIC(7,1), nullable=False)
    num_data_points = Column(Integer, nullable=True)
    std_deviation = Column(NUMERIC(7,3), nullable=True)

class NutritionData(Base):
    __tablename__ = 'nutrition_data'
    ndb_no = Column(String(5),
    # ForeignKey('food_description.ndb_no'),
    nullable = False,primary_key = True)
    nutrition_no = Column(String(3), nullable=False, primary_key = True)
    nutrition_value = Column(NUMERIC(10,3), nullable=False)
    num_datapoints = Column(NUMERIC(5,0), nullable=False)
    std_error = Column(NUMERIC(8,3), nullable=True)
    src_code = Column(String(2),nullable=False)
    derive_code = Column(String(4), nullable=True)
    ref_ndb_no = Column(String(10), nullable=True)
    add_nutrition_mark = Column(String(1), nullable=True)
    num_studies = Column(NUMERIC(2), nullable=True)
    min_value = Column(NUMERIC(10,3), nullable = True)
    max_value = Column(NUMERIC(10,3), nullable = True)
    degrees_of_freedom = Column(NUMERIC(4), nullable=True)
    lower_eb = Column(NUMERIC(10,3), nullable = True)
    upper_eb = Column(NUMERIC(10,3), nullable = True)
    statistical_comment = Column(String(10), nullable=True)
    add_modification_date = Column(String(10), nullable=True)
    confidence_code = Column(String(1), nullable = True)


class NutritionDefinition(Base):
    __tablename__ = 'nutrition_definition'
    nutrition_no = Column(String(3),
    # ForeignKey('nutrition_data.nutrition_no'),
    nullable=False, primary_key=True)
    units = Column(String(7), nullable=False)
    tag_name = Column(String(20), nullable=True)
    nutrition_description = Column(String(60), nullable=False)
    num_decimal = Column(String(1), nullable=False)
    sr_order = Column(String(6), nullable=False)

class SourceCodeDescription(Base):
    __tablename__ = 'source_code_description'
    src_code = Column(String(2),
    # ForeignKey('nutrition_data.source_code'),
    nullable=False, primary_key=True)
    src_cd_description = Column(String(60), nullable=False)

class DataDerivativeCodeDescription(Base):
    __tablename__ = 'data_derivation_code_description'
    derivation_code = Column(String(4),
    # ForeignKey('nutrition_data.source_code'),
     nullable=False, primary_key = True)
    derivation_description = Column(String(120), nullable=False)

class SourceOfDataLink(Base):
    __tablename__ = 'source_of_data_link'
    ndb_no = Column(String(5),
    # ForeignKey('food_description.ndb_no'),
    nullable=False, primary_key = True)
    nutrition_no = Column(String(3), nullable=False, primary_key=True)
    data_source_id = Column(String(6), nullable=False,primary_key=True)
class SourceOfData(Base):
    __tablename__ = 'source_of_data'
    data_source_id = Column(String(6),
    # ForeignKey('source_of_data_link.data_source_id'),
    nullable=False, primary_key=True)
    authors = Column(String(255), nullable=True)
    title = Column(String(255), nullable=False)
    year = Column(String(6), nullable=True)
    journal = Column(String(135), nullable=True)
    volume_city = Column(String(16), nullable=True)
    issue_state = Column(String(8), nullable=True)
    start_page = Column(String(8), nullable=True)
    end_page = Column(String(8), nullable=True)



class LanguaLFactor(Base):
    __tablename__ = 'langual_factor'
    ndb_no = Column(String(5),
    # ForeignKey('food_description.ndb_no'),
     nullable=False, primary_key=True)
    factor_code = Column(String(5), nullable=False, primary_key=True)



class LangualFactorDescription(Base):
    __tablename__ = 'langual_factor_description'
    factor_code = Column(String(5),
    # ForeignKey('langual_factor.factor_code'),
     nullable=False, primary_key = True)
    description = Column(String(140), nullable=False)





def initialise_DB():
    engine = create_engine('postgresql+psycopg2://localhost/USDA_DB', echo=True)
    Base.metadata.create_all(engine)

def clear_DB():
    engine = create_engine('postgresql+psycopg2://localhost/USDA_DB', echo=True)
    Base.metadata.drop_all(engine)

def get_tables():
    engine = create_engine('postgresql+psycopg2://localhost/USDA_DB', echo=True)
    return Base.metadata.sorted_tables



if __name__=='__main__':
    clear_DB()
    initialise_DB()
    # clear_DB()

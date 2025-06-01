import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
log_file = "log_file.txt" 
target_file = "transformed_data.csv" 
def extract_from_csv(filename):
    df=pd.read_csv(filename)
    return df
def extract_from_json(filename):
    df=pd.read_json(filename,lines=True)
    return df
def extract_from_xml(filename):
    df=pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree=ET.parse(filename)
    root=tree.getroot()
    for car in root:
        car_model=car.find('car_model').text
        year_of_manufacture=car.find('year_of_manufacture').text
        price=float(car.find('price').text)
        fuel=car.find('fuel').text
        df=pd.concat([df,pd.DataFrame([{'car_model':car_model,'year_of_manufacture':year_of_manufacture,'price':price,'fuel':fuel}])],ignore_index=True)
    return df
def extract():
    extracted_data=pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    for csv in glob.glob('*.csv'):
        if csv != target_file:
            extracted_data=pd.concat([extracted_data,pd.DataFrame(extract_from_csv(csv))],ignore_index=True)
    for json in glob.glob('*.json'):
        extracted_data=pd.concat([extracted_data,pd.DataFrame(extract_from_json(json))],ignore_index=True)
    for xml in glob.glob('*.xml'):
        extracted_data=pd.concat([extracted_data,pd.DataFrame(extract_from_xml(xml))],ignore_index=True)
    return extracted_data
    

def transform(data):
    data['price']=round(data.price,2)
    return data
def load_data(target_file,transformed_data):
    transformed_data.to_csv(target_file)
def log_progress(message):
    timestampform='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    timestamp=now.strftime(timestampform)
    with open(log_file,'a') as f:
        f.write(timestamp+','+message+'\n')
log_progress('etl started')
log_progress('extract started')
extracted_data=extract()
log_progress('transform started')
transformed_data=transform(extracted_data)
print("Transformed Data") 
print(transformed_data) 
log_progress('load started')
load_data(target_file,transformed_data)
log_progress('etl ended')


    
from bs4 import BeautifulSoup
import random
import string
import zipfile
from zipfile import ZipFile
from multiprocessing import Process
from multiprocessing import Queue
import os

from os import listdir
from os.path import isfile, join

import pandas as pd

def return_value(param):
    """ """
    length_str = 18
    if param == "rand_int":
        return 100*random.random()
    elif param == "rand_str":
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length_str))
    else:
        return param


def create_xml():
    """ """

    soup = BeautifulSoup()

    tags = [
        {"tag": "root", "count": 1, "parent": None},
        {"tag": "var", "count": 2, "parent": "root",
            "attr": {"name": ["id", "level"], "value": ["rand_str", "rand_int"]}},
        {"tag": "objects", "count": 1, "parent": "root"},
        {"tag": "object", "count": 10, "parent": "objects",
            "attr": {"name": "rand_str"}}]

    level = 0;

    for tag_dict in tags:
        parent = soup.find(tag_dict["parent"])
        for tag_number in range(tag_dict["count"]):
            new_tag = soup.new_tag(tag_dict["tag"])
            attr_dict = tag_dict.get("attr")
            if attr_dict is not None:
                for attr_key in attr_dict:
                    if type(attr_dict[attr_key]) is list:
                        param = attr_dict[attr_key][tag_number]
                        new_tag[attr_key] = return_value(param)
                    else:
                        new_tag[attr_key] = return_value(attr_dict[attr_key])

            if parent is None:
                soup.insert(0, new_tag)
            else:
                parent.insert(len(parent), new_tag)
    
    return soup.prettify()



def create_zip_with_xmls(list_zip_name):
    """ """
    count_xml = 100
    for num_zip in list_zip_name:
        with ZipFile('zip/{}.zip'.format(num_zip), 'w') as myzip:
            for num_xml in range(count_xml):
                xml = create_xml()
                myzip.writestr('{}_{}.xml'.format(num_zip, num_xml), xml)

def multiprocess_zip_creator():
    p = []
    for cpu in range(os.cpu_count()): 
        p.append(
            Process(
            target=create_zip_with_xmls,
            args=(list(
                range(cpu, 50, os.cpu_count())),)))
        p[-1].start()
    for _ in p:
        _.join()


def multiprocess_zip_read(mypath):
    onlyzip = [join(mypath, f) for f in listdir(mypath) if zipfile.is_zipfile(join(mypath, f))]
    p = []
    q = Queue()
    id_level = []
    id_object_name = []
    for cpu in range(os.cpu_count()):
        top = int(len(onlyzip)/os.cpu_count())
        upper = (cpu+1)*top if (cpu+2)*top < 50 else len(onlyzip)
        file_names = onlyzip[cpu*top : upper]
        p.append(
            Process(
            target=read_zip,
            args=(file_names, q)))
        p[-1].start()
    for _ in p:
        var = q.get()
        id_level.extend(var[0])
        id_object_name.extend(var[1])
    for _ in p:
        _.join()


    pd_id_level = pd.DataFrame(id_level, columns=["id","level"])
    pd_id_name = pd.DataFrame(id_object_name, columns=["id","object_name"])

    print(pd_id_level["id"].nunique(), pd_id_name["id"].nunique())
    pd_id_level.to_csv("pd_id_level.csv", sep='\t', encoding='utf-8')
    pd_id_name.to_csv("pd_id_name.csv", sep='\t', encoding='utf-8')

def read_zip(file_names, q):
    print(file_names)
    object_id_name = []
    id_level = []
    for file_name in file_names:
        with ZipFile(file_name) as myzip:
            for file_name in myzip.namelist():
                with myzip.open(file_name) as myfile:
                    soup = BeautifulSoup(myfile.read())
                    vars_ = soup.root.find("var", attrs={"name": "id"})
                    id_ = vars_["value"]
                    vars_ = soup.root.find("var", attrs={"name": "level"})
                    level = vars_["value"]
                    id_level.append([id_, level])
                    all_object = soup.root.objects.findAll("object")
                    for object_ in all_object:
                        object_id_name.append([id_, object_["name"]])
    q.put([id_level, object_id_name])
            
    

if __name__ == "__main__":
    multiprocess_zip_creator()
    multiprocess_zip_read("zip")



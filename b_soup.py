""" Create and parsing XML."""

import random
import string
import os
from os import cpu_count
from os import listdir
from os.path import join
from multiprocessing import Process
from multiprocessing import Queue
from zipfile import ZipFile
from zipfile import is_zipfile

from bs4 import BeautifulSoup
import pandas as pd


def return_value(param):
    """ Return random int, random str or param. """
    answer = param
    length_str = 18
    if param == "rand_int":
        answer = 100 * random.random()
    elif param == "rand_str":
        answer = ''.join(
            random.choice(
                string.ascii_uppercase + string.digits)
            for _ in range(length_str))
    return answer


def create_xml():
    """ Create XML. Return string (soup.prettify()). """
    soup = BeautifulSoup()

    tags = [
        {"tag": "root", "count": 1, "parent": None},
        {"tag": "var", "count": 2, "parent": "root",
         "attr": {"name": ["id", "level"], "value": ["rand_str", "rand_int"]}},
        {"tag": "objects", "count": 1, "parent": "root"},
        {"tag": "object", "count": int(random.uniform(1, 11)),
         "parent": "objects", "attr": {"name": "rand_str"}}]

    for tag_dict in tags:
        parent = soup.find(tag_dict["parent"])
        for tag_number in range(tag_dict["count"]):
            new_tag = soup.new_tag(tag_dict["tag"])
            attr_dict = tag_dict.get("attr")
            if attr_dict is not None:
                for attr_key in attr_dict:
                    if isinstance(attr_dict[attr_key], list):
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
    """ Create zip with {count_xml} xmls. """
    count_xml = 100
    if not os.path.exists('zip'):
        os.makedirs('zip')
    for num_zip in list_zip_name:
        with ZipFile('zip/{}.zip'.format(num_zip), 'w') as myzip:
            for num_xml in range(count_xml):
                xml = create_xml()
                myzip.writestr('{}_{}.xml'.format(num_zip, num_xml), xml)


def multiprocess_zip_creator():
    """ Start {cpu_count()} process create_zip_with_xmls. """
    pool_process = []
    num_zip = 50
    for cpu in range(cpu_count()):
        pool_process.append(
            Process(
                target=create_zip_with_xmls,
                args=(list(
                    range(cpu, num_zip, cpu_count())),)))
        pool_process[-1].start()
    for process in pool_process:
        process.join()


def multiprocess_zip_read(mypath):
    """ Start {cpu_count()} process read_zip in dir mypath.
        Save result to csv (pd_id_level.csv, pd_id_name.csv).
    """
    onlyzip = [join(mypath, f)
               for f in listdir(mypath)
               if is_zipfile(join(mypath, f))]
    pool_process = []
    queue = Queue()
    id_level = []
    id_object_name = []
    for cpu in range(cpu_count()):
        step = int(len(onlyzip)/cpu_count())
        top = (cpu+1)*step if (cpu+2)*step <= len(onlyzip) else len(onlyzip)
        file_names = onlyzip[cpu*step : top]
        pool_process.append(
            Process(
                target=read_zip,
                args=(file_names, queue)))
        pool_process[-1].start()
    for _ in range(len(pool_process)):
        list_id_level_id_name = queue.get()
        id_level.extend(list_id_level_id_name[0])
        id_object_name.extend(list_id_level_id_name[1])
    for process in pool_process:
        process.join()

    pd_id_level = pd.DataFrame(id_level, columns=["id", "level"])
    pd_id_name = pd.DataFrame(id_object_name, columns=["id", "object_name"])
    #print(pd_id_level["id"].nunique(), '\n', pd_id_name.count())
    pd_id_level.to_csv("pd_id_level.csv", sep='\t', encoding='utf-8')
    pd_id_name.to_csv("pd_id_name.csv", sep='\t', encoding='utf-8')


def read_zip(file_names, queue):
    """ Read zip with name from file_names.
        Open xml from zip.
        Parsing id, level, object_name from xml.
        Send result to queue.
    """
    id_object_name = []
    id_level = []
    for zip_file_name in file_names:
        with ZipFile(zip_file_name) as myzip:
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
                        id_object_name.append([id_, object_["name"]])
    queue.put([id_level, id_object_name])


if __name__ == "__main__":
    multiprocess_zip_creator()
    multiprocess_zip_read("zip")

from pymongo import MongoClient
from bson.objectid import ObjectId
from gridfs import *
import getpass
import os, zipfile
import time
import tarfile

class MongoFile():
    def __init__(self):
        pass

    def connect_server(self, address='localhost', port=27017):
        self.client = MongoClient(address, port)
        self.usr = getpass.getuser()
        self.tamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.file_name = self.usr + "_" + self.tamp
        print "Current User is : ", self.usr

    def make_targz(self, source_dir):
        self.gz_name = self.file_name + ".gz"
        with tarfile.open(self.gz_name, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

    def insert_file(self, database, collection, source_dir, keep_org_file = False):
        db = self.client[database]  # connect to database
        fs = GridFS(db, collection)  # conncet to a specific collection
        self.make_targz(source_dir)
        file_path = "./" + self.gz_name
        with open(file_path.decode('utf-8'), 'rb') as myfile: # read as binary format
            data = myfile.read()
            id = fs.put(data, filename=self.file_name)
            print id
        if not keep_org_file:
            gz_path = "./"+self.gz_name
            os.remove(gz_path)

    def get_file(self, database, collection, file_name, output_path="./"):
        db = self.client[database]  # connect to database
        fs = GridFS(db, collection)  # conncet to a specific collection
        file = fs.get_version(file_name)
        data = file.read()
        out_path = output_path + file_name +".gz"
        out = open(out_path.decode('utf-8'), 'wb') # write as binary format
        out.write(data)
        out.close()

    def get_id(self, database, collection, file_name):
        db = self.client[database]  # connect to database
        fs = GridFS(db, collection)  # conncet to a specific collection
        file = fs.get_version(file_name)
        return file._id

    def del_file(self, database, collection, file_name):
        id = self.get_id(self, database, collection, file_name, author)
        db = self.client[database]  # connect to database
        fs = GridFS(db, collection)  # conncet to a specific collection
        fs.delete(ObjectId(id))

    def list_name(self, database, collection):
        db = self.client[database]
        fs = GridFS(db, collection)
        print "File Name List: ", fs.list()
        return fs.list()

    def close_connection(self):
        print "Close MongoDB Connection"
        self.client.close()
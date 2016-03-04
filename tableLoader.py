import glob
import requests
import util
from multiprocessing import Pool

class TableLoader:
    
    def __init__(self, directory, dumped):
        self.directory = directory
        self.dumped = dumped

    def buildAndSendRequest(self, tableName):
        if self.dumped:
            loadTableRequest = self.buildLoadTableRequestDumped(tableName)
        else:
            loadTableRequest = self.buildLoadTableRequest(tableName)
        requests.post("http://localhost:5000/jsonQuery", data = loadTableRequest)

    def buildLoadTableRequest(self, tableName):
            loadTableRequest = {'query': '{\
                "operators": {\
                    "loadTable": {\
                        "type" : "GetTable",\
                        "name" : "%s"\
                    },\
                    "NoOp": {\
                        "type" : "NoOp"\
                    }\
                },\
                "edges" : [\
                    ["loadTable", "NoOp"]\
                ]\
            }' % (tableName)}

            return loadTableRequest

    def buildLoadTableRequestDumped(self, tableName):
            loadTableRequest = {'query': '{\
                "operators": {\
                    "loadDumpedTable": {\
                        "type" : "LoadDumpedTable",\
                        "name" : "%s_dumped"\
                    },\
                    "NoOp": {\
                        "type" : "NoOp"\
                    }\
                },\
                "edges" : [\
                    ["loadDumpedTable", "NoOp"]\
                ]\
            }' % (tableName)}

            return loadTableRequest

    def getTableNamesDumped(self):
        tableNames = []
        filenames = glob.glob("%s/*_dumped" % (self.directory))

        for filename in filenames:
            filename = filename.split("_dumped")[0]
            tableNames.append(filename.split('/')[-1])

        return tableNames

    def getTableNamesTbl(self):
        tableNames = []
        filenames = glob.glob("%s/*.tbl" % (self.directory))

        for filename in filenames:
            filename = filename.split(".tbl")[0]
            tableNames.append(filename.split('/')[-1])

        return tableNames

    def getTableNames(self):
        if self.dumped:
            tableNames = self.getTableNamesDumped()
        else:
            tableNames = self.getTableNamesTbl()

        tableNames.sort()
        return tableNames

    def loadTables(self):
        print "Load all tables in directory: %s" % (self.directory)

        tableNames = self.getTableNames()

        threadPool = Pool(len(tableNames))

        threadPool.map(self.buildAndSendRequest, tableNames)

        threadPool.close()
        threadPool.join()

        print "Succesfully loaded %i tables" % (len(tableNames))
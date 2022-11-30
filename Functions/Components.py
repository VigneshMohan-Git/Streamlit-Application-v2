#import spark as spark_
from transform import pandas_join, pandas_select, logical, datetime, addColumn, concatColumns, \
    applyAggregartion, limitedRecords, operatorFiltering, isInFiltering, startWith, contains, GroupBy,columnname, \
    dropna_, rm_duplicates, ColCompare, mathops_sub, assignColValue, stripspaces, toNumeric, pandas_dtype, pandas_removeInf
from read import Read
# -- Read Wrapper
from write_ import Write



def readData(file_path, file_type,sheetname=None, read=None):
    if file_type.lower() == 'csv':
        return read.pandas_csv(file_path)
    elif file_type.lower() == 'excel':
        return read.pandas_excel(file_path, sheetname)
    elif file_type.lower() == 'json':
        return read.readjson(file_path)
    elif file_type.lower() == 'xlsx':
        return read.pandas_excel(file_path, sheetname)


def db_readData(conn_type, server, database, port, username, password, query, read):
    if conn_type.lower() == 'sql_connection':
        Driver = "ODBC Driver 17 for SQL Server"
        return read.pyodbc_connection(server, database, port, username, password, query, Driver)
    elif conn_type.lower() == 'oracle_connection':
        Driver = "Oracle in OraClient11g_home1"
        return read.pyodbc_connection(server, database, port, username, password, query, Driver)


def writeData(file_type, tempPath, fileName, df,write,sheetName="default"):
    import os
    createDirectory(tempPath)
    file=tempPath+os.path.sep+fileName
    if file_type.lower() == 'csv':
        print(tempPath)
        return write.pandas_csv(df,tempPath,fileName)
    elif file_type.lower()=='xlsx':
         return df.to_excel(file,sheet_name=sheetName)
    elif file_type.lower()=='json':
        write.writejson(fileName,tempPath,df)



def transformDate(df,data):
    date=datetime()
    for key,value in data.items():
        for col in value:
            df=date.applydateformat(df,key,col)
    return df

def addColumns(df,data):
    for key,value in data.items():
            df=addColumn(df,key,value)
    return df


def ConcatColumns(df,data):
    for key,value in data.items():
            df=concatColumns(df,key,value)
    return df

def compare(df,value):
    key_1 = value["key1"]
    key_2 = value["key2"]
    ops_ = value["ops"]
    return ColCompare(df, key_1, key_2, ops_)


def mathops(df, value):
    ops_ = value["ops"]
    val1_ = value["val1"]
    val2_ = value["val2"]
    new_column_name_ = value["new_column_name"]
    return mathops_sub(df, new_column_name_ , val1_, val2_, ops_)

def assignColValue_Bool(df,value):
    newcolumn_= value["newcolumn"]
    col1_= value["col1"]
    col2_= value["col2"]
    assign1_= value["assign1"]
    assign2_= value["assign2"]
    ops_ = value["ops"]
    return assignColValue(df, newcolumn_, col1_, col2_, assign1_, assign2_, ops_)

def fillnaval(df, value):
    return df.fillna(value)

def removespaces(df, value):
    option_ = value["option_"]
    colname_ = value["colname"]
    return stripspaces(df, colname_, option_)


def filtering(df,data):
    for key, value in data.items():
        if key.lower() =="operators":
             df=operatorFiltering(df,value)
        elif key.lower()=="in":
             df=isInFiltering(df,value)
        elif key.lower()=="startwith":
             df=startWith(df,value)
        elif key.lower()=="contains":
             df=contains(df,value)
        elif key.lower()=="columnname":
             df=columnname(df,value)
        else:
            print("Not Supported Filtering")
    return df

def groupBY(df,value):
    aggregration=value["aggr"]
    select_Columns=value["selectcolumns"]
    groupColumns=value["groupcolumns"]
    newColumn = value["newcolumn"]
    return GroupBy(df,groupColumns,select_Columns,aggregration,newColumn)

def transform(df,metadata):
    transform_types= metadata['transform_types']
    log=logical()
    for transform_type_map in transform_types:
      for transform_type, value in transform_type_map.items():
        if transform_type=="rename":
            df=log.rename(df,value)
            print("Renamed")
        elif transform_type=="date":
             df=transformDate(df,value)
        elif transform_type=='add':
             df=addColumns(df,value)
        elif transform_type=="concat":
            print("Concatnating")
            df=ConcatColumns(df,value)
            print("Concatnated")
        elif transform_type=="aggregation":
            df=applyAggregartion(df,value)
        elif transform_type=="pandas_select":
            df=pandas_select(df, value)
        elif transform_type=="limit":
            df=limitedRecords(df,value)
        elif transform_type=="groupBY":
            print("GroupBy Applying")
            df=groupBY(df,value)
            print("GroupBy Applyed")
        elif transform_type == "filter":
            df = filtering(df, value)
        elif transform_type == "dropna":
            df = dropna_(df, value)
        elif transform_type == "rm_duplicates":
            df = rm_duplicates(df, value)
        elif transform_type == "compare":
            df = compare(df, value)
        elif transform_type == "mathops":
            df = mathops(df, value)
            print("Math Operation Applied")
        elif transform_type == "boolops":
            df = assignColValue_Bool(df, value)
            print("ColValue_Bool Operation Applied")
        elif transform_type == "fillna":
            df = fillnaval(df, value)
            print("Fillna Operation Applied")
        elif transform_type == "remove_spaces":
            df = removespaces(df, value)
            print("Leading & Trailing Spaces Removed")
        elif transform_type == "toNumeric":
            df = toNumeric(df, value)
            print("Removed String value from Numeric Column")
        elif transform_type == "dtypes":
            df = pandas_dtype(df, value)
            print("Applied Columns Data Types")
        elif transform_type == "removeInf":
            df = pandas_removeInf(df, value)
        else:
            print("not supported "+transform_type)
    return df



def selectComponent(runId,TaskId):
    try:
        config, metadata, transaction_type,json_metadata,func_type = INIT(runId, TaskId)
        parents = metadata['parents']
        if transaction_type==FILE_SYSTEM:
           input=prepareInput(parents)
           if func_type == PANDAS:
              df=transform_Select_pandas(input,json_metadata)
              fileName, tempPath = writeTempFile(TaskId, config, df, metadata, runId)
              outputMetadata=UpdateFileOutputMetadata(config['tempFileType'],fileName,tempPath,transaction_type)
        elif transaction_type == IN_MEMORY:
             outputMetadata = {}
             outputMetadata["transaction_type"] = transaction_type
             if func_type == PANDAS:
                 inMemoryDict[metadata['taskName']]=pandas_select(inMemoryDict[parents[0]['taskName']],json_metadata["columns"])
        updateTaskRunId(TaskId, runId, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
          updateTaskRunId(TaskId, runId, config["jinja_url"], "", "failed")
          raise e

def remoteReadComponent(runid,taskid):
    import os
    try:
         config, metadata, transaction_type, json_metadata, func_type = INIT(runid, taskid)
         tempPath = config['tempPath'].format(task_id=taskid, run_id=runid)+os.path.sep+metadata['taskName'] + "." + json_metadata['file_type']
         SSH_GET_FILE(json_metadata['server'],json_metadata['username'],json_metadata['password'],json_metadata['local_path'],tempPath,json_metadata['remoteFile_path'],json_metadata['decrypt'],json_metadata['decryption_key'])
         files=json_metadata['local_path'].split(os.path.sep)
         size=len(files)
         filename=files[size-1]
         path=os.path.dirname(json_metadata['local_path'])
         outputMetadata = UpdateFileOutputMetadata(json_metadata['file_type'], filename,
                                              path, transaction_type)
         updateTaskRunId(taskid, runid, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
           print("remoteReadComponent Exception:", e)
           updateTaskRunId(taskid, runid, config["jinja_url"], "", "failed")
           raise e


def remoteWriteComponent(runid,taskid):
    try:
         config, metadata, transaction_type, json_metadata, func_type = INIT(runid, taskid)
         parents = metadata['parents']
         if len(parents)>0:
             input = prepareInput(parents)
             localpath=input[parents[0]["taskName"]]
         else:
             localpath=json_metadata['local_path']
         SSH_PUT(json_metadata['server'],json_metadata['username'],json_metadata['password'],localpath,json_metadata['remoteFile_path'],json_metadata['encrypt'],json_metadata['encryption_key'])
         updateTaskRunId(taskid, runid, config["jinja_url"], {}, "success")
    except Exception as e:
           print("remoteReadComponent Exception:", e)
           updateTaskRunId(taskid, runid, config["jinja_url"], "", "failed")
           raise e


def readComponent(Runid, taskid):
    config={}
    try:
        config, metadata, transaction_type,json_metadata,func_type= INIT(Runid, taskid)
        read_type= json_metadata["read_type"]
        outputMetadata = {}
        read = Read()
        if read_type== FILE:
            sheetname=""
            if "sheetname" in json_metadata:
               sheetname = json_metadata["sheetname"]
            #if json_metadata["file_type"] == "excel":
            if transaction_type == FILE_SYSTEM:
                outputMetadata=UpdateFileOutputMetadata(json_metadata['file_type'], json_metadata['file_name'],  json_metadata['file_path'], transaction_type,sheetname)
            elif transaction_type == IN_MEMORY:
                outputMetadata["transaction_type"] = transaction_type
                if func_type==PANDAS:
                    inMemoryDict[metadata['taskName']]=readData(json_metadata["file_path"]+json_metadata["file_name"],json_metadata["file_type"],sheetname,read)
                    print(json_metadata["file_name"])

        elif read_type== DATABASE:
            if transaction_type == FILE_SYSTEM:
                df = db_readData(json_metadata['conn_type'], json_metadata['server'],json_metadata['database'], json_metadata['port'],json_metadata['username'], json_metadata['password'],json_metadata['query'], read)
                fileName, tempPath = writeTempFile(taskid, config, df, metadata, Runid)
                outputMetadata = UpdateFileOutputMetadata(config['tempFileType'], fileName, tempPath, transaction_type)
            elif transaction_type == IN_MEMORY:
                outputMetadata["transaction_type"] = transaction_type
                if func_type == PANDAS:
                    inMemoryDict[metadata['taskName']] = db_readData(json_metadata['conn_type'], json_metadata['server'],json_metadata['database'],json_metadata['port'], json_metadata['username'],json_metadata['password'], json_metadata['query'], read)
        elif read_type== REST:
            if transaction_type == FILE_SYSTEM:
                if func_type == PANDAS:
                    df = rest_readData(json_metadata, read)
                    config['tempFileType'] = "json"
                    fileName, tempPath = writeTempFile(taskid, config, df, metadata, Runid)
                outputMetadata = UpdateFileOutputMetadata(config['tempFileType'], fileName, tempPath, transaction_type)
            elif transaction_type == IN_MEMORY:
                outputMetadata["transaction_type"] = transaction_type
                if func_type == PANDAS:
                    inMemoryDict[metadata['taskName']] = rest_readData(json_metadata, read)
        updateTaskRunId(taskid, Runid, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
           print("ReadComponent Exception:", e)
           updateTaskRunId(taskid, Runid, config["jinja_url"], "", "failed")
           raise e


def rest_readData(json_metadata,read):
    import pandas as pd
    import json
    url=json_metadata["url"]
    headers=json_metadata["headers"]
    if 'request_body' in json_metadata:
       request= json_metadata['request_body']
       json_data = json.loads(request)
       data=read.postRequest(json_data,url,headers)
    else:
         data=read.getRestCall(headers,url)
    return data


def joinComponent(runId,TaskId):
    try:
        config, metadata, transaction_type,json_metadata,func_type = INIT(runId, TaskId)
        parents = metadata['parents']
        if transaction_type==FILE_SYSTEM:
           input = prepareInput(parents)
           if func_type == PANDAS:
              df=transform_pandas(input,json_metadata)
              fileName, tempPath = writeTempFile(TaskId, config, df, metadata, runId)
              outputMetadata = UpdateFileOutputMetadata(config['tempFileType'], fileName, tempPath, transaction_type)
        elif transaction_type == IN_MEMORY:
             outputMetadata = {}
             outputMetadata["transaction_type"] = transaction_type
             if func_type == PANDAS:
                 inMemoryDict[metadata['taskName']]=transform_pandas_inMemory(json_metadata,parents)
        updateTaskRunId(TaskId, runId, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
          updateTaskRunId(TaskId, runId, config["jinja_url"], "", "failed")
          raise e

def writeComponent(runId,TaskId):
    try:
        config, metadata, transaction_type,json_metadata,func_type = INIT(runId, TaskId)
        parents = metadata['parents']
        write = Write()
        if transaction_type==FILE_SYSTEM:
           input=prepareInput(parents)
           if func_type == PANDAS:
              dfs=get_df(input)
              if json_metadata['write_type']==FILE:
                 writeData(json_metadata['file_type'],json_metadata['file_path'],json_metadata['file_name'],dfs[0],write)
        elif transaction_type==IN_MEMORY:
            df=inMemoryDict[parents[0]['taskName']]
            writeData(json_metadata['file_type'], json_metadata['file_path'], json_metadata['file_name'], df, write)
        outputMetadata = UpdateFileOutputMetadata(json_metadata['file_type'], json_metadata['file_name'],  json_metadata['file_path'], transaction_type)
        updateTaskRunId(TaskId, runId, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
          updateTaskRunId(TaskId, runId, config["jinja_url"], "", "failed")
          raise e


def renameComponent(runId,TaskId):
    try:
        config, metadata, transaction_type,json_metadata,func_type = INIT(runId, TaskId)
        parents = metadata['parents']
        log = logical()
        columns = json_metadata["columns"]
        if transaction_type==FILE_SYSTEM:
           input=prepareInput(parents)
           if func_type == PANDAS:
               input_dfs = readInputintoPandasDF(input)
               df = log.rename(input_dfs[0], columns)
               fileName, tempPath = writeTempFile(TaskId, config, df, metadata, runId)
               outputMetadata=UpdateFileOutputMetadata(config['tempFileType'],fileName,tempPath,transaction_type)
        elif transaction_type == IN_MEMORY:
             outputMetadata = {}
             outputMetadata["transaction_type"] = transaction_type
             if func_type == PANDAS:
                 inMemoryDict[metadata['taskName']]=log.rename(inMemoryDict[parents[0]['taskName']],columns)
        updateTaskRunId(TaskId, runId, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
          print("Rename Exception ...", e)
          updateTaskRunId(TaskId, runId, config["jinja_url"], "", "failed")
          raise e

def transformComponent(runId,TaskId):
    try:
        config, metadata, transaction_type,json_metadata,func_type = INIT(runId, TaskId)
        parents = metadata['parents']
        if transaction_type==FILE_SYSTEM:
           input=prepareInput(parents)
           if func_type == PANDAS:
               input_dfs = readInputintoPandasDF(input)
               df = transform(input_dfs[parents[0]['taskName']], json_metadata)
               fileName, tempPath = writeTempFile(TaskId, config, df, metadata, runId)
               outputMetadata=UpdateFileOutputMetadata(config['tempFileType'],fileName,tempPath,transaction_type)
        elif transaction_type == IN_MEMORY:
             outputMetadata = {}
             outputMetadata["transaction_type"] = transaction_type
             if func_type == PANDAS:
                 inMemoryDict[metadata['taskName']]=transform(inMemoryDict[parents[0]['taskName']],json_metadata)
        updateTaskRunId(TaskId, runId, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
          print(e)
          updateTaskRunId(TaskId, runId, config["jinja_url"], "", "failed")
          raise e


def customPythonComponent(runId,TaskId):
    try:
        config, metadata, transaction_type,json_metadata,func_type = INIT(runId, TaskId)
        parents = metadata['parents']
        if transaction_type==FILE_SYSTEM:
           input=prepareInput(parents)
           if func_type == PANDAS:
               input_dfs = readInputintoPandasDF(input)
               df = runCustomPython(input_dfs[parents[0]['taskName']], json_metadata['fileName'],json_metadata['filePath'])
               fileName, tempPath = writeTempFile(TaskId, config, df, metadata, runId)
               outputMetadata=UpdateFileOutputMetadata(config['tempFileType'],fileName,tempPath,transaction_type)
        elif transaction_type == IN_MEMORY:
             outputMetadata = {}
             outputMetadata["transaction_type"] = transaction_type
             if func_type == PANDAS:
                 inMemoryDict[metadata['taskName']]=runCustomPython(inMemoryDict[parents[0]['taskName']],json_metadata['fileName'],json_metadata['filePath'])
        updateTaskRunId(TaskId, runId, config["jinja_url"], outputMetadata, "success")
    except Exception as e:
          print(e)
          updateTaskRunId(TaskId, runId, config["jinja_url"], "", "failed")
          raise e


def dynamic_imp(name, class_name):
    import imp
    try:
        fp, path, desc = imp.find_module(name)
    except ImportError:
        print("module not found: " + name)

    try:
        example_package = imp.load_module(name, fp,
                                          path, desc)
    except Exception as e:
        print(e)

    try:
        myclass = imp.load_module("% s.% s" % (name,
                                               class_name),
                                  fp, path, desc)
    except Exception as e:
        print(e)

    return example_package, myclass


def runCustomPython(df,name,path):
    import sys
    sys.path.insert(0,path)
    mod, modCl = dynamic_imp(name, name)
    return modCl.execute(df)
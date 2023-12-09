import xlrd
from azure.storage.blob import ContainerClient
import os
import sys
import io
 
def get_blob_clib(blobContainName):
    connection_string = "DefaultEndpointsProtocol=https;AccountName=proddataplatcn3blob01;AccountKey=ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ==;EndpointSuffix=core.chinacloudapi.cn"
 
    container = ContainerClient.from_connection_string(
        conn_str=connection_string,
        container_name=blobContainName)
    return container
 
class blob_tools():
    """
        给定container对象，以应用blob工具
    """
    def __init__(self, container):
        self.container = container
 
    def get_blobs(self, path):
        """获取指定目录下的blob

        Args:
            path (str): blob目录地址

        Returns:
            list: 该目录下所有blobs
        """
        blobs = list(self.container.list_blobs(name_starts_with=(path + '/')))
        blobs_list = []
        for i in blobs:
            i = i.name
            blobs_list.append(i)
        return blobs_list
 
    def get_path(self, blob_name,blobContainName):
        """下载blob并返回本地地址

        Args:
            blob_name (list): blob名字列表
            blobContainName (str): 容器名

        Returns:
            str: 本地下载地址
        """
        for b in blob_name:
            last_index = b.rfind('/')
            uplt = blobContainName + b[:last_index]
            blob = b[last_index + 1:]
            blobDirName = os.path.dirname(blob)
            newBlobDirName = os.path.join(uplt, blobDirName)
            if not os.path.exists(newBlobDirName):
                os.makedirs(newBlobDirName)
            localFileName = os.path.join(uplt, blob)
            downloadPath = sys.path[0].replace('\\','/') + "/" + localFileName
            print(downloadPath)
            print(localFileName)
            blob_client = container.get_blob_client(b)   
            with open(downloadPath, 'wb') as local_file:
                download = blob_client.download_blob()
                local_file.write(download.readall()) 
            return downloadPath
 
    def read_excel(self,path):
        """read excel and count rows

        Args:
            path (str): local file path

        Returns:
            int: excel rows num
        """
        workbook = xlrd.open_workbook(path)
        worksheet = workbook.sheet_by_index(0)
        total_rows = worksheet.nrows
        return total_rows
 
    def get_month(self, blobs,num):
        """read blobs list and dir_index,provide corresponding information

        Args:
            blobs (list): blob name list
            num (int): dir index

        Returns:
            list: Slice list
        """
        kehu = [a.split('/')[num] for a in blobs]
        if kehu == []:
            return 1
        else:
            return kehu
        
 

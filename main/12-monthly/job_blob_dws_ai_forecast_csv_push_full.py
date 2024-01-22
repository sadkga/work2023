from azure.storage.blob.blockblobservice import BlockBlobService
from datetime import datetime,timedelta
import os
import sys

class push_file_to_blob():
    def __init__(self, container_name, directory_path):
        self.container_name = container_name
        self.directory_path = directory_path
        self.block_blob_service = self.connect_client()

    @staticmethod
    def connect_client():
        block_blob_service =BlockBlobService(account_name='proddataplatcn3blob01'
                ,account_key='ScGueSagWl9s5XDCJeE6xOD8CupGFi5Jp0m9ZVi1Ri812p2GtXD5AXQ/zsVFIcUrNRE2zrIZlWLCjORJZyZHbQ=='
                , endpoint_suffix='core.chinacloudapi.cn')
        return block_blob_service


    def push_blob(self,file_name):
        exp = self.block_blob_service.create_blob_from_path(
                container_name=self.container_name
                ,blob_name=file_name
                ,file_path=self.directory_path + file_name)


    def get_file_push(self):
        try:
            formatted_endtime = datetime.now()
            formatted_endtime += timedelta(hours=8)
            month = formatted_endtime.strftime("%Y-%m")

            files = os.listdir(self.directory_path)
            for file in files:
                need_file = file.split('_')[-2]
                print('push_file: ',file)
                self.push_blob(file)

        except OSError as e:
            print(f"Error reading directory {path}: {e}")

if __name__ == '__main__':
    print('Start run : ',datetime.now())
    container_name = 'ads-del-demand-forecast'
    directory_path = sys.path[0]+"/AIForecast/ai_sandbox/last_version/"

    model = push_file_to_blob(container_name,directory_path)
    model.get_file_push()


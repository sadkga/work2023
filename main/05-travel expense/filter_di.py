# 导包
from notebookutils import mssparkutils  
from openpyxl import load_workbook
import pandas as pd
import os
from datetime import datetime,timedelta    
print('==================travel expense: Air Ticket=====================')


print('==================travel expense: Reimbursement=====================')
def get_last_month():
    """ 
     * @ message : 获取上月与本月月份名
     * @ return   [int] 月份
    """
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    previous_month = current_month - 1 if current_month > 1 else 12
    previous_year = current_year if current_month > 1 else current_year -1
    last_month = str(previous_year) + str(previous_month).zfill(2)
    now_month = str(current_year) + str(current_month).zfill(2)
    month_list = [last_month,now_month]
    print(month_list)
    return month_list

def data_need(blobs):
    """ 
     * @ message : 获取需要的blob
     * @ param2   [type] blobs: blobs
     * @ return   [type]
    """
    need_blobs = []
    for i in blobs:
        blob_name = i.split('_')[-1][:6]
        if blob_name not in get_last_month():
            continue
        need_blobs.append(i)
    return need_blobs

def hand_file():
    path = mssparkutils.fs.getMountPath("/mnt/bosch-dw-integration-layer")
    root = path + '/manual/AA_CTG/Reimbursement'
    files = os.listdir(root)
    files = data_need(files) # enble filter
    try:
        file_path = os.path.join(root,files[0])
        print(file_path)
        return file_path
    except:
        print('=======未识别到上月报销数据=========')
        send_to_email(receiver_email, subject, body)
        mssparkutils.notebook.exit()
file_path = hand_file()


file_path = hand_file()

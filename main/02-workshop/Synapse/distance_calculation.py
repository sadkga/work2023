import pandas as pd
from geopy.distance import geodesic

def calculate_distance(row):
    """
    计算两个经纬度坐标之间的距离

    :param coord1: 第一个坐标，格式为 (纬度, 经度)
    :param coord2: 第二个坐标，格式为 (纬度, 经度)
    :return: 距离（单位：千米）
    """
    coord1 = row['distance1']
    coord2 = row['distance2']
    
    if coord1 ==('nan',) or coord2 == ('nan',):
        distance = ''
    else:
        distance = geodesic(coord1, coord2).kilometers
    return distance


def split_to_tuple(row):
    """
    定义一个函数，将逗号分隔的字符串拆分成元组
    """
    row = str(row).split(',')
    row.reverse()
    return tuple(row)



if __name__ == "__main__":
    # TODO 1: read file
    read_branch = 'C:/Users/WZH8SGH/OneDrive - Bosch Group/code/main/02-worhop_distance/branch_address.xlsx'
    read_workshop = 'C:/Users/WZH8SGH/OneDrive - Bosch Group/code/main/02-worhop_distance/workshop_address.xlsx'
    df_branch = pd.read_excel(read_branch)
    df_workshop = pd.read_excel(read_workshop,dtype=str)

    # TODO 2：obtain_longitude_and_latitude
    df_branch['distance1'] = df_branch['latitude_and_longitude'].apply(split_to_tuple)
    df_workshop['distance2'] = df_workshop['latitude_and_longitude'].apply(split_to_tuple)
    df_branch = df_branch[['branch_name','distance1']]
    df_workshop = df_workshop[['client_name','distance2']]
    # print(df_branch)

   # TODO 3: calculate_distance
    df_merge = pd.merge(df_branch.assign(key=1),df_workshop.assign(key=1),on='key').drop('key',axis=1)
    df_merge['distance'] = df_merge.apply(calculate_distance,axis=1)
    df_result = df_merge[['client_name','branch_name','distance']]
    print(df_result)

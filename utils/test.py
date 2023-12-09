from blob_utools import blob_tools,get_blob_clib



if __name__ == '__main__':
    data_list1 = ['Dealer_forecast', 'pig/AA_BDO/Region_Turnover']
    blobContainName = 'bosch-dw-integration-layer'
    b2 = data_list1[1]
    i = '202308'

    # todo 0：connect blob
    container = get_blob_clib(blobContainName)
    model = blob_tools(container)

    
    # todo 1: read xlsx
    blobs = model.get_blobs(b2)
    month_blob = model.get_month(blobs,-2)

    print('月份：',i) 
    # dir_file = b2+'/'+i
    # dir_blob = model.get_blobs(dir_file)
    # dir_blob = [dir_blob[0]]
    # print(dir_blob)
    # path = model.get_path(dir_blob,blobContainName)
    # rows = model.read_excel(path)
    # print(path,f'Total Rows: {rows}')
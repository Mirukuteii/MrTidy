#!/usr/bin/env python 
# -*- coding: utf-8 -*-
"""
collect.py
介绍: 收集目标文件夹中的文件（照片、视频等）信息，生成DataFrame表，并保存为xlsx文件，以供下步操作。
日期: 2022-04-12
作者: MRKT
"""

# 导入python内建模块
import logging            # 日志模块
import os                 # 系统模块
import shutil             # 文件操作模块
import time               # 时间模块
import re                 # 正则模块
import json               # JSON模块
import hashlib            # HASH模块
import argparse           # 命令行参数模块

# 导入python第三方模块，需要安装exifread, pandas(包含numpy, openpyxl), hachoir
import exifread           # 照片文件EXIF解析模块
import pandas             # Pandas表格数据分析模块
import hachoir            # 影音文件元数据解析模块
from tqdm    import tqdm  # 进度条模块
from hachoir import metadata   
from hachoir import parser

# 定义媒体文件类别
_FILE_TYPE = ['image', 'video', 'other']

# 定义媒体文件扩展名类型(用于自动生成JSON文件)
_EXT_TYPE_SET = {
  "jpg": "image",
  "jpeg": "image",
  "png": "image",
  "bmp": "image",
  "gif": "image",
  "heic": "image",
  "mp4": "video",
  "mov": "video"
}

# 定义日期检验类型
_CHK_TYPE  = ['BOTH', 'META', 'EXIF', 'D_ERR', 'Y_ERR']

# 定义EXIF标签日期项序列
_EXIF_KEYS = ['EXIF DateTimeOriginal', 'EXIF DateTimeDigitized', 'Image DateTime']
_META_KEYS = ['Date-time original',    'Date-time digitized',    'Creation date' ]

# 定义JSON和XLSX文件路径
_JSON_PATH = r'ext_type_set.json'
_XLSX_PATH = r'result_df.xlsx'

# 定义XLSX字段（列名）
_COLS_SET = ['文件路径', '文件类型', '扩展名', '文件大小', 'MD5值', '日期校验', 'EXIF日期项', 'EXIF原日期', 'EXIF短日期', 'EXIF长日期', 'META日期项', 'META原日期', 'META短日期', 'META长日期']

# 初始化日志模块，建立3个日志器，2个文件日志，1个命令行日志。
_LOG_NAME       = 'Collect'
_DEBUG_LOG_PATH = r'logs\Collect_Debug.log'
_ERROR_LOG_PATH = r'logs\Collect_Error.log'
logger = logging.getLogger(_LOG_NAME)
logger.setLevel(logging.DEBUG)
fh_debug = logging.FileHandler(_DEBUG_LOG_PATH, encoding="utf-8")
fh_error = logging.FileHandler(_ERROR_LOG_PATH, encoding="utf-8")
ch       = logging.StreamHandler()
fh_debug.setLevel(logging.DEBUG)
fh_error.setLevel(logging.ERROR)
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt="[%(asctime)s] [%(levelname)s] [%(funcName)s]:\n%(message)s",
    datefmt="%Y/%m/%d %X"
    )
fh_debug.setFormatter(formatter)
fh_error.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh_debug)
logger.addHandler(fh_error)
logger.addHandler(ch)


def process_datetime(raw_datetime):
    '''
    此函数对原日期时间进行统一格式和自动修正，返回结果
    参数    raw_datetime:     原日期时间（字符串）
    返回值  datetime:         处理后的日期时间（字符串）
    '''
    datetime = re.sub(r'[\u4E00-\u9FA5A-Za-z]', '', raw_datetime)   # 去掉汉字和英文
    datetime = datetime.replace('-',':')                            # 替换-为:
    datetime = datetime.replace('/',':')                            # 替换/为:
    # datetime = datetime[:19]                                      # 保留前19个字符
    try:
        datetime = re.search(r'.*(?P<year>\d{4}):(?P<month>\d{1,2}):(?P<day>\d{1,2})\s+(?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2}).*', datetime)  # 按"year:month:day hour:minute:second"分组提取日期
    except Exception as e:
        logger.error(f'正则匹配错误:{e}')
        return None    
    # if not datetime:
    #     return None
    # print(datetime.groupdict())
    year   = int(datetime.group('year'))
    month  = int(datetime.group('month'))
    day    = int(datetime.group('day'))
    hour   = int(datetime.group('hour'))
    minute = int(datetime.group('minute'))
    second = int(datetime.group('second'))
    if year < 1900 or year > 2050:
        logger.debug(f'解析到的年:{year}错误, 解析失败')
        return None
    if month < 1 or month > 12:
        logger.debug(f'解析到的月:{month}错误, 解析失败')
        return None
    if day < 1 or day > 31:
        logger.debug(f'解析到的日:{day}错误, 解析失败')
        return None
    if hour < 0 or hour > 23:
        logger.debug(f'解析到的时:{hour}错误, 自动修复')
        hour = hour % 24 
    if minute < 0 or minute > 59:
        logger.debug(f'解析到的分:{minute}错误, 自动修复')
        minute = minute % 60
    if second < 0 or second > 59:
        logger.debug(f'解析到的秒:{second}错误, 自动修复')
        second = second % 60
    datetime = f"{year}:{month:02}:{day:02} {hour:02}:{minute:02}:{second:02}"
    return datetime


def get_exif_datetime(file, tag_keys=_EXIF_KEYS):
    '''
    此函数解析媒体文件的EXIF标签，返回解析结果
    参数    file:      需要解析的媒体文件
    参数    tag_keys:  需要提取的EXIF标签项序列
    返回值  msg:       解析结果，字典格式
    '''
    msg = {}               # 初始化msg
    msg['stat']  = '1'     # 解析状态 0:成功 1:无法解析文件 2:无EXIF标签 3:EXIF标签无日期项 4:日期项数据错误
    msg['type']  = 'EXIF'  # 解析类型
    msg['key']   = None    # 提取的标签项字段名
    msg['raw']   = None    # 提取到的原日期时间，如2022-04-01 15:25:38 下午
    msg['short'] = None    # 处理后的短日期时间，如2022/04
    msg['long']  = None    # 处理后的长日期时间，如20220401_152538
    try:
        logger.debug(f'开始解析EXIF标签:{file}')
        f = open(file, 'rb')
        exif_tags = exifread.process_file(f)
    except Exception as e:
        logger.error(f'解析EXIF失败，文件无法解析:{e}')
        return msg
    # 注意:exif_tags是字典
    if exif_tags:
        # logger.debug(f"解析到EXIF标签项:{exif_tags.keys()}")
        datetime_key = None
        for i in range(len(tag_keys)):
            if tag_keys[i] in exif_tags:
                datetime_key = tag_keys[i]
                break
        if datetime_key:
            raw_datetime = exif_tags.get(datetime_key).values
            logger.debug(f'EXIF初值: {raw_datetime}')
            pro_datetime = process_datetime(raw_datetime)
            if pro_datetime:
                datetime = time.strptime(pro_datetime, '%Y:%m:%d %H:%M:%S')      # 通过time模块二次检验和过滤格式
                logger.debug(f'EXIF中值: {datetime}')
                msg['stat']  = '0'
                msg['key']   = datetime_key
                msg['raw']   = raw_datetime
                msg['short'] = time.strftime('%Y/%m', datetime)
                msg['long']  = time.strftime('%Y%m%d_%H%M%S', datetime)
                logger.debug(f"EXIF终值: {msg['short']},{msg['long']}")
                return msg
            msg['stat'] = '4'
            logger.debug(f'解析EXIF失败，EXIF标签中的日期有错误')
            return msg
        msg['stat'] = '3'
        logger.debug(f'解析EXIF失败，EXIF标签中提取不到日期项')
        return msg
    msg['stat'] = '2'
    logger.debug(f'解析EXIF失败，未解析到EXIF标签')
    return msg


def get_meta_datetime(file, tag_keys=_META_KEYS):
    """
    此函数解析媒体文件的META标签，返回解析结果
    参数    file:      需要解析的媒体文件
    参数    tag_keys:  需要提取的META标签项序列
    返回值  msg:       解析结果，字典格式
    """
    msg = {}               # 初始化msg
    msg['stat']  = '1'     # 解析状态 0:成功 1:无法解析文件 2:无META标签 3:META标签无日期项 4:日期项数据错误
    msg['type']  = 'META'  # 解析类型
    msg['key']   = None    # 提取的标签项字段名
    msg['raw']   = None    # 提取到的原日期时间，如2022-04-01 15:25:38 下午
    msg['short'] = None    # 处理后的短日期时间，如2022/04
    msg['long']  = None    # 处理后的长日期时间，如20220401_152538
    try:
        logger.debug(f'开始解析META:{file}')
        f = parser.createParser(file)
        meta_decode = metadata.extractMetadata(f)
        meta_tags = meta_decode.exportPlaintext(line_prefix="") # 将文件的metadata转换为list,且将前缀设置为空
    except Exception as e:
        logger.error(f'解析META失败，文件无法解析:{e}')
        return msg
    # 注意:meta_tags是列表
    if meta_tags:
        # logger.debug(f"解析到META标签:{meta_tags}")
        datetime_key = None
        key_tag = None
        for i in range(len(tag_keys)):
            for tag in meta_tags:
                if tag.startswith(tag_keys[i]):
                    datetime_key = tag_keys[i]
                    key_tag = tag
                    break
            else:
                continue
            break
        if datetime_key and key_tag:
            raw_datetime = key_tag.replace(datetime_key+': ','')
            logger.debug(f'META初值:{raw_datetime}')
            pro_datetime = process_datetime(raw_datetime)
            if pro_datetime:
                datetime = time.strptime(pro_datetime, '%Y:%m:%d %H:%M:%S')      # 通过time模块二次检验和过滤格式
                logger.debug(f'META中值: {datetime}')
                msg['stat']  = '0'
                msg['key']   = datetime_key
                msg['raw']   = raw_datetime
                msg['short'] = time.strftime('%Y/%m', datetime)
                msg['long']  = time.strftime('%Y%m%d_%H%M%S', datetime)
                logger.debug(f"META终值: {msg['short']},{msg['long']}")
                return msg
            msg['stat'] = '4'
            logger.debug(f'解析META失败，META标签中的日期有错误')
            return msg
        msg['stat'] = '3'
        logger.debug(f'解析META失败，META标签中提取不到日期项')
        return msg
    msg['stat'] = '2'
    logger.debug(f'解析META失败，未解析到META标签')
    return msg


def get_datetime(file, begin_year=2000):
    """
    此函数读取文件的EXIF和META信息，返回解析结果msg
    参数    file:         需要解析解析文件
    返回值  msg:          解析的信息结果，字典格式
    """
    msg = {}                # 初始化msg
    msg['stat']   = '1'     # 状态 0:成功 1:失败
    msg['check']  = None    # 验证 BOTH:两者一致 EXIF|META:唯一 D_ERR:两者不一致 Y_ERR:年份超常
    msg['EXIF_K'] = None    # EXIF日期来源KEY
    msg['EXIF_R'] = None    # EXIF处理前的原日期
    msg['EXIF_S'] = None    # EXIF处理后的短日期
    msg['EXIF_L'] = None    # EXIF处理后的长日期
    msg['META_K'] = None    # META日期来源KEY
    msg['META_R'] = None    # META处理前的原日期
    msg['META_S'] = None    # META处理后的短日期
    msg['META_L'] = None    # META处理后的长日期
    logger.info(f'开始解析{file}')
    exif_dt = get_exif_datetime(file)
    meta_dt = get_meta_datetime(file)
    # 优先采用exif_dt信息
    if exif_dt['stat'] == '0':
        logger.debug(f'解析到文件{file}的EXIF日期信息{exif_dt}')
        msg['stat']   = '0'
        msg['EXIF_K'] = exif_dt['key']
        msg['EXIF_R'] = exif_dt['raw']
        msg['EXIF_S'] = exif_dt['short']
        msg['EXIF_L'] = exif_dt['long']
        the_year = int(msg['EXIF_S'].split('/')[0])
    if meta_dt['stat'] == '0':
        logger.debug(f'解析到文件{file}的META日期信息{meta_dt}')
        msg['stat']   = '0'
        msg['META_K'] = meta_dt['key']
        msg['META_R'] = meta_dt['raw']
        msg['META_S'] = meta_dt['short']
        msg['META_L'] = meta_dt['long']
        the_year = int(msg['META_S'].split('/')[0])
    if msg['stat']  == '0':
        if msg['EXIF_L'] and msg['META_L']:
            if msg['EXIF_L'] == msg['META_L']:
                msg['check'] = 'BOTH'
            else:
                msg['check'] = 'D_ERR'
        else:
            if msg['EXIF_L']:
                msg['check'] = 'EXIF'
            else:
                msg['check'] = 'META'
        if msg['check'] in ['BOTH', 'EXIF', 'META']:
            if the_year < begin_year or the_year > time.localtime().tm_year:
                msg['check'] = 'Y_ERR'
    else:
        logger.debug(f'文件:{file}无日期信息')
    logger.info(f'解析结果:{msg}')
    return msg


def get_file_size(file, unit='KB'):
    """
    此函数获取文件的大小
    参数:    file:         需要解析解析文件
    返回值:               文件大小，默认单位KB，保留小数点后2位
    """
    if not os.path.isfile(file):
        logger.error(f'文件不存在, 无法获取大小:{file}')
        return None
    f_size = os.path.getsize(file)
    f_KB   = round(f_size/float(1024), 2)
    f_MB   = round(f_size/float(1024**2),2)
    if unit == 'MB':
        return f_MB
    return f_KB


def get_file_md5(file):
    """
    此函数获取文件的MD5，返回hash值
    参数    file:         需要解析解析文件
    """
    if not os.path.isfile(file):
        logger.error(f'文件不存在, 无法获取HASH:{file}')
        return None
    f_md5 = hashlib.md5()
    try:
        f = open(file, 'rb')
        while True:
            b = f.read(8096)
            if not b:
                break
            f_md5.update(b)
        f.close()
    except Exception as e:
        logger.error('%s', e)
    return f_md5.hexdigest()


def process_files(root, files):
    '''
    该函数对文件夹内的所有文件进行解析，返回解析结果result
    参数    root:     需要解析的文件夹路径
    参数    files:    该文件夹下的所有文件
    返回值  result:   所有文件的解析的结果，二维列表格式
    '''
    result = []
    for file in files:
        file_path = os.path.join(root, file)
        file_ext = os.path.splitext(file_path)[1][1:].lower()
        file_dext = '.' + file_ext
        # 验证文件分类
        if file_ext not in ext_type_set.keys():
            file_type = input(f"{file_ext}属于何种类型:")
            while file_type not in _FILE_TYPE:
                print(f"请输入image或video或other")
                file_type = input(f"{file_ext}属于何种类型:")
            ext_type_set[file_ext] = file_type
            logger.debug(f'添加扩展名{file_ext}到{file_type}类型')
        file_type = ext_type_set[file_ext]
        file_size = get_file_size(file_path)
        file_md5  = get_file_md5(file_path)
        dt_stat       = None
        dt_check      = None
        dt_exif_raw   = None
        dt_exif_long  = None
        dt_exif_short = None
        dt_meta_raw   = None
        dt_meta_long  = None
        dt_meta_short = None
        if file_type == 'image' or file_type == 'video' :
            file_dtl = get_datetime(file_path)
            # logger.debug(f'文件:{file}已解析,结果为{file_dtl}')
            if file_dtl:
                dt_stat       = file_dtl['stat']
                dt_check      = file_dtl['check']
                dt_exif_key   = file_dtl['EXIF_K']
                dt_exif_raw   = file_dtl['EXIF_R']
                dt_exif_long  = file_dtl['EXIF_L']
                dt_exif_short = file_dtl['EXIF_S']
                dt_meta_key   = file_dtl['META_K']
                dt_meta_raw   = file_dtl['META_R']
                dt_meta_long  = file_dtl['META_L']
                dt_meta_short = file_dtl['META_S']
        file_datalist = [file_path, file_type, file_ext, file_size, file_md5, dt_check, dt_exif_key, dt_exif_raw, dt_exif_short, dt_exif_long, dt_meta_key, dt_meta_raw, dt_meta_short, dt_meta_long]
        result.append(file_datalist)
    return result


def count_nums(file_dataframe):
    record = {}
    record['total'] = len(file_dataframe)
    sp = ''
    print('\n')
    print(     '┌─────────────────────────[解析结果]────────────────────────────┐')
    print(    f"│{sp:<15}共解析文件记录..................{record['total']:>6}条{sp:<8}│")
    print(     '├──────────────────────────文件类型─────────────────────────────┤')
    for file_type in _FILE_TYPE:
        record[file_type] = list(file_dataframe['文件类型']).count(file_type)
        print(f"│{sp:<15}{file_type:<10}文件记录..............{record[file_type]:>6}条{sp:<8}│")
    print(     '├──────────────────────────扩展类型─────────────────────────────┤')
    for ext in ext_type_set.keys():
        record[ext]       = list(file_dataframe['扩展名']).count(ext)
        print(f"│{sp:<15}{ext:<10}文件记录..............{record[ext]:>6}条{sp:<8}│")
    print(     '├──────────────────────────日期解析─────────────────────────────┤')
    for info_type in _CHK_TYPE:
        record[info_type] = list(file_dataframe['日期校验']).count(info_type)
        print(f"│{sp:<15}{info_type:<10}文件记录..............{record[info_type]:>6}条{sp:<8}│")
    print(     '├──────────────────────────EXIF标签─────────────────────────────┤')
    for exif_key in _EXIF_KEYS:
        record[exif_key] = list(file_dataframe['EXIF日期项']).count(exif_key)
        print(f"│{sp:<2}{exif_key:<23}文件记录..............{record[exif_key]:>6}条{sp:<8}│")
    print(     '├──────────────────────────META标签─────────────────────────────┤')
    for meta_key in _META_KEYS:
        record[meta_key] = list(file_dataframe['META日期项']).count(meta_key)
        print(f"│{sp:<2}{meta_key:<23}文件记录..............{record[meta_key]:>6}条{sp:<8}│")
    print(     '└───────────────────────────────────────────────────────────────┘')
    return record


def main(process_directory):
    '''
    主函数
    参数    process_directory:     需要解析的文件夹路径
    '''
    curdir = os.path.abspath(os.curdir)
    files_total = 0
    for root, dirs, files in os.walk(process_directory):
        for each in files:
            files_total += 1
    logger.info(f'共发现待解析文件:{files_total}个')

    pbar = tqdm(total=files_total,ncols=80)
    files_datalist = []
    for root, dirs, files in os.walk(process_directory):
        files_updatelist = process_files(root, files)
        files_datalist += files_updatelist 
        files_num = len(files_datalist)
        files_per = files_num / files_total
        logger.info(f'已完成解析目录:{root}')
        logger.info(f'已完成解析文件:{files_num}/{files_total}, 完成率:{files_per:.2%}')
        pbar.update(len(files_updatelist))
    pbar.close()


    os.chdir(curdir)
    file_dataframe = pandas.DataFrame(files_datalist, columns=_COLS_SET)
    logger.info('开始写入XLSX文件...')
    file_dataframe.to_excel(_XLSX_PATH, sheet_name='文件解析结果')
    logger.info('写入XLSX文件完毕')
    count_nums(file_dataframe)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-d", "--dir", help="请填写准备解析的文件目录路径")
    args = arg_parser.parse_args()

    print('-------------欢迎使用本程序-------------')

    if os.path.exists(_JSON_PATH):
        logger.debug(f'正在导入JSON文件:{_JSON_PATH}')
        try:
            with open(_JSON_PATH, 'r') as f:
                ext_type_set = json.load(f)
        except Exception as e:
            logger.error(f'导入文件失败:{e}')
            sys.exit()
        logger.info('导入JSON文件完毕.')
    else:
        logger.debug(f'JSON文件:{_JSON_PATH}不存在，自动创建中')
        try:
            with open(_JSON_PATH, 'w') as f:
                json.dump(_EXT_TYPE_SET, f, indent=2)
        except Exception as e:
            logger.error(f'文件写入失败:{e}')
            sys.exit()
        ext_type_set = _EXT_TYPE_SET
        logger.info(f'自动生成JSON文件{_JSON_PATH}完毕.')

    main(args.dir)

    logger.debug(f'正在保存JSON文件:{_JSON_PATH}')
    try:
        with open(_JSON_PATH, 'w') as f:
            json.dump(ext_type_set, f, indent=2)
    except Exception as e:
        logger.error(f'文件写入失败:{e}')
        sys.exit()
    logger.info('保存JSON文件完毕.')

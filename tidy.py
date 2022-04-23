#!/usr/bin/env python 
# -*- coding: utf-8 -*-
"""
tidy.py
介绍: 读取保存有照片等信息的xlsx文件，经处理后重新整理归档。
日期: 2022-04-12
作者: MRKT
"""

# 导入python内建模块
import logging            # 日志模块
import os                 # 系统模块
import sys                # 系统模块
import shutil             # 文件操作模块
import time               # 时间模块
import re                 # 正则模块
import json               # JSON模块
import argparse           # 命令行参数模块

# 导入python第三方模块
import pandas             # Pandas表格数据分析模块
from tqdm import tqdm     # 进度条模块

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
_JSON_PATH   = r'ext_type_set.json'
_XLSX_PATH   = r'result_df.xlsx'
_D_XLSX_PATH = r'duplicated_df.xlsx'
_Y_XLSX_PATH = r'year_err_df.xlsx'
_F_XLSX_PATH = r'failed_files_df.xlsx'

# 定义XLSX字段（列首）
_COLS_SET   = ['文件路径', '文件类型', '扩展名', '文件大小', 'MD5值', '日期校验', 'EXIF日期项', 'EXIF原日期', 'EXIF短日期', 'EXIF长日期', 'META日期项', 'META原日期', 'META短日期', 'META长日期']
_Y_COLS_SET = ['文件原始路径', '文件当前路径', '文件大小', 'EXIF日期项', 'EXIF长日期', 'META日期项', 'META长日期']
_F_COLS_SET = ['文件原始路径', '文件目标路径', '文件大小', '整理方式', '整理失败原因']

# 定义XLSX去重匹配字段（列首）
_D_COLS_SET = ['扩展名', '文件大小', 'MD5值']

# 定义分类目录字典
_FILE_CATEGORY = {}
_FILE_CATEGORY['image_with_datetime']    = 'Photos_bydt'  
_FILE_CATEGORY['image_without_datetime'] = 'Photos_nodt'
_FILE_CATEGORY['video_with_datetime']    = 'Photos_bydt'   # 这里有日期的视频文件和有日期的照片文件放一起，也可设置不同的目录
_FILE_CATEGORY['video_without_datetime'] = 'Videos_nodt'
_FILE_CATEGORY['year_err']               = 'Year_err'
_FILE_CATEGORY['other']                  = 'Other_files'

# 定义需要生成年月子目录的目录列表(按with_datetime的分类目录自动定义)
_DATE_DIRS = []
for key in _FILE_CATEGORY.keys():
    if key.endswith('with_datetime'):
        t_dir=_FILE_CATEGORY[key]
        if not t_dir in _DATE_DIRS:
            _DATE_DIRS.append(t_dir)

# 初始化日志模块，建立3个日志器，2个文件日志，1个命令行日志。
_LOG_NAME       = 'Tidy'
_DEBUG_LOG_PATH = r'logs\Tidy_Debug.log'
_ERROR_LOG_PATH = r'logs\Tidy_Error.log'
logger = logging.getLogger(_LOG_NAME)
logger.setLevel(logging.DEBUG)
fh_debug = logging.FileHandler(_DEBUG_LOG_PATH, encoding="utf-8")
fh_error = logging.FileHandler(_ERROR_LOG_PATH, encoding="utf-8")
ch       = logging.StreamHandler()
fh_debug.setLevel(logging.DEBUG)
fh_error.setLevel(logging.ERROR)
ch.setLevel(logging.ERROR)
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


def mk_category_dirs(file_category):
    '''
    该函数创建空的分类目录
    参数    file_category:   已定义好的分类目录字典
    '''
    for category in file_category.values():
        directory = os.path.join(*category.split('/'))
        if not os.path.exists(directory):
            os.mkdir(directory)
            logger.debug(f'目录{directory}已创建')
        else:
            logger.debug(f'目录{directory}已存在')
    logger.info(f'分类目录创建完毕')


def mk_date_dirs(date_dirs, begin_year=2000):
    '''
    该函数创建空的年月目录
    参数   date_dirs:   需要创建年份子目录的目录列表
    参数   begin_year:  初始年份，默认为2000年
    '''
    for date_dir in date_dirs:
        date_directory = os.path.join(*date_dir.split('/'))
        for i in range(begin_year, time.localtime().tm_year + 1):
            directory = os.path.join(date_directory, f'{i}')
            if not os.path.exists(directory):
                os.mkdir(directory)
                logger.info(f'年目录{directory}已创建')
            else:
                logger.info(f'年目录{directory}已存在')
            for j in range(1, 13):
                directory = os.path.join(date_directory, f'{i}', f'{j:02}')
                if not os.path.exists(directory):
                    os.mkdir(directory)
                    logger.debug(f'月目录{directory}已创建')
                else:
                    logger.debug(f'月目录{directory}已存在')
            logger.debug(f'{i}年全年目录已创建')
        logger.debug(f'{date_dir}下的年月子目录已创建')
    logger.info(f'所有年月子目录已创建')


def rm_date_dirs(date_dirs, begin_year=2000):
    '''
    该函数删除空的年月目录
    参数   date_dirs:   需要删除年份子目录的目录列表
    参数   begin_year:  初始年份，默认为2000年
    '''
    print('正在删除空白年月目录')
    # os.removedirs区别
    for date_dir in date_dirs:
        date_directory = os.path.join(*date_dir.split('/'))
        for i in range(begin_year, time.localtime().tm_year + 1):
            for j in range(1, 13):
                directory = os.path.join(date_dir, f'{i}', f'{j:02}')
                if not os.listdir(directory):
                    os.rmdir(directory)
                    logger.debug(f'空的月目录{directory}已删除')
            logger.debug(f'{i}年空的月目录已删除')
        logger.debug(f'所有空的月目录已删除')
        for i in range(begin_year, time.localtime().tm_year + 1):
            directory = os.path.join(date_dir, f'{i}')
            if not os.listdir(directory):
                os.rmdir(directory)
                logger.debug(f'空的年目录{directory}已删除')
        logger.debug(f'{date_dir}下空的年目录已删除')
    logger.info(f'所有空的年月子目录已删除')


def chk_cols(file_dataframe, cols_set):
    if list(file_dataframe.columns)[1:] == cols_set:
        return True
    else:
        logger.error(f'文件格式不匹配, 列索引错误:\n{list(file_dataframe.columns)}不符合{cols_set}')
        return False 


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


def chk_duplicate(file_dataframe, duplicated_keys=_D_COLS_SET):
    '''
    该函数负责查找和去除重复文件
    参数    file_dataframe:  含有文件信息的DataFrame表, 列首为_COLS_SET
    参数    duplicated_keys: 需要匹配的去重列内容, 默认为_D_COLS_SET
    返回值: 
    '''
    d_df = file_dataframe[file_dataframe.duplicated(duplicated_keys, keep=False)]
    d_df = d_df.loc[:, ~d_df.columns.str.contains('^Unnamed')]
    print(d_df)
    d_df.to_excel(_D_XLSX_PATH, sheet_name='重名文件')
    print(f'以上电子表格已导出，详见{_D_XLSX_PATH}')
    sel = input("请确认是否去除重复文件(方法:保留重复的前项，删除后项):(YES/NO/EXIT):")
    while sel not in ['YES', 'NO', 'EXIT']:
        sel = input("输入不正确，请确认是否删除以上重复项:(YES/NO/EXIT):")
    if  sel == 'YES':
        logger.info('用户确认去除重复文件')
        return file_dataframe.drop_duplicates(duplicated_keys)
    elif sel == 'NO':
        logger.info('用户确认保留重复文件')
        return file_dataframe
    else:
        logger.info('用户在确认去重时选择退出')
        sys.exit()
    

def pick_num(fname):
    '''
    该函数选取文件名中的首个数字到末尾数字的之间的部分，保留单独的下划线信息
    参数    fname:           需要挑选的文件名
    '''
    result = re.sub(r'[\u4E00-\u9FA5A-Za-z]', '', fname) #去掉汉字和英文
    result = re.sub(r'^_+', '', result)                  #去掉前边所有的下划线
    result = re.sub(r'_+$', '', result)                  #去掉后边所有的下划线
    result = re.sub(r'___*', '_', result)                #去掉中间多余的下划线
    return result


def move_file(input_path, output_path, remove=False):
    '''
    该函数负责移动或复制文件
    参数    input_path:      原文件路径
    参数    output_path:     目标文件路径
    参数    remove:          移动或复制选项，默认复制
    返回值  reason           操作成功为None
    '''
    if not input_path:
        logger.error(f'文件原始路径不存在:{input_path}')
        reason = f'文件原始路径不存在:{input_path}'
        return reason
    output_dir = os.path.split(output_path)[0]
    if not os.path.exists(output_dir) and output_dir :
        logger.error(f'文件目标路径目录不存在:{output_dir}')
        reason = f'文件目标路径目录不存在:{output_dir}'
        return reason
    if remove:
        move_mod = '移动文件'
        try:
            shutil.move(input_path, output_path)
        except Exception as e:
            logger.error(f'移动文件错误:{e}')
            reason = f'移动文件错误:{e}'
            return reason
    else:
        move_mod = '复制文件'
        try:
            shutil.copyfile(input_path, output_path)
        except Exception as e:
            logger.error(f'复制文件错误:{e}')
            reason = f'复制文件错误:{e}'
            return reason
    logger.info(f'[{move_mod}]{input_path}至{output_path}')
    return None

# ['文件路径', '文件类型', '扩展名', '文件大小', 'MD5值', '日期校验', 'EXIF日期项', 'EXIF原日期', 'EXIF短日期', 'EXIF长日期', 'META日期项', 'META原日期', 'META短日期', 'META长日期']
# 信息类型不为空，则文件有datetime信息
# 对于有datetime信息的媒体文件， 保存到指定目录的:短日期/IMG_长日期_信息类型_00001.扩展名，如:2017/03/IMG_20170313_181520_EXIF_00001.ext
# 对于没有datetime信息的媒体文件，保存到指定目录下的IMG_(原文件名中首个数字到最后一个数字的部分)_NODT_00001.ext
# 对于文件类型为other的，保存到指定目录的原文件名_00001.ext
def reorgnize_file(file_dataframe, remove=False):
    '''
    该函数根据DataFrame表进行文件整理归档
    参数    file_dataframe:   含有文件信息的DataFrame表, 列首为_COLS_SET
    参数    remove:           移动或复制选项，默认复制
    '''
    success = 0
    fail    = 0
    confi =None   # 媒体文件日期置信度
    yerr_files   = []
    failed_files = []
    files_total = len(file_dataframe)
    logger.debug(f'共需要整理{files_total}个文件...')
    print(f'开始整理文件，共需整理{files_total}个')
    pbar = tqdm(total=files_total,ncols=80)
    for file in file_dataframe.itertuples():
        source_path = file.文件路径
        file_basename = os.path.basename(source_path)
        file_name     = os.path.splitext(file_basename)[0]
        if file.日期校验 in ['BOTH', 'D_ERR', 'META', 'EXIF']:
            target_key  = file.文件类型 + '_with_datetime'
            if file.日期校验 == 'EXIF':
                if   file.EXIF日期项 == _EXIF_KEYS[0]:
                    confi = 'EXIF_U'
                elif file.EXIF日期项 == _EXIF_KEYS[1]:
                    confi = 'EXIF_H'
                elif file.EXIF日期项 == _EXIF_KEYS[2]:
                    confi = 'EXIF_N'
                else:
                    logger.error(f'{file}的EXIF日期项错误')
                target_dir  = os.path.join(*_FILE_CATEGORY[target_key].split('/'), *file.EXIF短日期.split('/'))
                target_path = os.path.join(target_dir, f'IMG_{file.EXIF长日期}_{confi}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
            if file.日期校验 == 'META':
                if   file.META日期项 == _META_KEYS[0]:
                    confi = 'META_U'
                elif file.META日期项 == _META_KEYS[1]:
                    confi = 'META_H'
                elif file.META日期项 == _META_KEYS[2]:
                    confi = 'META_N'
                else:
                    logger.error(f'{file}的META日期项错误')
                target_dir  = os.path.join(*_FILE_CATEGORY[target_key].split('/'), *file.META短日期.split('/'))
                target_path = os.path.join(target_dir, f'IMG_{file.META长日期}_{confi}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
            if file.日期校验 == 'BOTH':
                if   file.EXIF日期项 == _EXIF_KEYS[0] or file.META日期项 == _META_KEYS[0]:
                    confi = 'BOTH_U'
                elif file.EXIF日期项 == _EXIF_KEYS[1] or file.META日期项 == _META_KEYS[1]:
                    confi = 'BOTH_H'
                elif file.EXIF日期项 == _EXIF_KEYS[2] or file.META日期项 == _META_KEYS[2]:
                    confi = 'BOTH_N'
                else:
                    logger.error(f'{file}的日期项错误')
                target_dir  = os.path.join(*_FILE_CATEGORY[target_key].split('/'), *file.EXIF短日期.split('/'))
                target_path = os.path.join(target_dir, f'IMG_{file.EXIF长日期}_{confi}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
            if file.日期校验 == 'D_ERR':
                chose = None
                if   file.EXIF日期项 == _EXIF_KEYS[0] or file.META日期项 == _META_KEYS[0]:
                    if file.EXIF日期项 == _EXIF_KEYS[0]:
                        chose = 'EXIF'
                    else:
                        chose = 'META'
                    confi = 'U'
                elif file.EXIF日期项 == _EXIF_KEYS[1] or file.META日期项 == _META_KEYS[1]:
                    if file.EXIF日期项 == _EXIF_KEYS[1]:
                        chose = 'EXIF'
                    else:
                        chose = 'META'
                    confi = 'H'
                elif file.EXIF日期项 == _EXIF_KEYS[2] or file.META日期项 == _META_KEYS[2]:
                    if file.EXIF日期项 == _EXIF_KEYS[2]:
                        chose = 'EXIF'
                    else:
                        chose = 'META'
                    confi = 'N'
                else:
                    logger.error(f'{file}的日期项错误')
                confi = 'D_' + chose + '_' + confi
                if   chose == 'EXIF':
                    target_dir  = os.path.join(*_FILE_CATEGORY[target_key].split('/'), *file.EXIF短日期.split('/'))
                    target_path = os.path.join(target_dir, f'IMG_{file.EXIF长日期}_{confi}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
                elif chose == 'META':
                    target_dir  = os.path.join(*_FILE_CATEGORY[target_key].split('/'), *file.META短日期.split('/'))
                    target_path = os.path.join(target_dir, f'IMG_{file.META长日期}_{confi}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
                else:
                    logger.error(f'错误')
        elif file.日期校验 == 'Y_ERR':
            target_dir  = os.path.join(*_FILE_CATEGORY['year_err'].split('/'))
            target_path = os.path.join(target_dir, f'{file_name}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
            yerr_file = [source_path, target_path, file.文件大小, file.EXIF日期项, file.EXIF长日期, file.META日期项, file.META长日期]
            yerr_files.append(yerr_file)
        elif not file.文件类型 == 'other':
            target_key  = file.文件类型 + '_without_datetime'
            target_dir  = os.path.join(*_FILE_CATEGORY[target_key].split('/'))
            file_num    = pick_num(file_name)
            target_path = os.path.join(target_dir, f'IMG_{file_num}_NODT_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
        else:
            target_dir  = os.path.join(*_FILE_CATEGORY['other'].split('/'))
            target_path = os.path.join(target_dir, f'{file_name}_{len(os.listdir(target_dir))+1:05}.{file.扩展名}')
        failed_reason = move_file(source_path, target_path, remove)
        if not failed_reason:
            success += 1
        else:
            fail    += 1
            failed_file  = [source_path, target_path, remove, file.文件大小, failed_reason]
            failed_files.append(failed_file)
        pbar.update(1)
    print(f'整理完毕，成功整理文件{success}个, 失败{fail}个，失败文件信息和原因稍后见{_F_XLSX_PATH}')
    if failed_files:
        f_df = pandas.DataFrame(failed_files, columns=_F_COLS_SET)
        logger.info('开始写入XLSX文件{_F_XLSX_PATH}...')
        f_df.to_excel(_F_XLSX_PATH, sheet_name='整理失败的文件')
        logger.info('写入XLSX文件完毕')
    if yerr_files:
        y_df = pandas.DataFrame(yerr_files, columns=_Y_COLS_SET)
        logger.info('开始写入XLSX文件{_Y_XLSX_PATH}...')
        y_path = os.path.join(*_FILE_CATEGORY['year_err'].split('/'), *_Y_XLSX_PATH.split('/'))
        y_df.to_excel(y_path, sheet_name='年份明显错误的文件')
        logger.info('写入XLSX文件完毕')
    pbar.close()

def main(target_dir, remove=False):
    '''
    主函数
    参数    target_dir:       整理归档的目标路径
    参数    remove:           移动或复制选项，默认复制
    '''
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    if not os.path.exists(_XLSX_PATH):
        logger.error('xlsx文件不存在,退出程序')
        sys.exit()
    logger.debug(f'开始读取XLSX文件')
    file_dataframe  = pandas.read_excel(_XLSX_PATH)

    if not chk_cols(file_dataframe, _COLS_SET):
        sys.exit()
    count_nums(file_dataframe)
    chked_dataframe = chk_duplicate(file_dataframe)
    curdir = os.path.abspath(os.curdir)
    os.chdir(target_dir)
    mk_category_dirs(_FILE_CATEGORY)
    mk_date_dirs(_DATE_DIRS)
    reorgnize_file(chked_dataframe, remove=False)
    rm_date_dirs(_DATE_DIRS)
    os.chdir(curdir)



if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-o", "--out", help="准备归档的文件目录路径")
    arg_parser.add_argument("-remove", "--remove", help="是否删除原文件", action="store_true")
    args = arg_parser.parse_args()

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
        logger.debug(f'JSON文件:{_JSON_PATH}不存在，使用默认配置')
        ext_type_set = _EXT_TYPE_SET
        logger.info(f'已应用默认文件类型配置.')

    main(args.out, args.remove)






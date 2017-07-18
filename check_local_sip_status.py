#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
@author: copy&upate by lintf
@summary: ͨ通过SqlPlus查询Oracles数据库
'''

import os
import sys
from datetime import datetime
import codecs
import ConfigParser
import logging

#其中os.environ['NLS_LANG']的值来自 select userenv('language') from dual;
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
gStrConnection = 'sipivr3/sipivr3@entpbx'

log_file='/home/entpbx/tools/check_sip.log'
cfg_file='/home/entpbx/sip_check_cfg.ini'

#解析SqlPlus的查询结果，返回列表
def parseQueryResult(listQueryResult):
  listResult = []
  #如果少于4行，说明查询结果为空
  if len(listQueryResult) < 4:
    return listResult
  #第0行是空行，第1行可以获取字段名称，第2行可获取SQLPlus原始结果中每列宽度，第3行开始是真正输出
  # 1 解析第2行，取得每列宽度，放在列表中
  listStrTmp = listQueryResult[2].split(' ')
  listIntWidth = []
  for oneStr in listStrTmp:
    listIntWidth.append(len(oneStr))
  # 2 解析第1行，取得字段名称放在列表中
  listStrFieldName = []
  iLastIndex = 0
  lineFieldNames = listQueryResult[1]
  for iWidth in listIntWidth:
    #截取[iLastIndex, iLastIndex+iWidth)之间的字符串
    strFieldName = lineFieldNames[iLastIndex:iLastIndex + iWidth]
    strFieldName = strFieldName.strip() #去除两端空白符
    listStrFieldName.append(strFieldName)
    iLastIndex = iLastIndex + iWidth + 1
  # 3 第3行开始，解析结果，并建立映射，存储到列表中
  for i in range(3, len(listQueryResult)):
    oneLiseResult = unicode(listQueryResult[i], 'UTF-8')
    fieldMap = {}
    iLastIndex = 0
    for j in range(len(listIntWidth)):
      strFieldValue = oneLiseResult[iLastIndex:iLastIndex + listIntWidth[j]]
      strFieldValue = strFieldValue.strip()
      fieldMap[listStrFieldName[j]] = strFieldValue
      iLastIndex = iLastIndex + listIntWidth[j] + 1
    listResult.append(fieldMap)
  return listResult

def QueryBySqlPlus(sqlCommand):
  global gStrConnection
  #构造查询命令
  strCommand = 'sqlplus -S %s <<!\n' % gStrConnection
  #strCommand = 'sqlplus -S %s <<' %gStrConnection + EOF + '\n' 
  strCommand = strCommand + 'set linesize 400\n'
  strCommand = strCommand + 'set pagesize 100\n'
  strCommand = strCommand + 'set term off verify off feedback off tab off \n'
  strCommand = strCommand + 'set numwidth 40\n'
  strCommand = strCommand + sqlCommand + '\n'
  # 调用系统命令收集结果
  try:
    result = os.popen(strCommand)
    logging.info(result.readline())
  except Exception, e:
    logging.warning(e.message)

  #result = os.popen(strCommand)
  #print result

  list = []
  for line in result:
    list.append(line)
  return parseQueryResult(list)


try:

  #logging.info('this is a loggging info message')
  #logging.debug('this is a loggging debug message')
  #logging.warning('this is loggging a warning message')
  #logging.error('this is an loggging error message')
  #logging.critical('this is a loggging critical message')

  if os.path.exists(log_file):
    # 判断日志文件大小, 如果超过50m, 备份目录并重新创建文件
    #with codecs.open('/home/entpbx/check_sip.log', 'w', 'utf-8') as file_cdr:
    #  log_file_header = "===[INFO] log file exists and less than 50M，skip rm logfile" + datetime.now().strftime(
    #    '%Y-%m-%d %H:%M:%S') + "===\r\n"
    #file_cdr.write(log_file_header)

    log_file_size = os.path.getsize(log_file)

    if (round((log_file_size / 1024 / 1024), 2) > 50.0):
      os.rename('/home/entpbx/check_sip.log', '/home/entpbx/check_sip.log_' + datetime.now().strftime('%Y%m%d%H%M%S'))
    else:
      pass
  else:
    # 文件不存在, 创建文件
    logging.basicConfig(level=logging.INFO,
                        filename=log_file,
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    logging.info('new log file created!')
    #with codecs.open('/home/entpbx/check_sip.log', 'w', 'utf-8') as file_cdr:
    #  log_file_header = "===[INFO] new log file create at " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "===\r\n"
    #  file_cdr.write(log_file_header)

  #sql = ''' select sysdate from dual; '''

  sql = """select count(ENABLED) as ROW_CNT
    from pbx_sip_checker
   where substr(destination, 1, instr(destination, ':', 1) - 1) = '10.16.155.12'
    and ENABLED = 'no' and tm_create > sysdate - 3/60/24;"""
  
  cur_time = datetime.now() 
  ihour = datetime.now().hour
  iminute = datetime.now().minute
  
  if (ihour == 3) and (iminute < 30):
    print '[INFO] dont check at this time! ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
  else:
    listResult = QueryBySqlPlus(sql)
    print listResult
    #print listResult[0]['ROW_CNT']
    if len(listResult) == 1:
      print "[INFO]query result cnt:" + listResult[0]['ROW_CNT'] 
      if listResult[0]['ROW_CNT'] == unicode('1', 'UTF-8'):
        print '[WARN]catch local sip OFFLINE! '
        # 发现异常执行相关处理
        #从文件INI文件获取上次异常处理时间
        cf = ConfigParser.ConfigParser()
        cf.read(cfg_file)
        s_last_dotime = cf.get("baseconf", "last_dotime")
        last_dotime = datetime.strptime(s_last_dotime,"%Y-%m-%d %H:%M:%S")
        t_delta = cur_time - last_dotime
        if (t_delta.days > 0) or (t_delta.seconds > 300):
        # 上次重启时间比对 ,本机python版本2.6.6，datetime.timedelta没有相关属性, 重新计算秒差, > 300s/5分钟执行重启
        #if round(1.0 * ((cur_time - last_dotime).microseconds + ((cur_time - last_dotime).seconds + (cur_time - last_dotime).days * 24 * 3600) * 10**6) / 10**6,0) > 600 :
          # 执行杀进程操作
          os.system('killall -9 ngcc1')
          print  '[WARN] catch local sip offline and DONE operate! ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
          #并发短信提醒
          #更新配置文件, 写入本次执行kill进程操作时间
          cf.set("baseconf", "last_dotime", datetime.strftime(cur_time,'%Y-%m-%d %H:%M:%S'))
          cf.write(open(cfg_file, "w"))
        else: #即5分钟内做了一次kill, 不执行kill操作
          print '[WARN] catch local sip offline, but time interval , SKIP operate! ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')  

finally:
  
  # print  '===py check finally end ' + datetime.now().strftime('%Y%m%d%H%M%S') + '==='
  print  '=== === py check finally end ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' === ==='

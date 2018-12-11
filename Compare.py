

      write_Log(Log_File,"02. %s | Select data from cdp.CRCAMF......"%str(datetime.datetime.now()))
      Original_df_CRCAMF = SQL_df_CRCAMF()
      df_CRCAMF = Original_df_CRCAMF.copy() #SQL 執行一次之後，儲存於 Original_df_CRCAMF，要用時直接copy出來，節省執行時間
      write_Log(Log_File,"ok\n")

      write_Log(Log_File,"03. %s | Clean the df_CRCAMF......"%str(datetime.datetime.now()))
      df_CRCAMF[u'LSKIDT_fix'] =time_cleaner(df_CRCAMF[u'LSKIDT'])
      df_CRCAMF[u'SEDLDT_fix'] =time_cleaner(df_CRCAMF[u'SEDLDT'])
      df_CRCAMF[u'REDLDT_fix'] =time_cleaner(df_CRCAMF[u'REDLDT'])
      df_CRCAMF[u'LSKIDT_stamp'] = df_CRCAMF[[u'LSKIDT_fix', u'SEDLDT_fix',u'REDLDT_fix']].min(axis=1)

      #修正 NaT 為 isnull()
      df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'LSKIDT_stamp'] = df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'STRDT']

      #車輛篩選
      #u'PRODDT', u'LOADT', u'ISADT', u'SALEDT', u'LSKIDT', u'REDLDT'
      df_CRCAMF[u'LSKIDT_fix'] =time_cleaner(df_CRCAMF[u'LSKIDT'])
      df_CRCAMF[u'SEDLDT_fix'] =time_cleaner(df_CRCAMF[u'SEDLDT'])
      df_CRCAMF[u'REDLDT_fix'] =time_cleaner(df_CRCAMF[u'REDLDT'])
      df_CRCAMF[u'LSKIDT_stamp'] = df_CRCAMF[[u'LSKIDT_fix', u'SEDLDT_fix',u'REDLDT_fix']].min(axis=1)

      df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'LSKIDT_stamp'] = df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'STRDT']

      # print ('暫用15年以上車輛') today.year - Candidate_Car_age
      df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'LSKIDT_stamp']<=datetime.datetime(today.year - Candidate_Car_age,today.month,1,0,0,0,0)) &                      (df_CRCAMF[u'LSKIDT_stamp']>=datetime.datetime(1988,1,1,0,0,0,0))]

      # print ('去除買不到 1 年的中古車') # 法規規定中古車須購買滿 1 年始可申請貨物稅補助
      df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'UCDELIVIDT'] <= datetime.datetime(today.year - 1,today.month,1,0,0,0,0))|                       df_CRCAMF[u'UCDELIVIDT'].isnull()]

      # print ('去除舊車牌已更換新車牌者')
      # df_remove_LICSNO = pd.read_csv(r"./Data/remove_LICSNO.csv", sep=',', encoding = 'utf-8' )
      df_remove_LICSNO = pd.read_csv(Import_Data_Path +"remove_LICSNO.csv", sep=',', encoding = 'utf-8' )


      df_remove_LICSNO[u'LICSNO'] = df_remove_LICSNO[u'LICSNO'].map(lambda x: x.strip().upper())
      remove_LICSNO_list = df_remove_LICSNO['LICSNO'].tolist()
      df_CRCAMF[u'LICSNO'] = df_CRCAMF[u'LICSNO'].map(lambda x: x.strip())
      df_CRCAMF = df_CRCAMF[~df_CRCAMF[u'LICSNO'].str.upper().isin(remove_LICSNO_list)]
      df_remove_LICSNO = None
      remove_LICSNO_list = None

      # print ('指定報廢區間為 20150708~ENDDATE現在 ') #沒有報廢日期者 不會被標示 因此三年內 回廠 有可能 已經報廢
      df_CRCAMF[u'LICSNO_upper'] = df_CRCAMF[u'LICSNO'].map(lambda x: x.strip().upper())
      # df_web_query = pd.read_csv(r"./Data/df_web_query_fix.csv", sep=',', encoding = 'utf-8' )
      df_web_query = pd.read_csv(Import_Data_Path +"df_web_query_fix.csv", sep=',', encoding = 'utf-8' )
      df_web_query[u'LICSNO_upper'] = df_web_query[u'LICSNO'].map(lambda x: x.strip().upper())
      df_web_query[u'CARDATE_fix'] = pd.to_datetime(df_web_query[u'CARDATE_fix'], format='%Y/%m/%d', errors = 'coerce')
      df_web_query = df_web_query[ (df_web_query[u'CARDATE_fix'] >= datetime.datetime(2015,7,8,0,0,0,0))&                             (df_web_query[u'CARDATE_fix'] <= pd.Timestamp(END_DATE))]

      df_CRCAMF = df_CRCAMF.merge(df_web_query[['LICSNO_upper','STATUS']], how = 'left', on='LICSNO_upper')
      df_CRCAMF = df_CRCAMF.drop('LICSNO_upper',1)
      df_CRCAMF_web_query20150708 = df_CRCAMF[df_CRCAMF['STATUS']==u'已回收']

      # print ('撈出', df_CRCAMF_web_query20150708.shape[0])
      df_CRCAMF_web_query20150708 = df_CRCAMF_web_query20150708[[u'LICSNO', u'CARNM', u'CARMDL', u'BDNO', u'EGNO', u'VIN']]
      # print (df_CRCAMF_web_query20150708.shape)

      df_CRCAMF = df_CRCAMF[df_CRCAMF['STATUS']!=u'已回收'] # 這裡是扣除已回收的車籍資料
      # print ('剩下', df_CRCAMF.shape)

      # print u'5. 去CARNM出現次數過少者 這邊不去除過少 由報廢去篩選出來'
      df_CRCAMF[u'CARNM'] = df_CRCAMF[u'CARNM'].map(lambda x: x.strip())
      dict_replacce_CARNM = {'EXSIOR':'CORONA', 'PREMIO':'CORONA', 'ALTIS':'COROLLA'}
      df_CRCAMF[u'CARNM_M'] = df_CRCAMF[u'CARNM'].replace(dict_replacce_CARNM)
      df_CRCAMF[u'BDNO'] = series_str_cleaner(df_CRCAMF[u'BDNO'])
      df_CRCAMF[u'EGNO'] = series_str_cleaner(df_CRCAMF[u'EGNO'])
      df_CRCAMF[u'VIN'] = series_str_cleaner(df_CRCAMF[u'VIN'])
      df_CRCAMF['BDNO'] = df_CRCAMF['BDNO'].str[-10:]
      df_CRCAMF['EGNO'] = df_CRCAMF['EGNO'].str[-10:]
      df_CRCAMF['VIN']  = df_CRCAMF['VIN'].str[-10:]
      df_CRCAMF = df_CRCAMF.reset_index(drop=True)

      write_Log(Log_File,"ok\n")

      #下載所有報廢車輛並且進行篩選
      write_Log(Log_File,"04. %s | Select data from cdp.SSHSCHISTORY......"%str(datetime.datetime.now()))
      Original_df_SSHSCHISTORY = SQL_df_SSHSCHISTORY()
      df_SSHSCHISTORY = Original_df_SSHSCHISTORY.copy() ##SQL 執行一次之後，儲存於 Original_df_XXX，要用時直接copy出來，節省執行時間
      write_Log(Log_File,"ok\n")
      # print (df_SSHSCHISTORY.shape)

      write_Log(Log_File,"05. %s | Clean the df_SSHSCHISTORY......"%str(datetime.datetime.now()))
      # print ('留下15年以上的車輛')# 10612改成留下15年的車子，ISSUE:發照日期
      df_SSHSCHISTORY[u'ISSUE_fix'] =time_cleaner(df_SSHSCHISTORY[u'ISSUE'])
      df_SSHSCHISTORY = df_SSHSCHISTORY[(df_SSHSCHISTORY[u'ISSUE_fix'] <= datetime.datetime(today.year - Candidate_Car_age,today.month,1,0,0,0,0)) &                      (df_SSHSCHISTORY[u'ISSUE_fix'] >= datetime.datetime(1988,1,1,0,0,0,0)) ]
      # print (df_SSHSCHISTORY.shape)

      # print ("2015 0708之後報廢的車輛 需要找回")
      df_SSHSCHISTORY[u'MODDT_fix'] = time_cleaner(df_SSHSCHISTORY[u'MODDT'])  #這次找2015 0708之後報廢的車輛  七月的從MODDT 8/3開始
      df_SSHSCHISTORY = df_SSHSCHISTORY[ df_SSHSCHISTORY['MODDT_fix'] >= datetime.datetime(2015,7,8,0,0,0,0) ]
      # print (df_SSHSCHISTORY.shape)

      df_SSHSCHISTORY[u'ENGINENO'] = series_str_cleaner(df_SSHSCHISTORY[u'ENGINENO'])
      df_SSHSCHISTORY[u'EGNOM'] = series_str_cleaner(df_SSHSCHISTORY[u'EGNOM'])
      df_SSHSCHISTORY[u'BODYNO'] = series_str_cleaner(df_SSHSCHISTORY[u'BODYNO'])
      df_SSHSCHISTORY[u'BDNOM'] = series_str_cleaner(df_SSHSCHISTORY[u'BDNOM'])
      df_SSHSCHISTORY[u'GRPNM'] = df_SSHSCHISTORY[u'GRPNM'].map(lambda x: x.strip())
      df_SSHSCHISTORY[u'EGNOM'] = df_SSHSCHISTORY.apply(lambda x: x['EGNOM'] if x['EGNOM'] not in x['ENGINENO'] else '', axis=1)
      df_SSHSCHISTORY[u'BDNOM'] = df_SSHSCHISTORY.apply(lambda x: x['BDNOM'] if x['BDNOM'] not in x['BODYNO'] else '', axis=1)

      # print (u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除')
      df_web_query[u'EGNO'] = series_str_cleaner(df_web_query[u'EGNO']).str[-9:]
      #9碼以下不清 後九碼相同者視為已報廢
      remove_LICSNO_list = df_web_query[u'EGNO'][df_web_query[u'EGNO'].map(lambda x: len(x))>8].tolist()
      df_SSHSCHISTORY['ENGINENO_temp'] = df_SSHSCHISTORY['ENGINENO'].str[-9:]
      #   有五萬多 ENGINENO_temp 是空白因此找不到
      # print (u'找到', df_SSHSCHISTORY[df_SSHSCHISTORY['ENGINENO_temp'].isin(remove_LICSNO_list)].shape)
      df_SSHSCHISTORY  = df_SSHSCHISTORY[~df_SSHSCHISTORY['ENGINENO_temp'].isin(remove_LICSNO_list)]
      # print (u'剩下',df_SSHSCHISTORY.shape)

      df_SSHSCHISTORY.loc[:,'is_scrapped']='1'

      # 用車身號碼 引擎號碼 後面10碼來比對 CRCAMF
      df_SSHSCHISTORY['ENGINENO'] = df_SSHSCHISTORY['ENGINENO'].str[-10:]
      df_SSHSCHISTORY['BODYNO'] = df_SSHSCHISTORY['BODYNO'].str[-10:]
      df_SSHSCHISTORY['EGNOM'] = df_SSHSCHISTORY['EGNOM'].str[-10:]
      df_SSHSCHISTORY['BDNOM'] = df_SSHSCHISTORY['BDNOM'].str[-10:]

      #報廢資料 限定9碼以上
      df_SSHSCHISTORY[u'ENGINENO'] = df_SSHSCHISTORY[u'ENGINENO'].map(lambda x: x if len(x) > 9 else '')
      df_SSHSCHISTORY[u'BODYNO'] = df_SSHSCHISTORY[u'BODYNO'].map(lambda x: x if len(x) > 9 else '')
      df_SSHSCHISTORY[u'EGNOM'] = df_SSHSCHISTORY[u'EGNOM'].map(lambda x: x if len(x) > 9 else '')
      df_SSHSCHISTORY[u'BDNOM'] = df_SSHSCHISTORY[u'BDNOM'].map(lambda x: x if len(x) > 9 else '')
      write_Log(Log_File,"ok\n")

      # list_HIST = ['ENGINENO','BODYNO','EGNOM','BDNOM']
      # list_CRCAMF = ['BDNO', 'EGNO', 'VIN']

      list_HIST = ['BODYNO','BDNOM']
      list_CRCAMF = ['BDNO']

      df_SSHSCHISTORY.to_csv(Temp_Path + "df_SSHSCHISTORY.csv", sep=',', encoding='utf-8')
      df_CRCAMF.to_csv(Temp_Path + "df_CRCAMF.csv", sep=',', encoding='utf-8')
      df_CRCAMF_web_query20150708.to_csv(Temp_Path + "df_CRCAMF_web_query20150708.csv", sep=',', encoding='utf-8')

      #交叉比對 也比對車名
      for indexHIST in list_HIST:
          for indexCRCAMF in list_CRCAMF:
      #         print (indexHIST, indexCRCAMF)
              merged = None
              merged = df_CRCAMF[['BDNO', 'EGNO', 'VIN','CARNM_M']].merge(df_SSHSCHISTORY[df_SSHSCHISTORY[indexHIST]!='']                [[indexHIST,'GRPNM','is_scrapped']].drop_duplicates(indexHIST), how='left', left_on=[indexCRCAMF,'CARNM_M'] , right_on=[indexHIST,'GRPNM'])
              merged['is_scrapped'] =  merged['is_scrapped'].fillna('0')
              df_CRCAMF[indexCRCAMF+'_'+indexHIST] = merged['is_scrapped'].reset_index(drop=True)

      #加回已知在20150708報廢的車輛 網站上查詢到 在crcamf比對到的 from  0001_web_query_minipulation.ipynb
      df_result_csv = df_CRCAMF[(df_CRCAMF['BDNO_BODYNO']=='1')|                (df_CRCAMF['BDNO_BDNOM']=='1')]
      df_result_csv = df_result_csv.append(df_CRCAMF_web_query20150708, ignore_index=True)
      df_result_csv.reset_index(drop=True, inplace =True)
      df_result_csv = df_result_csv[[u'LICSNO', u'CARNM', u'CARMDL', u'BDNO', u'EGNO', u'VIN']]
      df_result_csv.to_csv(Temp_Path+"df_CRCAMF_scrapped_after20150708.csv",  sep=',', encoding='utf-8')
        del df_result_csv
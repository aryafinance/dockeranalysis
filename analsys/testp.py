import argparse
import os
from google.cloud.sql.connector import Connector, IPTypes
import pytds
import sqlalchemy
from datetime import datetime
import time

OP_BUY = 0
OP_SELL = 1

def connect_with_connector(instance_connection_name, db_user, db_pass, db_name) -> sqlalchemy.engine.base.Engine:
    # Note: Saving credentials in environment variables is convenient, but not
    # secure - consider a more secure solution such as
    # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
    # keep secrets safe.

    ip_type = IPTypes.PUBLIC

    connector = Connector( ip_type=IPTypes.PUBLIC,
    enable_iam_auth=False,
    timeout=30)
    
    def getconn() -> pytds.Connection:        
        conn = connector.connect(
            instance_connection_name,
            "pytds",
            user=db_user,
            password=db_pass,
            db=db_name            
        )
        return conn

    pool = sqlalchemy.create_engine(
        "mssql+pytds://localhost",
        creator=getconn
        # ...
    )
    return pool

def BulTarih(dt, dakika):
    i = 0
    for an in dakika:             
        if an._datetime >= dt:
            tf = an._datetime - dt            
            if tf.seconds > 0:
                return i-1
            else:
                return i
        i = i+1
    return 0

def MinMax(ilk, son, fiyat, yon, dakika, before = True):
    minx: int = None
    minid = 0
    maxx: int = None
    maxid = 0
    if before:
        sira = son - ilk
    else:
        sira = 1
        
    minx = dakika[ilk]._low
    minid = sira
    maxx = dakika[ilk]._high
    maxid = sira
        
    for x in range(ilk, son):    
        #print(sira, dakika[x]._datetime, dakika[x]._low, dakika[x]._high)
        if minx > dakika[x]._low:            
            minx = dakika[x]._low
            minid = sira
            
        if maxx < dakika[x]._high:            
            maxx = dakika[x]._high      
            maxid = sira
        
        if before:
            sira = sira -1
        else:
            sira = sira +1
    
    if yon==OP_BUY:
        farkl = fiyat - minx
        farkh = fiyat - maxx
    elif yon==OP_SELL:
        farkl = minx - fiyat
        farkh = maxx - fiyat
    #print(fiyat, yon)    
    return minx, minid, maxx, maxid, farkl, farkh

def Analysis(ilk, son, fiyat, yon, dakika):
    duration = 0
    maxGain = None
    maxLoss = None
    maxGainID = 0
    maxLossID = 0
    gainDuration = 0
    lossDuration = 0
    
    h = dakika[ilk]._high
    l = dakika[ilk]._low
    maxGain = h
    maxGainID = ilk
    maxLoss = l
    maxLossID = ilk
    
    
    for x in range(ilk, son):
        h = dakika[x]._high
        l = dakika[x]._low
        c = dakika[x]._close
        duration = duration +1
        
        if maxGain < h:
            maxGain = h
            maxGainID = x
            
        if maxLoss > l:
            maxLoss = l
            maxLossID = x
                
        if yon == OP_BUY:                          
            if fiyat > c:
                gainDuration = gainDuration +1
            elif fiyat < c:
                lossDuration = lossDuration +1                             
            
        elif yon == OP_SELL:
            if fiyat < c:
                gainDuration = gainDuration +1
            elif fiyat > c:
                lossDuration = lossDuration +1
                             
            
            
    if yon == OP_BUY:
        maxGain = maxGain - fiyat
        maxLoss = maxLoss - fiyat       
        
    elif yon == OP_SELL:        
        temp = maxGain
        maxGain = fiyat - maxLoss
        maxLoss = fiyat - temp        
        
    return duration, gainDuration, lossDuration, maxGain, maxGainID-ilk, maxLoss, maxLossID-ilk



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_user', dest='db_user', type=str, help='Database username')
    parser.add_argument('--db_pass', dest='db_pass', type=str, help='Database password')
    parser.add_argument('--db_name', dest='db_name', type=str, help='Database name')
    parser.add_argument('--connection_name', dest='connection_name', type=str, help='Instance connection name')
    parser.add_argument('--gen_id', dest='gen_id', type=int, help='Gen ID Which one calculate')
    parser.add_argument('--pair', dest='pair', type=str, help='Symbol')


    args = parser.parse_args()
    db_user = args.db_user
    db_pass = args.db_pass
    db_name = args.db_name
    gen_id = args.gen_id
    pair = args.pair
    connection_name = args.connection_name
    if db_user == None or db_pass == None or db_name == None or connection_name == None or gen_id == None or pair == None:
        return print('PARAMETRE HATASI')
    else:
        print("Starting...")
        pool = connect_with_connector(connection_name, db_user, db_pass, db_name)
        print('Bağlandı')        
        result = pool.execute("SELECT [detayID],[fiyat],[tp],[sl],[yon],[tarihAcilis],[tarihKapanis],[kar],[genID] FROM [GenDetails] where genID = %s and tarihKapanis > '2000-01-01' order by detayID", gen_id).fetchall()
        print('Genler alındı.')
        satir = result[0]
        tarihBaslangic = satir.tarihAcilis.strftime("%Y-%m-%d %H:%M:%S")
        satir = result[-1]
        tarihBitis = satir.tarihKapanis.strftime("%Y-%m-%d %H:%M:%S")
        params = tarihBaslangic, tarihBitis, pair
        qr = "SELECT [fxid],[_open],[_high],[_low],[_close],[_datetime] FROM [dakikalik]	where _datetime between DATEADD(MINUTE, -10, %s) and DATEADD(MINUTE, 20, %s) and pair = %s order by _datetime"
        dakika = pool.execute(qr, params).fetchall()
        startTime = time.time()
        for genx in range (0, len(result)-1):
            ilkx = BulTarih(result[genx].tarihAcilis, dakika)
            sonx = BulTarih(result[genx].tarihKapanis, dakika)
            _, back10MinDist, _, back10MaxDist, back10Min, back10Max = (MinMax(ilkx-10,ilkx, result[genx].fiyat, result[genx].yon, dakika, True))
            _, forw10MinDist, _, forw10MaxDist, forw10Min, forw10Max = (MinMax(sonx+1,sonx+10, result[genx].fiyat, result[genx].yon, dakika, False))
            _, first20MinDist, _, first20MaxDist, first20Min, first20Max = MinMax(ilkx,ilkx+20, result[genx].fiyat, result[genx].yon, dakika, False)
                
            duration, gainDuration, lossDuration, maxGain, gainDist, maxLoss, lossDist  = Analysis(ilkx, sonx, result[genx].fiyat, result[genx].yon, dakika)
            sqlIns = 'INSERT INTO [dbo].[Analysis] ([genID],[dateOpen],[dateClose] ,[transactionID],[orderType],[profit],[openPrice],[closePrice],[maxGain],[maxGainDist],[maxLoss],[maxLossDist],[duration],[gainMinutes],[lossMinutes],[Back10Max],[Back10MaxDist],[Back10Min],[Back10MinDist],[Forw10Max],[Forw10MaxDist],[Forw10Min] ,[Forw10MinDist],[First20Max],[First20MaxDist],[First20Min] ,[First20MinDist]) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            fiyatKapanis = 0.00
            if result[genx].yon == OP_BUY:
                fiyatKapanis = result[genx].fiyat + (result[genx].kar / 100)
            else:
                fiyatKapanis = result[genx].fiyat - (result[genx].kar / 100)        
            
            paramsIns = (result[genx].genID, result[genx].tarihAcilis, result[genx].tarihKapanis, result[genx].detayID, result[genx].yon,
                        result[genx].kar / 100, result[genx].fiyat, fiyatKapanis, maxGain, gainDist, maxLoss, lossDist, duration,
                        gainDuration, lossDuration, back10Max, back10MaxDist, back10Min, back10MinDist, forw10Max, forw10MaxDist,
                        forw10Min, forw10MinDist, first20Max, first20MaxDist, first20Min, first20MinDist)
            
            res = pool.execute(sqlIns, paramsIns)
            print(res)
        print(time.time()-startTime)
        print('count ', len(result))
        print('OK')
        return print('OK')




if __name__ == "__main__":
    main()
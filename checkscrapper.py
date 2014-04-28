'''
Created on Apr 15, 2014

@author: Tataru Andrei Emanuel
'''

from bs4 import BeautifulSoup
import datetime
import logging
import glob
import os
import shutil

IMPORT_dir='./import/'
EXPORT_dir='./export/'
FILE_extension='.lst'
OUTPUT_SQL_FILE='output.sql'

HOSTNAME=''
ORASID=''
REPDATE=''
REPORTSTATUS=''
T1SESSIONS=''
T1PROCESSES=''
T1LOCKS=''
T1MAXROLLBACK=''

T2OVER10MIN=''
T2MAX=''

T3OVER10MIN=''
T3MAX=''

T4OVER1DAY=''
T4MAX=''

T5DEADLOCK=''

T6PUBSYNINVALID=''
T6USEROBJINVALID=''

TABLESPACENAME=''
TABLESPACENAME=''
MAXFREEPCT=''

T10UNEXTENDCOUNT=''

USERNAME=''
STATRUN=''
FAILURECOUNT=''

T12COMPLETED=''
T12DURATION=''

T13ARCHSIZE=''
T13FULLSIZE=''

def table1(REPDATE,HOSTNAME,tablestr):
    rows = tablestr.findAll(lambda tag: tag.name=='tr')
    r={}
    ReportStatus=0
    for row in rows:
        if row.contents[7].name <> 'th':
            respct = float(str(row.contents[7].string).strip())/float(str(row.contents[9].string).strip())
            #print "%.2f" % (respct)
            if respct > 0.95:
                ReportStatus=2
            elif respct > 0.85:
                ReportStatus=1
            
            rep={}
            rep[row.contents[1].string]=respct
            r.update(rep)
    '''
    if ReportStatus==0:                     
        print "UPDATE dailyreport set T1SESSIONS=%d, T1PROCESSES=%d, T1LOCKS=%d, T1MAXROLLBACK=%d WHERE Repdate = '%s';" % (r['sessions'],r['processes'],r['enqueue_locks'],r['max_rollback_segments'],REPDATE)
    else:
        print "UPDATE dailyreport set T1SESSIONS=%d, T1PROCESSES=%d, T1LOCKS=%d, T1MAXROLLBACK=%d, REPORTSTATUS=%s WHERE Repdate = '%s';" % (r['sessions'],r['processes'],r['enqueue_locks'],r['max_rollback_segments'],ReportStatus,REPDATE)
    '''
    return r['sessions'],r['processes'],r['enqueue_locks'],r['max_rollback_segments'],ReportStatus


def table2(REPDATE,HOSTNAME,tablestr):

        # table 2 
        t2 = tablestr
        #print 't2 --> '+str(t2)
        over10=0
        maxval=0
        rows = t2.findAll(lambda tag: tag.name=='tr')
        #r={}
        ReportStatus=0
        for row in rows:
            if row.contents[7].name <> 'th':
                #print row.contents[7].name
                #print row.contents[7].string
                if float(row.contents[7].string)>0:
                    over10=over10+1
                if float(row.contents[7].string)>maxval:
                    maxval=float(row.contents[7].string)
                
        if maxval>10:
            ReportStatus=1
        #    print "UPDATE dailyreport set T2OVER10MIN=%d, T2MAX=%s, REPORTSTATUS=%s WHERE Repdate = '%s';" % (over10,maxval,ReportStatus,REPDATE)
        #else:
        #    print "UPDATE dailyreport set T2OVER10MIN=%d, T2MAX=%s WHERE Repdate = '%s';" % (over10,maxval,REPDATE)
        
        return over10,maxval,ReportStatus
       

def table3(REPDATE,HOSTNAME,tablestr):
    # table 3 
    t3 = tablestr
    #print t3
    over10=0
    maxval=0
    rows = t3.findAll(lambda tag: tag.name=='tr')
    #r={}
    ReportStatus=0
    for row in rows:
        if row.contents[7].name <> 'th':
            #print row.contents[15].name
            #print row.contents[15].string
            try:
                if float(row.contents[15].string)>0:
                    over10=over10+1
                if float(row.contents[15].string)>maxval:
                    maxval=float(row.contents[15].string)
            except ValueError:
                if float(row.contents[17].string)>0:
                    over10=over10+1
                if float(row.contents[17].string)>maxval:
                    maxval=float(row.contents[17].string)
                
    if maxval>10:
        ReportStatus=1
    #    print "UPDATE dailyreport set T3OVER10MIN=%d, T3MAX=%s, REPORTSTATUS=%s WHERE Repdate = '%s';" % (over10,maxval,ReportStatus,REPDATE)
    #else:
    #    print "UPDATE dailyreport set T3OVER10MIN=%d, T3MAX=%s WHERE Repdate = '%s';" % (over10,maxval,REPDATE)
    return over10,maxval,ReportStatus


def table4(REPDATE,HOSTNAME,tablestr):
    # table 4 
    t4 = tablestr
    #print t4
    over10=0
    maxval=0
    rows = t4.findAll(lambda tag: tag.name=='tr')
    #r={}
    ReportStatus=0
    for row in rows:
        #print row.contents[11].name
        #print row.contents[11].string
        if row.contents[1].name <> 'th':
            #print row.contents[13].name
            #print row.contents[13].string
            try:
                duration2=row.contents[13].string.split(':')[0]
            except IndexError:
                #logging.error("malformed table -> T4")
                duration2=row.contents[11].string.split(':')[0]
            #print 'duration2 ->' + duration2
            try:
                t=int(duration2)
            except ValueError:
                t=duration2.split(' ')[0]
            
            if t>24:
                over10=over10+1
            if t>maxval:
                maxval=t
                    
        if maxval>24:
            ReportStatus=1
        #    print "UPDATE dailyreport set T4OVER1DAY=%d, T4MAX=%s, REPORTSTATUS=%s WHERE Repdate = '%s';" % (over10,maxval,ReportStatus,REPDATE)
        #else:
        #    print "UPDATE dailyreport set T4OVER1DAY=%d, T4MAX=%s WHERE Repdate = '%s';" % (over10,maxval,REPDATE)
    
    return T4OVER1DAY,T4MAX,ReportStatus


def table5(REPDATE,HOSTNAME,tablestr):
    # table 5 
    t5 = tablestr
    #print t5
    ReportStatus=0
    rows = t5.findAll(lambda tag: tag.name=='tr')
    countobj=len(rows)-1
    #print countobj
                
    if countobj>0:
        ReportStatus=1
    #    print "UPDATE dailyreport set T5DEADLOCK=%s, REPORTSTATUS=%s WHERE Repdate = '%s';" % (countobj,ReportStatus,REPDATE)
    #else:
    #    print "UPDATE dailyreport set T5DEADLOCK=%s WHERE Repdate = '%s';" % (countobj,REPDATE)
    return countobj,ReportStatus
        
def table6(REPDATE,HOSTNAME,tablestr):
    # table 6 
    t6 = tablestr
    #print t6
    count=0
    countSin=0
    countOthers=0
    rows = t6.findAll(lambda tag: tag.name=='tr')
    #r={}
    ReportStatus=0
    for row in rows:
        if row.contents[7].name <> 'th':
            #print row.contents[5].name
            #print row.contents[5].string
            
            if row.contents[5].string=="SYNONYM":
                countSin=countSin+1
            else:
                countOthers=countOthers+1
            count=count+1
            
    if countOthers>0:
        ReportStatus=1
    #    print "UPDATE dailyreport set T6PUBSYNINVALID=%s, T6USEROBJINVALID=%s, REPORTSTATUS=%s WHERE Repdate = '%s';" % (countSin,countOthers,ReportStatus,REPDATE)
    #else:
    #    print "UPDATE dailyreport set T6PUBSYNINVALID=%s, T6USEROBJINVALID=%s WHERE Repdate = '%s';" % (countSin,countOthers,REPDATE)
    return countSin,countOthers,ReportStatus

def table7(REPDATE,HOSTNAME,tablestr):
    # table 7 
    t7 = tablestr
    #print t7
    rows = t7.findAll(lambda tag: tag.name=='tr')
    r=[]
    ReportStatus=0
    for row in rows:
        #print row
        #print row.contents[3].name
        #print row.contents[3].string
        if row.contents[3].name <> 'th':
            
            #print "*****************************************"
            
            
            
            tablespaceName=row.contents[1].string
            try:
                freeperc=float(row.contents[11].string)
            except IndexError:
                freeperc=float(row.contents[9].string)
            if freeperc<15:
                ReportStatus=2
            #    print "UPDATE dailyreport set REPORTSTATUS=%s WHERE Repdate = '%s';" % (ReportStatus,REPDATE)
            #    print "INSERT into dailyreportt7 (CLIENT,DBLINK,REPDATE,TABLESPACENAME,MAXFREEPCT) values (%s,%s,%s,%s,%d);" % (HOSTNAME,'dblink',REPDATE,tablespaceName,freeperc)
            elif freeperc<20:
                ReportStatus=1
            #    print "UPDATE dailyreport set REPORTSTATUS=%s WHERE Repdate = '%s';" % (ReportStatus,REPDATE)
            #    print "INSERT into dailyreportt7 (CLIENT,DBLINK,REPDATE,TABLESPACENAME,MAXFREEPCT) values (%s,%s,%s,%s,%d);" % (HOSTNAME,'dblink',REPDATE,tablespaceName,freeperc)
            #else:
            #    print "INSERT into dailyreportt7 (CLIENT,DBLINK,REPDATE,TABLESPACENAME,MAXFREEPCT) values (%s,%s,%s,%s,%d);" % (HOSTNAME,'dblink',REPDATE,tablespaceName,freeperc)
            rep={}
            rep['tablespaceName']=tablespaceName
            rep['freeperc']=freeperc
            r.append(rep)
            #print "^^^^^^^^^^ %s" % r
            
    return  r,ReportStatus

def table10(REPDATE,HOSTNAME,tablestr):
    # table 10 
    t10 = tablestr
    #print t5
    ReportStatus=0
    rows = t10.findAll(lambda tag: tag.name=='tr')
    countobj=len(rows)-1
    #print countobj
                
    if countobj>0:
        ReportStatus=1
    #    print "UPDATE dailyreport set T10UNEXTENDCOUNT=%s, REPORTSTATUS=%s WHERE Repdate = '%s';" % (countobj,ReportStatus,REPDATE)
    #else:
    #    print "UPDATE dailyreport set T10UNEXTENDCOUNT=%s WHERE Repdate = '%s';" % (countobj,REPDATE)
    
    return countobj,ReportStatus

def table11(REPDATE,HOSTNAME,tablestr):
    # table 11 
    t11 = tablestr
    #print t11
    rows = t11.findAll(lambda tag: tag.name=='tr')
    r=[]
    ReportStatus=0
    for row in rows:
        if row.contents[3].name <> 'th':
            #print row.contents[9].name
            #print row.contents[9].string
            
            USERNAME=row.contents[1].string
            try:
                FAILURECOUNT=int(row.contents[7].string)
            except ValueError:
                FAILURECOUNT=int(row.contents[5].string)
            try:
                daterun=datetime.datetime.strptime(row.contents[11].string,'%d-%b-%Y %H:%M')
            except ValueError:
                daterun=datetime.datetime.strptime(row.contents[9].string,'%d-%b-%y')
            datecheck=datetime.datetime.strptime(REPDATE,'%d-%b-%y')
            diff=daterun-datecheck
            
            
            #print daterun.strftime('%d-%b-%Y %H:%M')
            #print datecheck.strftime('%d-%b-%Y %H:%M')
            #print diff.days
            
            if diff.days==0:
                STATRUN=1
            else:
                STATRUN=0
            
            if FAILURECOUNT>0 or STATRUN<>1:
                ReportStatus=1
                #print "UPDATE dailyreport set REPORTSTATUS=%s WHERE Repdate = '%s';" % (ReportStatus,REPDATE)
            #print "INSERT into dailyreportt11 (CLIENT,DBLINK,REPDATE,USERNAME,STATRUN,FAILURECOUNT) values (%s,%s,%s,%s,%s,%s);" % (HOSTNAME,'dblink',REPDATE,USERNAME,STATRUN,FAILURECOUNT)
            rep={}
            rep['USERNAME']=USERNAME
            rep['STATRUN']=STATRUN
            rep['FAILURECOUNT']=FAILURECOUNT
            r.append(rep)
    return r,ReportStatus

def table12(REPDATE,HOSTNAME,tablestr):
    # table 12 
    t12 = tablestr
    #print t12
    rows = t12.findAll(lambda tag: tag.name=='tr')
    ReportStatus=0
    BACKRUN=0
    BACKDUR=0
    for row in rows:
        if row.contents[3].name <> 'th':
  
            try:
                daterun=datetime.datetime.strptime(row.contents[5].string,'%d-%b-%Y %H:%M')
                BACKDURz = int(str(row.contents[9].string).split(':')[0].strip())*3600+int(str(row.contents[9].string).split(':')[1].strip())*60+int(str(row.contents[9].string).split(':')[2].strip())
            except ValueError:
                daterun=datetime.datetime.strptime(row.contents[7].string,'%m/%d/%y %H:%M')
                BACKDURz = float(row.contents[11].string)*60

            datecheck=datetime.datetime.strptime(REPDATE,'%d-%b-%y')
            diff=daterun-datecheck
   
            #print daterun.strftime('%d-%b-%Y %H:%M')
            #print datecheck.strftime('%d-%b-%Y %H:%M')
            #print diff.days
            
            if diff.days==0:
                BACKRUN=1
                BACKDUR=BACKDURz

    if BACKRUN==0:
        ReportStatus=1

    return BACKRUN,BACKDURz,ReportStatus


def table13(REPDATE,HOSTNAME,tablestr):
    
    T13ARCHSIZE=0
    T13FULLSIZE=0
    ReportStatus=1
    arch=False
    full=False
    rows = tablestr.findAll(lambda tag: tag.name=='tr')
    for row in rows:
        if row.contents[3].name <> 'th':
            try:
                daterun=datetime.datetime.strptime(row.contents[1].string,'%d-%b-%Y %H:%M')
            except ValueError:
                daterun=datetime.datetime.strptime(row.contents[1].string,'%d-%b-%y')
            datecheck=datetime.datetime.strptime(REPDATE,'%d-%b-%y')
            diff=daterun-datecheck
            if diff.days==0:
                #print row.contents[3].name
                #print row.contents[3].string
                if str(row.contents[3].string)=='Archive Log':
                    T13ARCHSIZE=row.contents[5].string
                    arch=True
                if str(row.contents[3].string)=='Full':
                    T13FULLSIZE=row.contents[5].string
                    full=True
    if arch==True and full==True:
        ReportStatus=0
    
    return T13ARCHSIZE,T13FULLSIZE,ReportStatus

def table14(REPDATE,HOSTNAME,tablestr):
    rows = tablestr.findAll(lambda tag: tag.name=='tr')
    r=[]
        
    ReportStatus=0
    for row in rows:
        if row.contents[3].name <> 'th':
            driveName=row.contents[0].string
            drivedescr=row.contents[1].string
            freeperc=float(row.contents[4].string)
            if freeperc<15:
                ReportStatus=2
            #    print "UPDATE dailyreport set REPORTSTATUS=%s WHERE Repdate = '%s';" % (ReportStatus,REPDATE)
            #    print "INSERT into dailyreportt14 (CLIENT,DBLINK,REPDATE,DRIVENAME,DRIVEDESCR,FREEPERC) values (%s,%s,%s,%s,%s,%d) ;" % (HOSTNAME,'dblink',REPDATE,driveName,drivedescr,freeperc)
            elif freeperc<20:
                ReportStatus=1
            #    print "UPDATE dailyreport set REPORTSTATUS=%s WHERE Repdate = '%s';" % (ReportStatus,REPDATE)
            #    print "INSERT into dailyreportt14 (CLIENT,DBLINK,REPDATE,DRIVENAME,DRIVEDESCR,FREEPERC) values (%s,%s,%s,%s,%s,%d) ;" % (HOSTNAME,'dblink',REPDATE,driveName,drivedescr,freeperc)
            #else:
            #    print "INSERT into dailyreportt14 (CLIENT,DBLINK,REPDATE,DRIVENAME,DRIVEDESCR,FREEPERC) values (%s,%s,%s,%s,%s,%d) ;" % (HOSTNAME,'dblink',REPDATE,driveName,drivedescr,freeperc)
            rep={}
            rep['driveName']=driveName
            rep['drivedescr']=drivedescr
            rep['freeperc']=freeperc
            r.append(rep)
    return r,ReportStatus            

def initExport():
    shutil.rmtree(EXPORT_dir)
    if not os.path.exists(EXPORT_dir):
        os.makedirs(EXPORT_dir)


def writeFile(fileName,wstring,mode):
    fo=open(fileName,mode)
    fo.write(wstring)
    fo.close()

def dataPreprocesing(InputFolder,OutputFolder):
    FILES=glob.glob(InputFolder+"*"+FILE_extension)
    logging.info("Files to be processed: "+', '.join(map(str, FILES)))
    for lstfile in FILES:
        logging.info("Processing file : "+lstfile)
        fo = open(lstfile,"r")
        s = fo.read()
        fo.close()
        array = s.split("<<<<%%%%>>>>")
        i=0
        j=0
        for check in array:
            if 'Health Check on' in check:
                fileName=OutputFolder+str(lstfile).replace(InputFolder,'')+'HC'+str(i)+".html"
                writeFile(fileName=fileName,wstring=check,mode='w')
                logging.info('Check html file created -> '+fileName)
                i=i+1
            elif 'Disk Usage Check on' in check:
                fileName=OutputFolder+str(lstfile).replace(InputFolder,'')+'DC'+str(j)+".html"
                writeFile(fileName=fileName,wstring=check,mode='w')
                logging.info('Check html file created -> '+fileName)
                j=j+1
            else:
                #logging.warning('Unrecognized check: '+check)
                pass
        logging.info("File "+str(lstfile).replace(InputFolder,'')+" processed")
        
def dataProcesing(InputFolder):
    FILES=glob.glob(InputFolder+"*")
    logging.info("Files to be processed: "+', '.join(map(str, FILES)))
    for lstfile in FILES:
        fileName=str(lstfile).replace(InputFolder,'')
        if 'HC' in fileName:
            logging.info("Processing HealthCheck file : "+fileName)
            parseHealthCheck(file=lstfile)
        elif 'DC' in fileName:
            logging.info("Processing DiskCheck file : "+fileName)
            parseDiskCheck(file=lstfile)



            
def parseDiskCheck(file):
        f = open(file, 'r')
        doc = BeautifulSoup(f.read())
        f.close()
        #print doc
        texts = doc.findAll(text=True)
        #print texts
        hoststring = texts[6].replace('Disk Usage Check on ', '').replace(' at','')
        HOSTNAME = hoststring.split(' ')[0]
        
        #print 'HOSTNAME= '+HOSTNAME
        REPDATE=texts[0]
        #print 'REPDATE= '+REPDATE
        tables = doc.findAll(lambda tag: tag.name=='table')
        #print tables
        t14 = tables[0]
        
        #
        #
        table14(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=t14)
        

        if 'Table 14 : Disk Usage' in str(t14):
            x,REPORTSTATUS = table14(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=t14)
            for row in x:
                s= "INSERT into dailyreportt14 (CLIENT,DBLINK,REPDATE,DRIVENAME,DRIVEDESCR,FREEPERC) values ('%s','%s','%s','%s','%s',%d) ;" % (HOSTNAME,'dblink',REPDATE,row['driveName'],row['drivedescr'],row['freeperc'])
                s=s+'\n'
                writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
        else:       
            logging.error("unknown table -> T14")
        
def parseHealthCheck(file):
    f = open(file, 'r')
    doc = BeautifulSoup(f.read())
    f.close()
    #print doc
    texts = doc.findAll(text=True)
    #print texts
    hoststring = texts[6].replace('Health Check on ', '').replace(' at','')
    ORASID = hoststring.split(' ')[0]
    HOSTNAME = hoststring.split(' ')[2]
    #print ORASID
    #print 'HOSTNAME= '+HOSTNAME
    REPDATE=texts[0]
    #print 'REPDATE= '+REPDATE
    tables = doc.findAll(lambda tag: tag.name=='table')
    #print tables
    
    #
    #
    if 'Table 1: Resource usage' in str(tables[0]):
        T1SESSIONS, T1PROCESSES, T1LOCKS, T1MAXROLLBACK, REPORTSTATUS = table1(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[0])
    else:
        logging.error("unknown table -> T1")
    
    if 'Table 2: TOP 10 programs by cpu time' in str(tables[1]):
        T2OVER10MIN,T2MAX,REPORTSTATUS=table2(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[1])
    else:
        logging.error("unknown table -> T2")
            
    if 'Table 3: TOP 10 long operations' in str(tables[2]):
        T3OVER10MIN, T3MAX, REPORTSTATUS = table3(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[2])
    else:
        logging.error("unknown table -> T3")
        
    if 'Table 4: TOP 10 IDLE Sesions' in str(tables[4]):
        T4OVER1DAY,T4MAX,REPORTSTATUS = table4(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[4])
    elif 'Table 4: TOP 10 IDLE Sesions' in str(tables[3]):
        T4OVER1DAY,T4MAX,REPORTSTATUS = table4(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[3])
    else:       
        logging.error("unknown table -> T4")
        
    
    if 'Table 5: DeadLocks' in str(tables[5]):
        T5DEADLOCK,REPORTSTATUS = table5(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[5])
    elif 'Table 5: DeadLocks' in str(tables[4]):
        T5DEADLOCK,REPORTSTATUS = table5(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[4])
    else:       
        logging.error("unknown table -> T5")
       
    if 'Table 6: Invalid Objects' in str(tables[6]):
        T6PUBSYNINVALID, T6USEROBJINVALID,REPORTSTATUS = table6(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[6])
    elif 'Table 6: Invalid Objects' in str(tables[5]):
        T6PUBSYNINVALID, T6USEROBJINVALID,REPORTSTATUS = table6(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[5])
    else:       
        logging.error("unknown table -> T6")    
    
    if 'Table 7: Tablespace utilization' in str(tables[7]):
        #TABLESPACENAME,MAXFREEPCT,REPORTSTATUS
        x,REPORTSTATUS=table7(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[7])        
        for row in x:
            s = "INSERT into dailyreportt7 (CLIENT,DBLINK,REPDATE,TABLESPACENAME,MAXFREEPCT) values ('%s','%s','%s','%s',%d);" % (HOSTNAME,'dblink',REPDATE,row['tablespaceName'],row['freeperc'])
            s = s+'\n'
            writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
        #print x
    elif 'Table 7: Tablespace utilization' in str(tables[6]):
        x,REPORTSTATUS = table7(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[6])
        for row in x:
            s = "INSERT into dailyreportt7 (CLIENT,DBLINK,REPDATE,TABLESPACENAME,MAXFREEPCT) values ('%s','%s','%s','%s',%d);" % (HOSTNAME,'dblink',REPDATE,row['tablespaceName'],row['freeperc'])
            s = s+'\n'
            writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
        #print x
    else:       
        logging.error("unknown table -> T7")
        
        
    if 'Table 10: Unextendable Objects' in str(tables[10]):
        T10UNEXTENDCOUNT,REPORTSTATUS = table10(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[10])
    elif 'Table 10: Unextendable Objects' in str(tables[9]):
        T10UNEXTENDCOUNT,REPORTSTATUS = table10(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[9])
    else:       
        logging.error("unknown table -> T10") 
        
    if 'Table 11: Statistics job Status' in str(tables[11]):
        x,REPORTSTATUS = table11(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[11])
        for row in x:
            s = "INSERT into dailyreportt11 (CLIENT,DBLINK,REPDATE,USERNAME,STATRUN,FAILURECOUNT) values ('%s','%s','%s','%s','%s','%s');" % (HOSTNAME,'dblink',REPDATE,row['USERNAME'],row['STATRUN'],row['FAILURECOUNT'])
            s = s+'\n'
            writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
    elif 'Table 11: Statistics job Status' in str(tables[10]):
        x,REPORTSTATUS = table11(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[10])
        for row in x:
            s = "INSERT into dailyreportt11 (CLIENT,DBLINK,REPDATE,USERNAME,STATRUN,FAILURECOUNT) values ('%s','%s','%s','%s','%s','%s');" % (HOSTNAME,'dblink',REPDATE,row['USERNAME'],row['STATRUN'],row['FAILURECOUNT'])
            s = s+'\n'
            writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
    else:       
        logging.error("unknown table -> T11")        
        
    if 'Table 12: RMAN BackUP job Status' in str(tables[12]):
        T12COMPLETED,T12DURATION,REPORTSTATUS=table12(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[12])
    elif 'Table 12: RMAN BackUP job Status' in str(tables[11]):
        T12COMPLETED,T12DURATION,REPORTSTATUS=table12(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[11])
    else:       
        logging.error("unknown table -> T12")
        
    if 'Table 13: RMAN BackUP job Status' in str(tables[13]):
        T13ARCHSIZE,T13FULLSIZE,ReportStatus=table13(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[13])
    elif 'Table 13: RMAN BackUP job Status' in str(tables[12]):
        T13ARCHSIZE,T13FULLSIZE,ReportStatus=table13(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[12])
    else:       
        logging.error("unknown table -> T13")
        
    try:    
        if 'Table 14 : Disk Usage' in str(tables[13]):
            x,REPORTSTATUS = table14(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[13])
            for row in x:
                s = "INSERT into dailyreportt14 (CLIENT,DBLINK,REPDATE,DRIVENAME,DRIVEDESCR,FREEPERC) values ('%s','%s','%s','%s','%s',%d) ;" % (HOSTNAME,'dblink',REPDATE,row['driveName'],row['drivedescr'],row['freeperc'])
                s = s+'\n'
                writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
        elif 'Table 14 : Disk Usage' in str(tables[14]):
            x,REPORTSTATUS = table14(REPDATE=REPDATE,HOSTNAME=HOSTNAME,tablestr=tables[14])
            for row in x:
                s = "INSERT into dailyreportt14 (CLIENT,DBLINK,REPDATE,DRIVENAME,DRIVEDESCR,FREEPERC) values ('%s','%s','%s','%s','%s',%d) ;" % (HOSTNAME,'dblink',REPDATE,row['driveName'],row['drivedescr'],row['freeperc'])
                s = s+'\n'
                writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
        else:       
            logging.error("unknown table -> T14")
    except IndexError:
        logging.error("unknown table -> T14")
    
    
    #print "INSERT into dailyreport (HOSTNAME,ORASID,REPDATE,T1PROCESSES,T1SESSIONS,T1LOCKS,T1MAXROLLBACK,T2OVER10MIN,T2MAX,T3OVER10MIN,T3MAX,T4OVER1DAY,T4MAX,T6PUBSYNINVALID,T6USEROBJINVALID,T10UNEXTENDCOUNT,T12COMPLETED,T12DURATION,T13ARCHSIZE,T13FULLSIZE,REPORTSTATUS,T5DEADLOCK) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" % (HOSTNAME,ORASID,REPDATE,T1PROCESSES,T1SESSIONS,T1LOCKS,T1MAXROLLBACK,T2OVER10MIN,T2MAX,T3OVER10MIN,T3MAX,T4OVER1DAY,T4MAX,T6PUBSYNINVALID,T6USEROBJINVALID,T10UNEXTENDCOUNT,T12COMPLETED,T12DURATION,T13ARCHSIZE,T13FULLSIZE,REPORTSTATUS,T5DEADLOCK)
    s = "INSERT into dailyreport (HOSTNAME,ORASID,REPDATE,T1PROCESSES,T1SESSIONS,T1LOCKS,T1MAXROLLBACK,T2OVER10MIN,T2MAX,T3OVER10MIN,T3MAX,T4OVER1DAY,T4MAX,T6PUBSYNINVALID,T6USEROBJINVALID,T10UNEXTENDCOUNT,T12COMPLETED,T12DURATION,T13ARCHSIZE,T13FULLSIZE,REPORTSTATUS,T5DEADLOCK) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" % (HOSTNAME,ORASID,REPDATE,T1PROCESSES,T1SESSIONS,T1LOCKS,T1MAXROLLBACK,T2OVER10MIN,T2MAX,T3OVER10MIN,T3MAX,T4OVER1DAY,T4MAX,T6PUBSYNINVALID,T6USEROBJINVALID,T10UNEXTENDCOUNT,T12COMPLETED,T12DURATION,T13ARCHSIZE,T13FULLSIZE,REPORTSTATUS,T5DEADLOCK)
    s = s+'\n'
    writeFile(fileName=EXPORT_dir+OUTPUT_SQL_FILE, wstring=s,mode='a')
    

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    initExport()
    logging.info("EXPORT Folder cleaned")
    logging.info("Scrape started")
    dataPreprocesing(InputFolder=IMPORT_dir,OutputFolder=EXPORT_dir)
    dataProcesing(InputFolder=EXPORT_dir)

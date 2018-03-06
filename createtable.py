
# creating trace file at 2018-03-06 05:31:18.706

# HDB information
# Version: 2.00.024.00.1519806017 (fa/hana2sp02)
# Build host: ld4551
# Build time: 2018-02-28 09:54:10
# Platform: linuxx86_64
# Compiler: gcc (SAP release 20170307, based on SUSE gcc6-6.2.1+r239768-2.4) 6.2.1 20160826 [gcc-6-branch revision 239773]
# Maketype: rel
# Branch: fa/hana2sp02
# Git hash: 267bef7bceb6773a90546ba25c4a02b33ae1cc0b
# Git mergetime: 2018-02-28 09:20:17
# Weekstone: 0000.00.0

# Trace options
# Trace level: all_with_results
# Trace file: mytrace.py
# Database user: SYSTEM
# Application user: 
# Table/View: 
# Application: 
# Statement type: 
# Flush limit: 16

from hdbcli import dbapi
import binascii
from datetime import datetime, date, time
import sys

_DBADDRESS_='localhost'
_DBPORT_=38015
_DBUSER_='SYSTEM'
_DBPW_='manager'
try:
    _DBADDRESS_ = sys.argv[1]
    _DBPORT_ = int(sys.argv[2])
    _DBUSER_ = sys.argv[3]
    _DBPW_ = sys.argv[4]
except:
    pass


# getConnection call (thread 26970, con-id 300122) at 2018-03-06 05:31:18.707138
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122 = dbapi.connect( user=_DBUSER_, password=_DBPW_, address=_DBADDRESS_, port=_DBPORT_, autocommit=True )


# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:18.707403
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)
# begin prepareStatement (thread 25504, con-id 300122) at 2018-03-06 05:31:18.707861
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139825752121344_c122 = con_c122.cursor()
# end prepareStatement (thread 25504, con-id 300122) at 2018-03-06 05:31:18.709350

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:18.953196
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
try:
    cursor_139825752121344_c122.execute(''' drop table TEST1 ''')
except:
    pass


try:
    cursor_139825752121344_c122.execute(''' drop table TEST2 ''')
except:
    pass

# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:18.953582
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139825752121344_c122.execute(''' CREATE COLUMN TABLE "SYSTEM"."TEST1" ("NAME" VARCHAR(1)) UNLOAD PRIORITY 5 AUTO MERGE ''')
# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:18.959201
# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:19.211151
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)
# begin prepareStatement (thread 25504, con-id 300122) at 2018-03-06 05:31:19.211631
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139825752152064_c122 = con_c122.cursor()
# end prepareStatement (thread 25504, con-id 300122) at 2018-03-06 05:31:19.213069

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:19.458247
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:19.458631
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139825752152064_c122.execute(''' CREATE ROW TABLE "SYSTEM"."TEST2" ( "NAME" VARCHAR CS_STRING ) ''')
# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:19.460893
# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:19.712542
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)
# begin prepareStatement (thread 25937, con-id 300122) at 2018-03-06 05:31:19.713047
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139830987214848_c122 = con_c122.cursor()
# end prepareStatement (thread 25937, con-id 300122) at 2018-03-06 05:31:19.718207

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:19.965342
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:19.965706
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139830987214848_c122.execute(''' insert into "SYSTEM"."TEST1" values ('a') ''')

# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:19.969560
# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:20.221623
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)
# begin prepareStatement (thread 25666, con-id 300122) at 2018-03-06 05:31:20.222169
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139829207279616_c122 = con_c122.cursor()
# end prepareStatement (thread 25666, con-id 300122) at 2018-03-06 05:31:20.224271

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:20.468457
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:20.468769
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139829207279616_c122.execute(''' insert into "SYSTEM"."TEST2" values ('b') ''')
# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:20.469290
# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:20.723122
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)
# begin prepareStatement (thread 25947, con-id 300122) at 2018-03-06 05:31:20.723624
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139830987233280_c122 = con_c122.cursor()
# end prepareStatement (thread 25947, con-id 300122) at 2018-03-06 05:31:20.727723

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:20.971739
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:20.971957
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139830987233280_c122.execute(''' select * from "SYSTEM"."TEST1" ''')
# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:20.972220
# ResultSet.columnLabel = [NAME:VARCHAR] (thread 26970, con-id 300122) at 2018-03-06 05:31:20.972320
# ResultSet.row[1] = [u'''a'''] (thread 26970, con-id 300122) at 2018-03-06 05:31:20.972341
# ResultSet:: there are no more rows(Accumulated Row Count:1) (thread 26970, con-id 300122) at 2018-03-06 05:31:20.972346
# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:21.226858
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)

# getConnection call (thread 26963, con-id 300106) at 2018-03-06 05:31:21.686039
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c106 = dbapi.connect( user=_DBUSER_, password=_DBPW_, address=_DBADDRESS_, port=_DBPORT_, autocommit=False )


# begin setAutoCommit (thread 26963, con-id 300106) at 2018-03-06 05:31:21.686159
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c106.setautocommit(True)
# begin prepareStatement (thread 26963, con-id 300106) at 2018-03-06 05:31:21.686411
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139834097659904_c106 = con_c106.cursor()
# end prepareStatement (thread 26963, con-id 300106) at 2018-03-06 05:31:21.686655

# begin PreparedStatement_execute (thread 26963, con-id 300106) at 2018-03-06 05:31:21.686709
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139834097659904_c106.execute(''' SELECT TOP 1 		t1.BACKUP_ID, 		(			SELECT sum(t2.BACKUP_SIZE) 			FROM public.M_BACKUP_CATALOG_FILES t2 			WHERE t2.BACKUP_ID = t1.BACKUP_ID 			GROUP BY t2.BACKUP_ID		) as BBSIZE, 		t1.UTC_START_TIME, 		t1.UTC_END_TIME, 		t1.ENTRY_TYPE_NAME FROM public.M_BACKUP_CATALOG AS t1 WHERE 		t1.STATE_NAME = 'successful'  AND 		(		t1.ENTRY_TYPE_NAME = 'complete data backup' OR 		t1.ENTRY_TYPE_NAME = 'data snapshot'		) ORDER BY t1.UTC_START_TIME DESC ''')
# end PreparedStatement_execute (thread 26963, con-id 300106) at 2018-03-06 05:31:21.690942
# ResultSet.columnLabel = [BACKUP_ID:BIGINT, BBSIZE:BIGINT, UTC_START_TIME:LONGDATE, UTC_END_TIME:LONGDATE, ENTRY_TYPE_NAME:STRING] (thread 26963, con-id 300106) at 2018-03-06 05:31:21.691051
# ResultSet.row[1] = [1520307173760, 1879049607, 2018-03-06 03:32:53.7600000, 2018-03-06 03:33:03.7910000, u'''complete data backup'''] (thread 26963, con-id 300106) at 2018-03-06 05:31:21.691089
# ResultSet:: there are no more rows(Accumulated Row Count:1) (thread 26963, con-id 300106) at 2018-03-06 05:31:21.691096
# begin PreparedStatement_close (thread 26963, con-id 300106) at 2018-03-06 05:31:21.691320
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139834097659904_c106.close()
# end PreparedStatement_close (thread 26963, con-id 300106) at 2018-03-06 05:31:21.691340
# begin prepareStatement (thread 26963, con-id 300106) at 2018-03-06 05:31:21.935181
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139834473105408_c106 = con_c106.cursor()
# end prepareStatement (thread 26963, con-id 300106) at 2018-03-06 05:31:21.935486

# begin PreparedStatement_execute (thread 26963, con-id 300106) at 2018-03-06 05:31:21.935551
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139834473105408_c106.execute(''' SELECT TOP 1 		t2.DESTINATION_TYPE_NAME FROM public.M_BACKUP_CATALOG AS t1 JOIN public.M_BACKUP_CATALOG_FILES AS t2 ON t1.ENTRY_ID = t2.ENTRY_ID WHERE 		t1.BACKUP_ID = 1520307173760 ''')
# end PreparedStatement_execute (thread 26963, con-id 300106) at 2018-03-06 05:31:21.938909
# ResultSet.columnLabel = [DESTINATION_TYPE_NAME:STRING] (thread 26963, con-id 300106) at 2018-03-06 05:31:21.938981
# ResultSet.row[1] = [u'''file'''] (thread 26963, con-id 300106) at 2018-03-06 05:31:21.939008
# ResultSet:: there are no more rows(Accumulated Row Count:1) (thread 26963, con-id 300106) at 2018-03-06 05:31:21.939012

# begin prepareStatement (thread 25934, con-id 300122) at 2018-03-06 05:31:21.227513
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139828128225280_c122 = con_c122.cursor()
# end prepareStatement (thread 25934, con-id 300122) at 2018-03-06 05:31:21.232950

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:21.476446
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:21.476642
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139828128225280_c122.execute(''' SELECT * from SYS.M_FEATURES where COMPONENT_NAME='FULLTEXTINDEX' and FEATURE_NAME='TOKEN_SEPARATORS' ''')
# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:21.476853
# ResultSet.columnLabel = [COMPONENT_NAME:VARCHAR, FEATURE_NAME:VARCHAR, FEATURE_VERSION:BIGINT] (thread 26970, con-id 300122) at 2018-03-06 05:31:21.476928
# ResultSet.row[1] = [u'''FULLTEXTINDEX''', u'''TOKEN_SEPARATORS''', 1] (thread 26970, con-id 300122) at 2018-03-06 05:31:21.476946
# ResultSet:: there are no more rows(Accumulated Row Count:1) (thread 26970, con-id 300122) at 2018-03-06 05:31:21.476956
# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:22.046502
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(False)
# begin prepareStatement (thread 25507, con-id 300122) at 2018-03-06 05:31:22.046907
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139829207304192_c122 = con_c122.cursor()
# end prepareStatement (thread 25507, con-id 300122) at 2018-03-06 05:31:22.050196

# begin setAutoCommit (thread 26970, con-id 300122) at 2018-03-06 05:31:22.295675
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
con_c122.setautocommit(True)
# begin PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:22.295869
# con info [con-id 300122, tx-id 34, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139829207304192_c122.execute(''' select * from "SYSTEM"."TEST2" ''')
# end PreparedStatement_execute (thread 26970, con-id 300122) at 2018-03-06 05:31:22.296022
# ResultSet.columnLabel = [NAME:VARCHAR] (thread 26970, con-id 300122) at 2018-03-06 05:31:22.296100
# ResultSet.row[1] = [u'''b'''] (thread 26970, con-id 300122) at 2018-03-06 05:31:22.296114
# ResultSet:: there are no more rows(Accumulated Row Count:1) (thread 26970, con-id 300122) at 2018-03-06 05:31:22.296119

# begin PreparedStatement_close (thread 26963, con-id 300106) at 2018-03-06 05:31:21.939304
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]
cursor_139834473105408_c106.close()
# end PreparedStatement_close (thread 26963, con-id 300106) at 2018-03-06 05:31:21.939322
# begin setAutoCommit (thread 26963, con-id 300106) at 2018-03-06 05:31:22.182105
# con info [con-id 300106, tx-id 27, cl-pid 10644, cl-ip 10.56.177.241, user: SYSTEM, schema: SYSTEM]

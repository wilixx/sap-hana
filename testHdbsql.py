import os, sys
import tempfile, shutil
import subprocess
import re
from lib.sqlTest import SqlTestCase, classification
from lib.hdbsqlHelper import HdbsqlHelper

class Hdbsql(SqlTestCase):
    def setUp(self):
        super(Hdbsql, self).setUp()
        self.helper = HdbsqlHelper(self.conman)

    @classmethod
    def setUpTestCase(self):
        if os.environ.has_key('NGDBTESTROOT'):
            self.__tempDirectory = os.path.join(os.environ['NGDBTESTROOT'], 'testhdbsql')
            if not os.path.exists(self.__tempDirectory):
                os.makedirs(self.__tempDirectory)
        else: 
            self.__tempDirectory = tempfile.mkdtemp(prefix='testhdbsql')

    def tearDownTestCase(self):
        shutil.rmtree(self.__tempDirectory)
        super(SqlTestCase, self).tearDownTestCase()

    def testVersion(self):
        """check hdbsql -v output"""

        self.helper.callHdbsql(commandline="-v",
                        expected=['HDBSQL version ' + self.helper.VERSION_PATTERN_WITH_BUILD + ', the SAP HANA Database interactive terminal\.',
                                  'Copyright (\d)+-(\d+) by SAP SE\.',
                                  ''])

    def testMixedPortNumberConnect(self):
        """check hdbsql -i -n option"""

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   'host          : [a-zA-Z0-9._-]+:3\d\d\d\d',
                   'sid           : \w\w\w',
                   'dbname        : \w+',
                   'user          : \w+',
                   'kernel version:\s+' + self.helper.KERNEL_VERSION_PATTERN,
                   'SQLDBC version:\s+' + self.helper.SQLDBC_VERSION_PATTERN,
                   'autocommit    : ON',
                   'locale        : .+',
                   'input encoding: \w+',
                   '',
                   'DUMMY',
                   '"X"',
                   '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                   '',
                   '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParametersDuplicatePortInfo(),
                        inputData="\\s\nselect * from dummy\n\\q\n",
                        expected=expected)

    def testSeparatorInInteractiveMode(self):
        """check hdbsql -c option"""

        self.dropProcedure("blub")

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                   '',
                   '']

# please note: we can only use multiline mode in sense of passing one statement that includes multiple lines (e.g. create procedure)
# it is not possible to add another statement to the string below (e.g. additionally drop procedure)
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -m -c '#'",
                        inputData="CREATE PROCEDURE blub(OUT bla CHAR)\nLANGUAGE SQLSCRIPT\nAS BEGIN\nSELECT 'a' INTO bla FROM dummy; \nEND#\n",
                        expected=expected)

    def testCommandLineOptionInputEncoding(self):
        """check hdbsql -B option"""

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   'host          : [a-zA-Z0-9._-]+:3\d\d\d\d',
                   'sid           : \w\w\w',
                   'dbname        : \w+',
                   'user          : \w+',
                   'kernel version:\s+' + self.helper.KERNEL_VERSION_PATTERN,
                   'SQLDBC version:\s+' + self.helper.SQLDBC_VERSION_PATTERN,
                   'autocommit    : ON',
                   'locale        : .+',
                   'input encoding: UTF8',
                   '',
                   '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -B UTF8",
                        inputData="\\s\n",
                        expected=expected)

    def testInternalOptionInputEncoding(self):
        """check internal option \ie UTF8"""

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   'host          : [a-zA-Z0-9._-]+:3\d\d\d\d',
                   'sid           : \w\w\w',
                   'dbname        : \w+',
                   'user          : \w+',
                   'kernel version:\s+' + self.helper.KERNEL_VERSION_PATTERN,
                   'SQLDBC version:\s+' + self.helper.SQLDBC_VERSION_PATTERN,
                   'autocommit    : ON',
                   'locale        : .+',
                   'input encoding: UTF8',
                   '',
                   '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="\\ie UTF8\n\s\n",
                        expected=expected)

    def testInternalOptionInputFileQuotedFileName(self):
        """Check internal option \i for a quoted input file"""

        if sys.platform == 'win32':
            print "test is not supported on this platform"
            return

        filePath = os.path.join(self.__tempDirectory, "te st1.sql")
        inputFile = open(filePath, "w")
        inputFile.write("select * from dummy")
        inputFile.close()

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   'DUMMY',
                   '"X"',
                   '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                   '',
                   '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData='\i "' + filePath + '"\n',
                        expected=expected)

    def testInternalOptionInputFile(self):
        """Check internal option \i for a simple input file"""

        if sys.platform == 'win32':
            print "test is not supported on this platform"
            return

        filePath = os.path.join(self.__tempDirectory, "test.sql")
        inputFile = open(filePath, "w")
        inputFile.write("select * from dummy")
        inputFile.close()

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   'DUMMY',
                   '"X"',
                   '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                   '',
                   '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="\i " + filePath + "\n",
                        expected=expected)
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="\input " + filePath + "\n",
                        expected=expected)
    
    def testCommandSeparatorOnOwnLine(self):
        filePath = os.path.join(self.__tempDirectory, "test.sql")
        inputFile = open(filePath, "w")
        inputFile.write("""CREATE PROCEDURE ownline()
LANGUAGE SQLSCRIPT 
AS
BEGIN
    select ';' from dummy;
END
 ; 
select ';' from dummy""")
        inputFile.close()

        expected = ["';'",
                    '";"',
                   '']
        
        self.dropProcedure('ownline')
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + '-separatorownline -m -I ' + filePath,
                        inputData="",
                        expected=expected)

    def testStrictSeparatorLine(self):
        filePath = os.path.join(self.__tempDirectory, "test.sql")
        inputFile = open(filePath, "w")
        inputFile.write(
"""
/*
Case1: Not sending comment only but still report in -fn
*/
go
-- Case2: Not sending comment only but still report in -fn
go
/* Case3: Not sending incomplete comment but still report in -fn
go
*/
go
Case4: Sending this and expects server error
go
Case5: Sending the next go with leading space and expects server error
 go
go
Case6: Sending the next go with trailing space and expects server error
go 
go
Case7: Sending the next Go with capital and expects server error
Go
go
Case8: Sending lone quote ' and expects server error
go
Case9: Sending lone double quote " and expects server error
go
Case10: Sending lone begin and expects server error
go
-- Case11: A normal batch
create procedure testProc()
as
begin
    select 1 from dummy;
end
go
/* Case12: Another normal batch */
drop procedure testProc;
-- The next 3 cases are batches with 4 spaces, empty line and empty respectively
go
    
go

go
go
define variables var1='should work'
select '&var1' from dummy;
go
""")
        inputFile.close()

        expected = [
            'Batch starting at line #2:',
            '1) /*',
            '2) Case1: Not sending comment only but still report in -fn',
            '3) */',
            '',
            'Batch starting at line #6:',
            '1) -- Case2: Not sending comment only but still report in -fn',
            '',
            'Batch starting at line #8:',
            '1) /* Case3: Not sending incomplete comment but still report in -fn',
            '2) go',
            '3) */',
            '',
            'Batch starting at line #12:',
            '1) Case4: Sending this and expects server error',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case4": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #14:',
            '1) Case5: Sending the next go with leading space and expects server error',
            '2)  go',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case5": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #17:',
            '1) Case6: Sending the next go with trailing space and expects server error',
            '2) go ',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case6": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #20:',
            '1) Case7: Sending the next Go with capital and expects server error',
            '2) Go',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case7": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #23:',
            "1) Case8: Sending lone quote ' and expects server error",
            '',
            '* 257: sql syntax error: incorrect syntax near "Case8": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #25:',
            '1) Case9: Sending lone double quote " and expects server error',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case9": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #27:',
            '1) Case10: Sending lone begin and expects server error',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case10": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            'Batch starting at line #29:',
            '1) -- Case11: A normal batch',
            '2) create procedure testProc()',
            '3) as',
            '4) begin',
            '5)     select 1 from dummy;',
            '6) end',
            '',
            'Batch starting at line #36:',
            '1) /* Case12: Another normal batch */',
            '2) drop procedure testProc;',
            '3) -- The next 3 cases are batches with 4 spaces, empty line and empty respectively',
            '',
            'Batch starting at line #40:',
            '1)     ',
            '',
            '',
            '',
            'Batch starting at line #45:',
            "1) select 'should work' from dummy;",
            '',
            "'should work'",
            '"should work"',
            '',
            ''
            ]
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + '-strictseparatorline -cgo -V var1 -m -fn -I ' + filePath,
                        inputData="",
                        expected=expected)

    def testStrictSeparatorLineInteractive(self):
        """Test -strictseparatorline in interactive mode"""
        inputData = (
"""
/*
Case1: Not sending comment only but still report in -fn
*/
go
-- Case2: Not sending comment only but still report in -fn
go
/* Case3: Not sending incomplete comment but still report in -fn
go
*/
go
Case4: Sending this and expects server error
go
Case5: Sending the next go with leading space and expects server error
 go
go
Case6: Sending the next go with trailing space and expects server error
go 
go
Case7: Sending the next Go with capital and expects server error
Go
go
Case8: Sending lone quote ' and expects server error
go
Case9: Sending lone double quote " and expects server error
go
Case10: Sending lone begin and expects server error
go
-- Case11: A normal batch
create procedure testProc()
as
begin
    select 1 from dummy;
end
go
/* Case12: Another normal batch */
drop procedure testProc;
-- The next 3 cases are batches with 4 spaces, empty line and empty respectively
go
    
go

go
go
define variables var1='should work'
select '&var1' from dummy;
go
""")
        expected = [
            '1) /*',
            '2) Case1: Not sending comment only but still report in -fn',
            '3) */',
            '',
            '1) -- Case2: Not sending comment only but still report in -fn',
            '',
            '1) /* Case3: Not sending incomplete comment but still report in -fn',
            '2) go',
            '3) */',
            '',
            '1) Case4: Sending this and expects server error',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case4": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            '1) Case5: Sending the next go with leading space and expects server error',
            '2)  go',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case5": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            '1) Case6: Sending the next go with trailing space and expects server error',
            '2) go ',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case6": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            '1) Case7: Sending the next Go with capital and expects server error',
            '2) Go',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case7": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            "1) Case8: Sending lone quote ' and expects server error",
            '',
            '* 257: sql syntax error: incorrect syntax near "Case8": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            '1) Case9: Sending lone double quote " and expects server error',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case9": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            '1) Case10: Sending lone begin and expects server error',
            '',
            '* 257: sql syntax error: incorrect syntax near "Case10": line 1 col 1 (at pos 1) SQLSTATE: HY000',
            '1) -- Case11: A normal batch',
            '2) create procedure testProc()',
            '3) as',
            '4) begin',
            '5)     select 1 from dummy;',
            '6) end',
            '',
            '1) /* Case12: Another normal batch */',
            '2) drop procedure testProc;',
            '3) -- The next 3 cases are batches with 4 spaces, empty line and empty respectively',
            '',
            '1)     ',
            '',
            '',
            '',
            "1) select 'should work' from dummy;",
            '',
            "'should work'",
            '"should work"',
            ''
            ]
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + ' -x -fn -j -quiet -strictseparatorline -m -cgo -V var1',
                        inputData=inputData,
                        expected=expected)

    def testInternalOptionOutputFileMultiline(self):
        """Check internal option \o for an output file"""

        if sys.platform == 'win32':
            print "test is not supported on this platform"
            return

        outfile1 = os.path.join(self.__tempDirectory, "out put.sql")
        outfile2 = os.path.join(self.__tempDirectory, "output.sql")


        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   'Multiline mode switched ON',
                   '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                   '',
                   '']

        # short option and non-quoted output file
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="\mu\n\o " + outfile1 + "\nselect '\o' from dummy\g\n ",
                        expected=expected)
        self.assertTrue(os.path.isfile(outfile1), "Output file " + outfile1 + " not created")
        self.assertTrue(os.path.getsize(outfile1) > 0, "Output file " + outfile2 + " empty")

        # long option and quoted output file
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData='\mu\n\output "' + outfile2 + '"\nselect \'\output\' from dummy\g\n ',
                        expected=expected)
        self.assertTrue(os.path.isfile(outfile2), "Output file " + outfile2 + " not created")
        self.assertTrue(os.path.getsize(outfile2) > 0, "Output file " + outfile2 + " empty")

    def testTraceOption(self):
        """check hdbsql -T option"""

        traceFileName = os.path.join(self.__tempDirectory, "testtrace.trc")

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -T " + traceFileName,
                        exitcode=0, inputData="SELECT * FROM DUMMY\n")

        self.assertTrue(os.path.isfile(traceFileName), "Trace file " + traceFileName + " not created")

    def testWrongResultSetEncoding(self):
        """test for wrong usage of -resultsetencoding option"""
        expected = ["Invalid result encoding 'HOSSA'",
                   '',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding HOSSA",
                        inputData="\q\n",
                        expected=expected)

    def testDefaultCharacterOutput(self):
        """check output for varchar/nvarchar data"""
        self.dropTable("ZZTCOUT")
        c = self.conn.cursor()
        c.execute("create table zztcout(k integer primary key, v1 varchar(100), v2 nvarchar(100))")
        c.execute(u"insert into zztcout values(1, 'Petr\u00f3leo Brasleiro S.A.', 'Petr\u00f3leo Brasleiro S.A.')")
        self.conn.commit()
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'K,V1,V2',
                  '1,"Petr\xc3\xb3leo Brasleiro S.A.","Petr\xc3\xb3leo Brasleiro S.A."',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="select * from zztcout\n",
                        expected=expected)
        c.execute("drop table zztcout")

    def testNoTotalIndicatorOutput(self):
        """check correct output when indicator is NO_TOTAL"""
        self.dropTable("COMPOUNDSEARCH")
        c = self.conn.cursor()
        c.execute("CREATE COLUMN TABLE compoundSearch(T NCLOB)")
        c.execute("insert into compoundSearch values('kokakola erfrischungsgetraenke kokakola')")
        self.conn.commit()
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'T',
                  '"kokakola erfrischungsgetraenke k"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="select T from compoundSearch\n",
                        expected=expected)
        c.execute("drop table compoundSearch")


    def testSporadicCrash (self):
        """check for sporadic crash reproducable with \s command"""

        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'host          : [a-zA-Z0-9._-]+:3\d\d\d\d',
                  'sid           : \w\w\w',
                  'dbname        : \w+',
                  'user          : \w+',
                  'kernel version:\s+' + self.helper.KERNEL_VERSION_PATTERN,
                  'SQLDBC version:\s+' + self.helper.SQLDBC_VERSION_PATTERN,
                  'autocommit    : ON',
                  'locale        : .+',
                  'input encoding: \w+',
                  '',
                  'DUMMY',
                  '"X"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="\\s\nselect * from dummy\n\\q\n",
                        expected=expected)

    def testLatin1CharacterOutput(self):
        """check output for varchar/nvarchar data and LATIN1 result encoding"""
        self.dropTable("ZZTCOUT")
        c = self.conn.cursor()
        c.execute("create table zztcout(k integer primary key, v1 varchar(100), v2 nvarchar(100))")
        c.execute(u"insert into zztcout values(1, 'Petr\u00f3leo Brasleiro S.A.', 'Petr\u00f3leo Brasleiro S.A.')")
        self.conn.commit()
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'K,V1,V2',
                  '1,"Petr\xf3leo Brasleiro S.A.","Petr\xf3leo Brasleiro S.A."',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding LATIN1",
                        inputData="select * from zztcout\n",
                        expected=expected)
        c.execute("drop table zztcout")

    def testTooManySpacesForNullValues(self):
        """check output for null values"""
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'NULL,HOMER',
                  '?,"HOMER"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding LATIN1",
                        inputData="select NULL,'HOMER' as HOMER from dummy\n",
                        expected=expected)

    def testUCS2BEFromFile(self):
        """check execution of UCS2 BE script with BOM marker"""
        self.dropTable("\"\xe4\xfc\xf6\"")

        expected = [
                     '',
                     'Welcome to the SAP HANA Database interactive terminal.',
                     '                                           ',
                     'Type:  \\h for help with commands          ',
                     '       \\q to quit                         ',
                     '',
                     '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                     '',
                    '1 row affected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    '\xc3\x84',
                    '"\xc3\xbc\xc3\xa4\xc3\xb6"',
                    '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    ''
                  ]

        inputFile = self.helper.getInputFile("testHdbsqlUCS2BE.sql")
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding UTF8",
                        inputData="\i " + inputFile + "\n",
                        expected=expected)

    def testUCS2LEFromFile(self):
        """check execution of UCS2 LE script with BOM marker"""
        self.dropTable("\"\xe4\xfc\xf6\"")

        expected = [
                     '',
                     'Welcome to the SAP HANA Database interactive terminal.',
                     '                                           ',
                     'Type:  \\h for help with commands          ',
                     '       \\q to quit                         ',
                     '',
                     '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                     '',
                    '1 row affected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    '\xc3\x84',
                    '"\xc3\xbc\xc3\xa4\xc3\xb6"',
                    '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    ''
                  ]

        inputFile = self.helper.getInputFile("testHdbsqlUCS2LE.sql")
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding UTF8",
                        inputData="\i " + inputFile + "\n",
                        expected=expected)

    def testUTF8BOMFromFile(self):
        """check execution of UTF8 script with BOM marker"""
        self.dropTable("\"\xe4\xfc\xf6\"")

        expected = [
                     '',
                     'Welcome to the SAP HANA Database interactive terminal.',
                     '                                           ',
                     'Type:  \\h for help with commands          ',
                     '       \\q to quit                         ',
                     '',
                     '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                     '',
                    '1 row affected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    '\xc3\x84',
                    '"\xc3\xbc\xc3\xa4\xc3\xb6"',
                    '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    ''
                  ]

        inputFile = self.helper.getInputFile("testHdbsqlUTF8bom.sql")
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding UTF8",
                        inputData="\i " + inputFile + "\n",
                        expected=expected)

    def testUtf8FromFile(self):
        """check execution of UTF8 script"""
        self.dropTable("\"\xe4\xfc\xf6\"")

        expected = [
                     '',
                     'Welcome to the SAP HANA Database interactive terminal.',
                     '                                           ',
                     'Type:  \\h for help with commands          ',
                     '       \\q to quit                         ',
                     '',
                     '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                     '',
                    '1 row affected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    '\xc3\x84',
                    '"\xc3\xbc\xc3\xa4\xc3\xb6"',
                    '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    ''
                  ]

        inputFile = self.helper.getInputFile("testHdbsqlUTF8.sql")
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding UTF8",
                        inputData="\ie UTF8\n\i " + inputFile + "\n",
                        expected=expected)

    def testUtf8CharacterInputFromCommandLine(self):
        """check UTF8 input from command line"""
        expected = ['* 257: sql syntax error: incorrect syntax near "a\xc3\x9f": line 1 col 13 (at pos 13) SQLSTATE: HY000',
                     ''
                     ]
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + "\"Create user 'a\xc3\x9f' password Abcd1234\"",
                        inputData="",
                        expected=expected,
                        exitcode=1)

    def testUtf8CharacterOutput(self):
        """check output for varchar/nvarchar data and UTF8 result encoding"""
        self.dropTable("ZZTCOUT")
        c = self.conn.cursor()
        c.execute("create table zztcout(k integer primary key, v1 varchar(100), v2 nvarchar(100))")
        c.execute(u"insert into zztcout values(1, 'Petr\u00f3leo Brasleiro S.A.', 'Petr\u00f3leo Brasleiro S.A.')")
        self.conn.commit()
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'K,V1,V2',
                  '1,"Petr\xc3\xb3leo Brasleiro S.A.","Petr\xc3\xb3leo Brasleiro S.A."',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding UTF8",
                        inputData="select * from zztcout\n",
                        expected=expected)
        c.execute("drop table zztcout")

    def testMultiByteUtf8CharacterOutput(self):
        """check output for varchar/nvarchar data with 100 UTF-8 encoded characters """
        self.dropTable("ZZTCOUT")

        hundredDigitsUnicode = u"\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070\u00B9\u00B2\u00B3\u2074\u2075\u2076\u2077\u2078\u2079\u2070"
        hundredDigitsUTF8 = "\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0\xc2\xb9\xc2\xb2\xc2\xb3\xe2\x81\xb4\xe2\x81\xb5\xe2\x81\xb6\xe2\x81\xb7\xe2\x81\xb8\xe2\x81\xb9\xe2\x81\xb0"

        c = self.conn.cursor()
        c.execute("CREATE COLUMN TABLE zztcout (\"KEY\" INTEGER CS_INT NOT NULL , \"VAL\" NVARCHAR(100), PRIMARY KEY (\"KEY\"))")
        c.execute(u"INSERT INTO zztcout VALUES(1,'" + hundredDigitsUnicode + "')");
        c.execute("INSERT INTO zztcout VALUES(2,'1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890')");

        self.conn.commit()
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'KEY,VAL',
                  '1,"' + hundredDigitsUTF8 + '"',
                  '2,"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"',
                  '2 rows selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -resultencoding UTF8",
                        inputData="select * from zztcout\n",
                        expected=expected)
        c.execute("drop table zztcout")

    def testAlignedCharacterOutput(self):
        """check output for varchar/nvarchar data (aligned)"""
        for _ in ("", "-resultencoding LATIN1", "-resultencoding UTF8"):
            self.dropTable("ZZTCOUT")

            c = self.conn.cursor()
            c.execute("create table zztcout(k integer primary key, v1 varchar(100), v2 nvarchar(100))")
            c.execute(u"insert into zztcout values(1, 'Petr\u00f3leo Brasleiro S.A.', 'Petr\u00f3leo Brasleiro S.A.')")
            self.conn.commit()
            expected = ['',
                       'Welcome to the SAP HANA Database interactive terminal.',
                       '                                           ',
                       'Type:  \\h for help with commands          ',
                       '       \\q to quit                         ',
                       '',
                       'Aligned output mode switched ON',
                       '| K           | V1                      | V2                      |',
                       '| ----------- | ----------------------- | ----------------------- |',
                       '|           1 | Petr\xc3\xb3leo Brasleiro S.A. | Petr\xc3\xb3leo Brasleiro S.A. |',
                       '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                       '',
                       '']
            self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                            inputData="\\al\nselect * from zztcout\n",
                            expected=expected)
            c.execute("drop table zztcout")

    def testAlignedMultiByteUtf8CharacterOutput(self):
        """check output for varchar/nvarchar data (aligned) with 100 UTF-8 encoded characters"""
        for _ in ("", "-resultencoding LATIN1", "-resultencoding UTF8"):
            self.dropTable("ZZTCOUT")

            hundredDigitsUnicode = u"\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19\uFF10\uFF11\uFF12\uFF13\uFF14\uFF15\uFF16\uFF17\uFF18\uFF19"
            hundredDigitsUTF8 = hundredDigitsUnicode.encode("utf-8")

            c = self.conn.cursor()
            c.execute("CREATE COLUMN TABLE zztcout (\"KEY\" INTEGER CS_INT NOT NULL , \"VAL\" NVARCHAR(100), PRIMARY KEY (\"KEY\"))")
            c.execute(u"INSERT INTO zztcout VALUES(1,'" + hundredDigitsUnicode + "')");
            c.execute("INSERT INTO zztcout VALUES(2,'1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890')");

            self.conn.commit()
            expected = ['',
                       'Welcome to the SAP HANA Database interactive terminal.',
                       '                                           ',
                       'Type:  \\h for help with commands          ',
                       '       \\q to quit                         ',
                       '',
                       'Aligned output mode switched ON',
                       '| KEY         | VAL                                                                                                  |',
                       '| ----------- | ---------------------------------------------------------------------------------------------------- |',
                       '|           1 | ' + hundredDigitsUTF8 + ' |',
                       '|           2 | 1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890 |',
                       '2 rows selected ' + self.helper.PROCESSING_TIME_PATTERN,
                       '',
                       '']
            self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                            inputData="\\al\nselect * from zztcout\n",
                            expected=expected)
            c.execute("drop table zztcout")

    def testAlignedOutputNullValuesAreDisplayedCorrectly(self):
        try:
            filepath = os.path.join(self.__tempDirectory, "testAlignedOutputNullValues.txt")
            with open(filepath, "w") as inputFile:
                inputFile.write(
                                """
                                do begin
                                    select NULL as Z, substring(NULL,1) as A, length(substring(NULL,1)) as B from dummy;
                                end
                                """
                                )
            expected = [
                            '| Z | A | B           |',
                            '| - | - | ----------- |',
                            '| ? | ? | ?           |',
                            ''
                       ]

            self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -quiet -A -j -I " + filepath, expected=expected)

            with open(filepath, "w") as inputFile:
                inputFile.write(
                                """
                                do begin
                                    declare b blob;
                                    select NULL as Z, substring(b,0,1) as A, length(substring(b,0,1)) as B from dummy --Same as #1
                                        union all
                                    select NULL as Z, substring(NULL,1) as A, length(substring(NULL,1)) as B from dummy; --Same as #2
                                end
                                """
                                )
            expected = [
                            '| Z | A | B                    |',
                            '| - | - | -------------------- |',
                            '| ? | ? | ?                    |',
                            '| ? | ? | ?                    |',
                            ''
                       ]

            self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -quiet -A -j -I " + filepath, expected=expected)

            with open(filepath, "w") as inputFile:
                inputFile.write(
                                """
                                do begin
                                    declare b blob;
                                    select NULL as Z, substring(b,0,1) as A, length(substring(b,0,1)) as B from dummy --Same as #1
                                        union all
                                    select NULL as Z, substring(NULL,1) as A, length(substring(NULL,1)) as B from dummy; --Same as #2
                                end
                                """
                                )
            expected = [
                            '| Z | A | B                    |',
                            '| - | - | -------------------- |',
                            '| ? | ? | ?                    |',
                            '| ? | ? | ?                    |',
                            ''
                       ]

            self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -quiet -A -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testB1TestRegex(self):
        """Check hdbsql output as needed in b1a.py"""
        self.dropTable("ZZB1ATEST")
        self.conn.commit();
        (_, out) = self.helper.callHdbsql(inputData="\nCREATE TABLE ZZB1ATEST (A INTEGER PRIMARY KEY)\n",
                                    exitcode=0);
        pattern = re.compile(self.helper.PROCESSING_TIME_PATTERN)
        match = pattern.search(out)
        if not match:
            self.fail("expected b1a regex did not match, check and update b1a.py")

    def testWarningWhenPasswordExpired(self):
        """check warning handling when password is expired"""
        c = self.conn.cursor()
        try:
            c.execute("drop user pw_user9")
        except:
            pass
        c.execute("create user pw_user9 password elke1Ute")

        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('password policy','password_parameter_just_for_test' ) = '0' with reconfigure")
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('password policy','maximum_password_lifetime' ) = '182' with reconfigure")
        c.execute("ALTER SYSTEM ALTER CONFIGURATION ('indexserver.ini','SYSTEM') set ('authentication','last_successful_connect_update_interval' ) = '0' with reconfigure")

        conn1 = self.conman.createConnection(user='pw_user9', password='elke1Ute')
        c1 = conn1.cursor()
        c1.execute("alter user pw_user9 password Abcd1234")
        conn1.commit()
        self.conman.closeConnection(conn1)

        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('password policy','password_parameter_just_for_test' ) =  '5' with reconfigure")
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('password policy','maximum_password_lifetime' ) =  '15' with reconfigure")

        self.conn.commit()
        expected = [
                    '',
                    'Welcome to the SAP HANA Database interactive terminal.',
                    '                                           ',
                    'Type:  \\h for help with commands          ',
                    '       \\q to quit                         ',
                    '',
                    "Warning:\s.\s431:\suser's\spassword\swill\sexpire\swithin\sfew\sdays:\sexpire\stime\s\(UTC\)\sat\s\[\d+\-\d\d\-\d\d\s\d\d\:\d\d\:\d\d\.\d+\]\sSQLSTATE\:\s01010",
                    'DUMMY',
                    '"X"',
                    '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    ''
                    ]
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters("pw_user9", "Abcd1234"),
                        exitcode=0,
                        inputData="select * from dummy\n",
                        expected=expected)

        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('password policy','password_parameter_just_for_test' ) = '0' with reconfigure");
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('password policy','maximum_password_lifetime' ) = '182' with reconfigure");
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') set ('authentication','last_successful_connect_update_interval' ) = '5' with reconfigure")
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') unset ('password policy','password_parameter_just_for_test' ) with reconfigure")
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') unset ('password policy','maximum_password_lifetime' ) with reconfigure")
        c.execute("ALTER SYSTEM ALTER CONFIGURATION  ('indexserver.ini','SYSTEM') unset ('authentication','last_successful_connect_update_interval' ) with reconfigure")

        c.execute("drop user pw_user9")

    def testUnalignedVarbinaryOutput(self):
        """check output for varbinary data (unaligned)"""
        c = self.conn.cursor()
        try:
            c.execute("drop table varbinarydata")
        except:
            pass
        c.execute("create table varbinarydata (GUID VARBINARY(16) CS_RAW NOT NULL, PRIMARY KEY (GUID))")
        c.execute("insert into varbinarydata values(x'00199960EAF11ED09186395C09DC2360')")
        self.conn.commit()

        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'GUID',
                  '0x00199960EAF11ED09186395C09DC2360',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="SELECT * from varbinarydata WHERE GUID = '00199960EAF11ED09186395C09DC2360'\n",
                        expected=expected)
        c.execute("drop table varbinarydata")

    def testVarbinaryOutput(self):
        """check output for varbinary data (unaligned)"""
        c = self.conn.cursor()
        try:
            c.execute("drop table varbinarydata")
        except:
            pass
        c.execute("create table varbinarydata (GUID VARBINARY(16) CS_RAW NOT NULL, PRIMARY KEY (GUID))")
        c.execute("insert into varbinarydata values(x'00199960EAF11ED09186395C09DC2360')")
        self.conn.commit()

        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  '| GUID                               |',
                  '| ---------------------------------- |',
                  '| 0x00199960EAF11ED09186395C09DC2360 |',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -A ",
                        inputData="SELECT * from varbinarydata WHERE GUID = '00199960EAF11ED09186395C09DC2360'\n",
                        expected=expected)
        c.execute("drop table varbinarydata")

    def testClientInfo(self):
        """check setting client info"""

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   '| KEY                | VALUE      |',
                   '| ------------------ | ---------- |',
                   '| APPLICATION        | hdbsql     |',
                   '| APPLICATIONUSER    | h11adm     |',
                   '\| PROTOCOL_VERSION   \| 4.1 \([\d], [\d]\) \|',
                   '| XS_APPLICATIONUSER | SYSTEM     |',
                   '| homer              | simpson    |',
                   '| moe                | bar        |',
                   '6 rows selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -A ",
                        inputData="\cl homer=simpson;moe=bar;APPLICATIONUSER=h11adm;APPLICATION=hdbsql\n select KEY,VALUE from m_session_context b, \"PUBLIC\".\"M_CONNECTIONS\" c where c.own='TRUE' and c.connection_id=b.connection_id order by key asc\n",
                        expected=expected)

    def testBug37406(self):
        """hdbsql should not crash when selecting an empty result and using option -A"""

        expected = ['',
                   'Welcome to the SAP HANA Database interactive terminal.',
                   '                                           ',
                   'Type:  \\h for help with commands          ',
                   '       \\q to quit                         ',
                   '',
                   '| DUMMY |',
                   '| ----- |',
                   '0 rows selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -A ",
                        inputData="select * from dummy where 1 = 2\n",
                        expected=expected)

    def testQuiet(self):
        """test hiding the banner via the quiet switch"""
        expected = ['DUMMY',
                  '"X"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + ' -quiet',
                        inputData="select * from dummy\n",
                        expected=expected)

    def testSecureStoreConnect(self):
        hostname, instance, userName, passwd, _ = self.helper.getConnectParameters()

        if subprocess.call(['hdbuserstore', 'SET', str(instance), hostname + ":3" + str(instance) + "15", userName, passwd]) is not 0:
            self.fail("could not store connection details in secure store")

        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'DUMMY',
                  '"X"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline='-U ' + str(instance),
                        inputData="select * from dummy\n",
                        expected=expected)

    def testInternalConnect(self):
        """check internal connect"""
        hostname, _, userName, passwd, port = self.helper.getConnectParameters()

        expected = ['',
         'Welcome to the SAP HANA Database interactive terminal.',
         '                                           ',
         'Type:  \\h for help with commands          ',
         '       \\q to quit                         ',
         '',
         'Connected to \w\w\w@[a-zA-Z0-9._-]*:\d*',
         '']

        self.helper.callHdbsql(commandline='',
                        inputData='\c -n ' + hostname + ':' + str(port) + ' -u ' + userName + ' -p ' + passwd + '\n',
                        expected=expected)

    def testInternalConnectWithBadOptions(self):
        """check rejection of internal connect with bad options"""

        expected = ['',
         'Welcome to the SAP HANA Database interactive terminal.',
         '                                           ',
         'Type:  \\h for help with commands          ',
         '       \\q to quit                         ',
         '',
         'Bad connect options (use -i ##,-n,-d,-u,-p,-U): berl30052166a:30915 -u system -p *******',
         '']

        self.helper.callHdbsql(commandline='',
                        inputData='\c berl30052166a:30915 -u system -p manager\n',
                        expected=expected)
    
    def testInternalConnectViaInstanceID(self):
        """check internal connect using instance id"""
        hostname, instance, userName, passwd, port = self.helper.getConnectParameters()

        expected = ['',
         'Welcome to the SAP HANA Database interactive terminal.',
         '                                           ',
         'Type:  \\h for help with commands          ',
         '       \\q to quit                         ',
         '',
         'Connected to \w\w\w@[a-zA-Z0-9._-]*:\d*',
         '']

        self.helper.callHdbsql(commandline='',
                        inputData='\c -i ' + instance + ' -n ' + hostname + ':' + str(port) + ' -u ' + userName + ' -p ' + passwd + '\n',
                        expected=expected)

    def testControlCharacters(self):
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  '\\a (bell),\\b (backspace),\\t (horizontal tab),\\n (line feed),\\f (form feed),\\r (carriage return),\\r\\n (CRLF)',
                  '"\\a","\\b","\\t","\\n","\\f","\\r","\\r\\n"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="select CHAR(7) as \"\\a (bell)\", CHAR(8) as \"\\b (backspace)\", CHAR(9) as \"\\t (horizontal tab)\", CHAR(10) as \"\\n (line feed)\", CHAR(12) as \"\\f (form feed)\", CHAR(13) as \"\\r (carriage return)\", CHAR(13)||CHAR(10) as \"\\r\\n (CRLF)\" from dummy\n",
                        expected=expected)

    def testBinaryDisplayLimit(self):
        """Checks the display length of binary fields (default 32 bytes)."""
        c = self.conn.cursor()
        try:
           c.execute("DROP TABLE zzbinarylimit")
        except:
           pass
        c.execute("CREATE COLUMN TABLE zzbinarylimit (mybinary varbinary(64))")
        c.execute("INSERT INTO zzbinarylimit VALUES(x'000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F')");

        self.conn.commit()
        expected = ['',
           'Welcome to the SAP HANA Database interactive terminal.',
            '                                           ',
            'Type:  \\h for help with commands          ',
            '       \\q to quit                         ',
            '',
            'MYBINARY',
            '0x000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F',
            '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
            '',
            '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="SELECT mybinary FROM zzbinarylimit\n",
                        expected=expected)

        expected = ['',
           'Welcome to the SAP HANA Database interactive terminal.',
            '                                           ',
            'Type:  \\h for help with commands          ',
            '       \\q to quit                         ',
            '',
            'MYBINARY',
            '0x000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F',
            '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
            '',
            '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + " -b 64",
                        inputData="SELECT mybinary FROM zzbinarylimit\n",
                        expected=expected)

        c.execute("DROP TABLE zzbinarylimit")


    def testLobDisplayLimit(self):
        """Checks if the limited display of a result LOB has equal length after selecting the LOB twice."""
        c = self.conn.cursor()
        try:
            c.execute("DROP TABLE zzloblimit")
        except:
            pass

        c.execute("CREATE COLUMN TABLE zzloblimit (id integer, mylob text)")
        c.execute("INSERT INTO zzloblimit VALUES(1, 'That is a text that is over LOB limit.')");

        self.conn.commit()
        expected = ['',
            'Welcome to the SAP HANA Database interactive terminal.',
            '                                           ',
            'Type:  \\h for help with commands          ',
            '       \\q to quit                         ',
            '',
            'MYLOB,MYLOB',
            '"That is a text that is over LOB ","That is a text that is over LOB "',
            '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
            '',
            '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="SELECT a.mylob, b.mylob FROM zzloblimit a join zzloblimit b on a.id = b.id\n",
                        expected=expected)
        c.execute("DROP TABLE zzloblimit")

    def testExitWhenInputFilePermissionsAreNotSufficientInBatchMode(self):
        """Checks that if hdbsql is executed in batch mode with input file, the program ends (and does not switch to interactive mode) if the file permissions are not sufficient"""
        if sys.platform == 'win32':
            print "test is not supported on this platform"
            return

        filepath = os.path.join(self.__tempDirectory, "hdbsqltest")
        inputFile = open(filepath, "w")
        inputFile.write("select * from dummy")
        inputFile.close()

        os.chmod(filepath, 0)

        commandline = self.helper.getHdbsqlConnectParameters() + "-I " + filepath
        expected = ["Cannot open file " + filepath + ": Permission denied", ""]

        self.helper.callHdbsql(commandline=commandline, expected=expected, exitcode=13)

    def testCommentsInASimpleStatement(self):
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'DUMMY',
                  '"X"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                        inputData="/*comment1*/select * from dummy--comment2\n",
                        expected=expected)

    def testMultilineStatements(self):
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'Multiline mode switched ON',
                  "'1'",
                  '"1"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  "'2'",
                  '"2"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  "'3'",
                  '"3"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                inputData="\mu\nselect \n '1' \n from dummy;\n"
                       "select '2' from dummy \g\n"
                       "select '3' from dummy\n\go\n",
                expected=expected)

    def testCommentsInAMultilineStatement(self):
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'Multiline mode switched ON',
                  "'1'",
                  '"1"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  "'2'",
                  '"2"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                inputData="\mu\n--comment1\n"
                       "select '1' from dummy;\n"
                       "/*comment2*/select '2' from dummy--comment3\g\n",
                expected=expected)

    def testCommentsInFileInput(self):
        if sys.platform == 'win32':
            print "test is not supported on this platform"
            return

        filepath = os.path.join(self.__tempDirectory, "hdbsqltest")
        inputFile = open(filepath, "w")
        inputFile.write(
                   "--comment1\n"
                   "select \n '1' \n from dummy;\n"
                   "/*comment2*/select '2' from dummy --comment3")
        inputFile.close()

        expected = ["'1'",
                    '"1"',
                    "'2'",
                    '"2"',
                    '']

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + "-I " + filepath,
                expected=expected)

    def testEngineWarning(self):
        self.dropTable("WARNINGTESTTABLE")
        self.dropProcedure("WARNINGTEST")
        self.execute("CREATE TABLE WARNINGTESTTABLE (A INT)")
        c = self.conn.cursor()
        c.execute("""create procedure WarningTest (out out1 WarningTestTable)
                LANGUAGE SQLSCRIPT AS
                BEGIN
                 out1 = Select * from WarningTestTable;
                END""")

        # 0 rows affected -> calling this procedure is no query
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'Warning: * 432: this syntax has been deprecated and will be removed in next release: NULL SQLSTATE: HY000',
                  'A',
                  '0 rows selected ' + self.helper.PROCESSING_TIME_PATTERN, 
                  '',
                  '',
                  ]

        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters(),
                exitcode=0,
                inputData="call WarningTest(NULL)\n",
                expected=expected)

    def testDisplayViewsAndDisplayTablesWork(self):
        "Test that \dv and \dt work (See bug 101667 for regression)"
        filepathInput = os.path.join(self.__tempDirectory, "hdbsqltestInput.txt")
        filepathOutput = os.path.join(self.__tempDirectory, "hdbsqltestOutput.txt")
        inputFile = open(filepathInput, "w")
        inputFile.write('\\dv\n')
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -I "
        commandLine += filepathInput
        commandLine += " -o "
        commandLine += filepathOutput
        try:
            process = self.helper.callHdbsql(commandline=commandLine)
        except:
            os.remove(filepathInput)
            os.remove(filepathOutput)
            raise Exception("Program crashed when \dv was specified")
        os.remove(filepathInput)
        os.remove(filepathOutput)
        inputFile = open(filepathInput, "w")
        inputFile.write('\\dt\n')
        inputFile.close()
        try:
            process = self.helper.callHdbsql(commandline=commandLine)
        except:
            os.remove(filepathInput)
            os.remove(filepathOutput)
            raise Exception("Program crashed when \dt was specified")
        os.remove(filepathInput)
        os.remove(filepathOutput)

    def testOutputLineNumberingForBatchFile(self):
        """Test that output line numbering works for batch input file"""
        filepathInput = os.path.join(self.__tempDirectory, "hdbsqltestInput.txt")
        self.dropTable("HDBSQLLINENUMBERING")
        inputFile = open(filepathInput, "w")
        inputFile.write("create table hdbsqllinenumbering(a integer);\n\n\n"
                        "insert into hdbsqllinenumbering values(1);\n"
                        "select a\n\n"
                        "from hdbsqllinenumbering;\n"
                        "select a from hdbsqllinenumbering;")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -fn "
        commandLine += " -j "
        commandLine += " -I "
        commandLine += filepathInput
        expected = [
                        'Batch starting at line #1:',
                        '1) create table hdbsqllinenumbering(a integer)',
                        '',
                        'Batch starting at line #4:',
                        '1) insert into hdbsqllinenumbering values(1)',
                        '',
                        'Batch starting at line #5:',
                        '1) select a',
                        '2) ',
                        '3) from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        'Batch starting at line #8:',
                        '1) select a from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        ''
                    ]
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            self.dropTable("HDBSQLLINENUMBERING")
            os.remove(filepathInput)

    def testOutputLineNumberingForBatchFileWithSeparatorOwnline(self):
        """Test that output line numbering works correctly for batch input file with separatorownline"""
        filepathInput = os.path.join(self.__tempDirectory, "hdbsqltestInput.txt")
        self.dropTable("HDBSQLLINENUMBERING")
        inputFile = open(filepathInput, "w")
        inputFile.write("create table hdbsqllinenumbering(a integer)\n"
                        "go\n"
                        "insert into hdbsqllinenumbering values(1)\n"
                        "go\n"
                        "select a\n"
                        "from hdbsqllinenumbering\n"
                        "go\n"
                        "select a from hdbsqllinenumbering\n"
                        "go\n"
                        "select a from hdbsqllinenumbering\n"
                        "go\n"
                        "select a from hdbsqllinenumbering\n"
                        "go\n")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -fn -j -c go -separatorownline -I "
        commandLine += filepathInput
        expected =  [
                        'Batch starting at line #1:',
                        '1) create table hdbsqllinenumbering(a integer)',
                        '',
                        'Batch starting at line #3:',
                        '1) insert into hdbsqllinenumbering values(1)',
                        '',
                        'Batch starting at line #5:',
                        '1) select a',
                        '2) from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        'Batch starting at line #8:',
                        '1) select a from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        'Batch starting at line #10:',
                        '1) select a from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        'Batch starting at line #12:',
                        '1) select a from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        ''
                    ]
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            self.dropTable("HDBSQLLINENUMBERING")

    def testOutputLineNumberingForBatchFileWithBatchReset(self):
        """Test that output line numbering works correctly for batch input file with batch reset"""
        filepathInput = os.path.join(self.__tempDirectory, "hdbsqltestInput.txt")
        self.dropTable("HDBSQLLINENUMBERING")
        inputFile = open(filepathInput, "w")
        inputFile.write("create table hdbsqllinenumbering(a integer)\n"
                        "go\n"
                        "insert into hdbsqllinenumbering values(1)\n"
                        "go\n"
                        "select a\n"
                        "from hdbsqllinenumbering\n"
                        "reset\n"
                        "select a from hdbsqllinenumbering\n"
                        "go\n"
                        "select a from hdbsqllinenumbering\n"
                        "reset\n"
                        "select a from hdbsqllinenumbering\n"
                        "go\n")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -fn -j -c go -br reset -I "
        commandLine += filepathInput
        expected =  [
                        'Batch starting at line #1:',
                        '1) create table hdbsqllinenumbering(a integer)',
                        '',
                        'Batch starting at line #3:',
                        '1) insert into hdbsqllinenumbering values(1)',
                        '',
                        'Batch starting at line #8:',
                        '1) select a from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        'Batch starting at line #12:',
                        '1) select a from hdbsqllinenumbering',
                        '',
                        'A',
                        '1',
                        ''
                    ]
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            self.dropTable("HDBSQLLINENUMBERING")

    def testOutputLineNumberingForInteractiveInput(self):
        """Test that output line numbering works for interactive input"""
        self.dropTable("HDBSQLLINENUMBERING2")
        inputData = ('\mu on\n'
                     'create table\n'
                     'hdbsqllinenumbering2(a integer);\n\n\n'
                     'insert into hdbsqllinenumbering2 values(1);\n'
                     'select a\n\n'
                     'from hdbsqllinenumbering2;\n')
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -fn "
        commandLine += " -j "
        commandLine += " -quiet"
        expected = [
                        'Multiline mode switched ON',
                        '1) create table',
                        '2) hdbsqllinenumbering2(a integer)',
                        '',
                        '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                        '',
                        '1) insert into hdbsqllinenumbering2 values(1)',
                        '',
                        '1 row affected ' + self.helper.PROCESSING_TIME_PATTERN,
                        '',
                        '1) select a',
                        '2) ',
                        '3) from hdbsqllinenumbering2',
                        '',
                        'A',
                        '1',
                        '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                        '',
                        ''
                   ]
        try:
            process = self.helper.callHdbsql(commandline=commandLine, inputData=inputData, expected=expected)
        finally:
            self.dropTable("HDBSQLLINENUMBERING2")

    def testSQLErrorCodeAsExitCode(self):
        commandLine = self.helper.getHdbsqlConnectParameters()
        if sys.platform == "win32":
            exitcode = 259 # SQL Code 259 Invalid Table Name
        else:
            exitcode = 3 # 259 -> uint8_t
        self.helper.callHdbsql(commandline=commandLine,
                exitcode=exitcode,
                inputData="SELECT * FROM testSQLErrorCodeAsExitCodeDoesntExist\n")

    def testCustomExitCodeOnError(self):
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -E 123 "
        self.helper.callHdbsql(commandline=commandLine,
                exitcode=123,
                inputData="SELECT * FROM testCustomExitCodeOnErrorDoesntExist\n")

    def testSQLExitCodeOnError(self):
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -E STRING_WITHOUT_DIGITS "
        if sys.platform == "win32":
            exitcode = 259 # SQL Code 259 Invalid Table Name
        else:
            exitcode = 3 # 259 -> uint8_t
        self.helper.callHdbsql(commandline=commandLine,
                exitcode=exitcode,
                inputData="SELECT * FROM testCustomExitCodeOnErrorDoesntExist\n")

    def testStopOnError(self):
        filepath = os.path.join(self.__tempDirectory, "testStopOnError.txt")
        inputFile = open(filepath, "w")
        inputFile.write("select * from testCustomExitCodeOnErrorDoesntExist;\n"
                        "select * from dummy;\n")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -E STRING_WITHOUT_DIGITS -I " + filepath
        if sys.platform == "win32":
            exitcode = 259 # SQL Code 259 Invalid Table Name
        else:
            exitcode = 3 # 259 -> uint8_t
        try:
            process = self.helper.callHdbsql(commandline=commandLine, exitcode=exitcode)
        finally:
            os.remove(filepath)

    def testDoNotStopOnError(self):
        filepath = os.path.join(self.__tempDirectory, "testDoNotStopOnError.txt")
        inputFile = open(filepath, "w")
        inputFile.write("select * from testCustomExitCodeOnErrorDoesntExist;\n"
                        "select * from dummy;\n")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -I " + filepath
        expected = ['.*invalid table name.*', 'DUMMY', '"X"', '']
        try:
            process = self.helper.callHdbsql(commandline=commandLine, exitcode=0, expected=expected)
        finally:
            os.remove(filepath)

    def testBatchResetCommandForFileInput(self):
        filepath = os.path.join(self.__tempDirectory, "hdbsqltest")
        inputFile = open(filepath, "w")
        inputFile.write("select * from invalid\n"
                        "\\reset\n"
                        "select * from dummy\n"
                        ";\n")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += ' -br "\\reset" -separatorownline -f -j -I ' + filepath
        expected = ['select * from dummy', '', '', 'DUMMY', '"X"', '']
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            os.remove(filepath)

    def testBatchFileContainingOnlyCommentDoestNotSubmitToServer(self):
        """Test that a batch file with only comments does not submit anything to the server"""
        filepathInput = os.path.join(self.__tempDirectory, "hdbsqltestBatchInputOnlyComment.txt")
        self.dropTable("HDBSQLLINENUMBERING")
        inputFile = open(filepathInput, "w")
        inputFile.write("-- select * from dummy\n"
                        "/*select * from dummy;*/")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -fn -j -I "
        commandLine += filepathInput
        expected =  [
                        ""
                    ]
        process = self.helper.callHdbsql(commandline=commandLine, expected=expected)

    def testBatchFileWithCommentContainingSeparatorDoestNotSubmitPartialCommentToServer(self):
        """Test that a batch file with comment containing the separator does not submit the comment up to the separator"""
        filepathInput = os.path.join(self.__tempDirectory, "hdbsqltestBatchInputCommentWithSeparator.txt")
        inputFile = open(filepathInput, "w")
        inputFile.write("-- select * ; from dummy;\n"
                        "/*select * ; from dummy*/\n"
                        "select * from dummy")
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -f -j -I "
        commandLine += filepathInput
        expected =  [
                        '-- select * ; from dummy;',
                        '/*select * ; from dummy*/',
                        'select * from dummy',
                        '',
                        'DUMMY',
                        '"X"',
                        ''
                    ]
        process = self.helper.callHdbsql(commandline=commandLine, expected=expected)

    def testInteractiveModeWithCommentContainingSeparatorDoestNotSubmitPartialCommentToServer(self):
        """Test that interactive input with comment containing the separator does not submit the comment up to the separator"""
        inputData = ("-- select * ; from dummy;\n"
                     "/*select * ; from dummy*/\n"
                     "select * from dummy\n"
                     ";\n")
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -f -j -quiet -separatorownline -multilinemode"
        expected =  [
                        '-- select * ; from dummy;',
                        '/*select * ; from dummy*/',
                        'select * from dummy',
                        '',
                        '',
                        'DUMMY',
                        '"X"',
                        '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                        '',
                        ''
                    ]
        process = self.helper.callHdbsql(commandline=commandLine, inputData=inputData, expected=expected)

    def testNoLoginCrentialsWithBatchFileDoesNotCrash(self):
        """Test that specification of a batch file (-I <file>) without specifying login credentials does not cause a crash"""
        filepath = os.path.join(self.__tempDirectory, "testNoLoginCrentialsWithBatchFile.txt")
        inputFile = open(filepath, "w")
        inputFile.write("select * from dummy")
        inputFile.close()
        # Specify commandline: hdbsql -I <filepath>
        hostname, instance = self.helper.getConnectParameters()[:2]
        if self._multiDBInstance:
            commandLine = "-n " + hostname + ":3{}13".format(str(instance)) + " -d SYSTEMDB -I " + filepath
        else:
            commandLine = "-n " + hostname + ":3{}15".format(str(instance)) + " -I " + filepath
        expected =  [
                        'Single Sign-On authentication failed',
                        ''
                    ]
        # Check for the output
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            os.remove(filepath)

    def testInDoubleQuote(self):
        """Test comment start and single quote start in double quote"""
        filepath = os.path.join(self.__tempDirectory, "testInDoubleQuote.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            """create table "A'" (B int);\n"""
            """create table "C" ("D/*notcomment" int);\n"""
            """create table "E--notcomment" (F int);\n"""
            """select * from "A'", "C", "E--notcomment";\n"""
            """drop table "A'";\n"""
            """drop table "C";\n"""
            """drop table "E--notcomment";\n"""
        )
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -fn -j -I "
        commandLine += filepath
        expected =  [
            """Batch starting at line #1:""",
            """1) create table "A'" (B int)""",
            '',
            """Batch starting at line #2:""",
            """1) create table "C" ("D/*notcomment" int)""",
            '',
            """Batch starting at line #3:""",
            """1) create table "E--notcomment" (F int)""",
            '',
            """Batch starting at line #4:""",
            '''1) select * from "A'", "C", "E--notcomment"''',
            '',
            """B,D/*notcomment,F""",
            """Batch starting at line #5:""",
            '''1) drop table "A'"''',
            '',
            """Batch starting at line #6:""",
            '''1) drop table "C"''',
            '',
            """Batch starting at line #7:""",
            '''1) drop table "E--notcomment"''',
            '',
            ''
                    ]
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            os.remove(filepath)

    def testBeginEndInFile(self):
        """Test begin and end in sql file"""
        c = self.conn.cursor()
        try:
            c.execute('drop table "begin"')
        except:
            pass
        try:
            c.execute('drop table "end"')
        except:
            pass
        try:
            c.execute('drop procedure "populate end"')
        except:
            pass
        filepath = os.path.join(self.__tempDirectory, "testBeginEndInFile.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            """define variables var1='works'\n"""
            """create table "begin" ("begin" nvarchar(100));\n"""
            """insert into "begin" values('begin a');\n"""
            """insert into "begin" values('begin b');\n"""
            """insert into "begin" values('begin c');\n"""
            """create procedure "populate end"\n"""
            """language sqlscript as\n"""
            """begin\n"""
            """    -- comment1 end\n"""
            """    declare text nvarchar(1000); -- comment2 end\n"""
            """    /*comment3 end*/ declare /* comment4 end */ cursor c_cursor for\n"""
            """    select /*comment6 end*/ "begin" from "begin";--comment7 end\n"""
            """    create /* multi line comment\n"""
            """    not a real end\n"""
            """    end\n"""
            """    not a real end either */\n"""
            """    column\n"""
            """    table "end" ("end" nvarchar(100));\n"""
            """\n"""
            """    for cur_row as c_cursor do\n"""
            """        text := 'insert into "end" values( ''' || cur_row."begin" || ' end &var1' || ''');';\n"""
            """        exec(:text);\n"""
            """    end for;\n"""
            """\n"""
            """    begin\n"""
            """        insert into "end" values( 'nested begin end &var1');\n"""
            """\n"""
            """        begin\n"""
            """            insert into "end" values(\n"""
            """                'nested\n"""
            """                nested\n"""
            """                multiline\n"""
            """                end\n"""
            """                &var1');\n"""
            """        end;\n"""
            """    end;\n"""
            """    if 1=1 then\n"""
            """        drop table "begin";\n"""
            """    end if;\n"""
            """end;\n"""
            """call "populate end";\n"""
            """;\n"""
            """select "end" from "end" order by "end" asc;\n"""
            '''drop table "end";\n'''
            '''drop procedure "populate end";\n'''
            """create procedure _begin()\n"""
            """as\n"""
            """begin\n"""
            """end;\n"""
            """drop procedure _begin;\n"""
            """create procedure end_()\n"""
            """as\n"""
            """begin\n"""
            """end;\n"""
            """drop procedure end_;\n"""
            """create table xyz(beginsequence int, endsequence int);\n"""
            """COMMENT ON COLUMN xyz.beginsequence IS 'Start of the sequence for the meter reader.';\n"""
            """COMMENT ON COLUMN xyz.endsequence IS 'End of sequence for the meter reader.';\n"""
            """drop table xyz;\n"""
            """create table beginxyz(a int);\n"""
            """drop table beginxyz;\n"""
            """create table /*comment*/beginxyz(a int);\n"""
            """drop table beginxyz;\n"""
            """create table NotEnd(c int);\n"""
            """create procedure p()\n"""
            """as\n"""
            """begin\n"""
            """select * from NotEnd;\n"""
            """end;\n"""
            """drop procedure p;\n"""
            """drop table NotEnd;\n"""
        )
        inputFile.close()
        commandLine = self.helper.getHdbsqlConnectParameters()
        commandLine += " -V var1 -fn -j -I "
        commandLine += filepath
        expected =  [
            'Batch starting at line #2:',
            '1) create table "begin" ("begin" nvarchar(100))',
            '',
            'Batch starting at line #3:',
            '1) insert into "begin" values(\'begin a\')',
            '',
            'Batch starting at line #4:',
            '1) insert into "begin" values(\'begin b\')',
            '',
            'Batch starting at line #5:',
            '1) insert into "begin" values(\'begin c\')',
            '',
            'Batch starting at line #6:',
            '1) create procedure "populate end"',
            '2) language sqlscript as',
            '3) begin',
            '4)     -- comment1 end',
            '5)     declare text nvarchar(1000); -- comment2 end',
            '6)     /*comment3 end*/ declare /* comment4 end */ cursor c_cursor for',
            '7)     select /*comment6 end*/ "begin" from "begin";--comment7 end',
            '8)     create /* multi line comment',
            '9)     not a real end',
            '10)     end',
            '11)     not a real end either */',
            '12)     column',
            '13)     table "end" ("end" nvarchar(100));',
            '14) ',
            '15)     for cur_row as c_cursor do',
            '16)         text := \'insert into "end" values( \'\'\' || cur_row."begin" || \' end works\' || \'\'\');\';',
            '17)         exec(:text);',
            '18)     end for;',
            '19) ',
            '20)     begin',
            '21)         insert into "end" values( \'nested begin end works\');',
            '22) ',
            '23)         begin',
            '24)             insert into "end" values(',
            "25)                 'nested",
            '26)                 nested',
            '27)                 multiline',
            '28)                 end',
            "29)                 works');",
            '30)         end;',
            '31)     end;',
            '32)     if 1=1 then',
            '33)         drop table "begin";',
            '34)     end if;',
            '35) end',
            '',
            'Batch starting at line #41:',
            '1) call "populate end"',
            '',
            'Batch starting at line #43:',
            '1) select "end" from "end" order by "end" asc',
            '',
            'end',
            '"begin a end works"',
            '"begin b end works"',
            '"begin c end works"',
            '"nested\\n                nested\\n                multiline\\n                end\\n                works"',
            '"nested begin end works"',
            'Batch starting at line #44:',
            '1) drop table "end"',
            '',
            'Batch starting at line #45:',
            '1) drop procedure "populate end"',
            '',
            'Batch starting at line #46:',
            '1) create procedure _begin()',
            '2) as',
            '3) begin',
            '4) end',
            '',
            'Batch starting at line #50:',
            '1) drop procedure _begin',
            '',
            'Batch starting at line #51:',
            '1) create procedure end_()',
            '2) as',
            '3) begin',
            '4) end',
            '',
            'Batch starting at line #55:',
            '1) drop procedure end_',
            '',
            'Batch starting at line #56:',
            '1) create table xyz(beginsequence int, endsequence int)',
            '',
            'Batch starting at line #57:',
            "1) COMMENT ON COLUMN xyz.beginsequence IS 'Start of the sequence for the meter reader.'",
            '',
            'Batch starting at line #58:',
            "1) COMMENT ON COLUMN xyz.endsequence IS 'End of sequence for the meter reader.'",
            '',
            'Batch starting at line #59:',
            '1) drop table xyz',
            '',
            'Batch starting at line #60:',
            '1) create table beginxyz(a int)',
            '',
            'Batch starting at line #61:',
            '1) drop table beginxyz',
            '',
            'Batch starting at line #62:',
            '1) create table /*comment*/beginxyz(a int)',
            '',
            'Batch starting at line #63:',
            '1) drop table beginxyz',
            '',
            'Batch starting at line #64:',
            '1) create table NotEnd(c int)',
            '',
            'Batch starting at line #65:',
            '1) create procedure p()',
            '2) as',
            '3) begin',
            '4) select * from NotEnd;',
            '5) end',
            '',
            'Batch starting at line #70:',
            '1) drop procedure p',
            '',
            'Batch starting at line #71:',
            '1) drop table NotEnd',
            '',
            ''
        ]
        try:
            process = self.helper.callHdbsql(commandline=commandLine, expected=expected)
        finally:
            self.conn.commit()
            os.remove(filepath)

    def testBeginEndInteractive(self):
        """Test begin and end in interactive mode"""
        c = self.conn.cursor()
        try:
            c.execute('drop procedure "testProcedure"')
        except:
            pass
        try:
            c.execute('drop table "end"')
        except:
            pass
        expected = ['',
                  'Welcome to the SAP HANA Database interactive terminal.',
                  '                                           ',
                  'Type:  \\h for help with commands          ',
                  '       \\q to quit                         ',
                  '',
                  'Multiline mode switched ON',
                  '1) create procedure "testProcedure"',
                  '2) language sqlscript as',
                  '3) begin',
                  '4)     create table "end"("end" nvarchar(100));',
                  '5)     insert into "end" values(\'row1\');',
                  '6) end',
                  '',
                  '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '1) call "testProcedure"',
                  '',
                  '1 row affected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '1) select "end" from "end"',
                  '',
                  'end',
                  '"row1"',
                  '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '1) drop table "end"',
                  '',
                  '0 rows affected ' + self.helper.PROCESSING_TIME_PATTERN,
                  '',
                  '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + ' -fn -j',
                        inputData=
                            """\mu\n"""
                            """create procedure "testProcedure"\n"""
                            """language sqlscript as\n"""
                            """begin\n"""
                            """    create table "end"("end" nvarchar(100));\n"""
                            """    insert into "end" values('row1');\n"""
                            """end;\n"""
                            """call "testProcedure";\n"""
                            """select "end" from "end";\n"""
                            """drop table "end";\n"""
                            """drop procedure "testProcedure";""",
                        expected=expected)
        self.conn.commit()

    def testLineNumberWithDefineVar(self):
        """Test line number in file with define variable"""
        filepath = os.path.join(self.__tempDirectory, "testLineNumberWithDefineVar.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            """define variables var1='2'\n"""
            """select 'line &var1' from dummy;"""
        )
        inputFile.close()
        expected =  [
            '''Batch starting at line #2:''',
            '''1) select 'line 2' from dummy''',
            '',
            """'line 2'""",
            '''"line 2"''',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -V var1 -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testLineNumberInRegularModeWithSeparatorOwnline(self):
        """Test line number in regular mode with separator on is own line but with -separatorownline"""
        filepath = os.path.join(self.__tempDirectory, "testLineNumberRegSep.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            'select 1 from dummy;\n'
            ';\n'
            'select 3 from dummy;\n'
            '\n'
            'select 5 from dummy;\n'
            ' ;\n'
            'select 7 from dummy;\n'
            ';\n'
            ';\n'
            ';\n'
            'select 11 from dummy;\n'
        )
        inputFile.close()
        expected =  [
            'Batch starting at line #1:',
            '1) select 1 from dummy',
            '',
            '1',
            'Batch starting at line #3:',
            '1) select 3 from dummy',
            '',
            '3',
            'Batch starting at line #5:',
            '1) select 5 from dummy',
            '',
            '5',
            'Batch starting at line #7:',
            '1) select 7 from dummy',
            '',
            '7',
            'Batch starting at line #11:',
            '1) select 11 from dummy',
            '',
            '11',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -a -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testRemovalOnlyPureComment(self):
        """Test only batch with pure comment is removed"""
        filepath = os.path.join(self.__tempDirectory, "testRemoveOnlyPureComment.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            "select /*comment1a*/ '1a' from dummy; -- comment1b\n"
            "select '-- comment2a' from dummy; /* comment2b\n"
            "is sent */\n"
            "select 'line4' from dummy; -- Bug not this\n"
            "/* Nor any\n"
            "of this*/\n"
        )
        inputFile.close()
        expected =  [
            'Batch starting at line #1:',
            "1) select /*comment1a*/ '1a' from dummy",
            '',
            '"1a"',
            'Batch starting at line #1:',
            '1)  -- comment1b',
            "2) select '-- comment2a' from dummy",
            '',
            '"-- comment2a"',
            'Batch starting at line #2:',
            '1)  /* comment2b',
            '2) is sent */',
            "3) select 'line4' from dummy",
            '',
            '"line4"',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -a -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testLineNumberMultilineCommentSepCmd(self):
        """Test line numbering when multiline comment followed by separator and then command"""
        filepath = os.path.join(self.__tempDirectory, "testLineNumberMultilineCommentSepCmd.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            "/* Multiline\n"
            "comment */;select 2 from dummy;\n"
            "select 3 from dummy"
        )
        inputFile.close()
        expected =  [
            'Batch starting at line #2:',
            '1) select 2 from dummy',
            '',
            '2',
            'Batch starting at line #3:',
            '1) select 3 from dummy',
            '',
            '3',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -a -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testLineNumberSinglelineCommentSepCmd(self):
        """Test line number when singleline comment followed by separator and then command"""
        filepath = os.path.join(self.__tempDirectory, "testLineNumberSinglelineCommentSepCmd.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            "-- Single line comment\n"
            ";select 2 from dummy;\n"
            "select 3 from dummy"
        )
        inputFile.close()
        expected =  [
            'Batch starting at line #2:',
            '1) select 2 from dummy',
            '',
            '2',
            'Batch starting at line #3:',
            '1) select 3 from dummy',
            '',
            '3',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -a -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testVariableValueWithSeparator(self):
        """Test variable value with separator"""
        filepath = os.path.join(self.__tempDirectory, "testVariableValueWithSeparator.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            """variables on\n"""
            """define variables var1='normal1', var2='with separator ;', var3='normal3'\n"""
            """select '&var1', '&var2', '&var3' from dummy;"""
        )
        inputFile.close()
        expected =  [
            '''Batch starting at line #3:''',
            '''1) select 'normal1', 'with separator ;', 'normal3' from dummy''',
            '',
            '''"normal1","with separator ;","normal3"''',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -a -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    def testVariableValueWithNewline(self):
        """Test variable value with newline"""
        filepath = os.path.join(self.__tempDirectory, "testVariableValueWithNewline.txt")
        inputFile = open(filepath, "w")
        inputFile.write(
            """variables on\n"""
            """define variables var1='normal1', var2='with newline \n', var3='normal3'\n"""
            """select '&var1', '&var2', '&var3' from dummy;"""
        )
        inputFile.close()
        expected =  [
            '''Batch starting at line #3:''',
            '''1) select 'normal1', 'with newline ''',
            '''2) ', 'normal3' from dummy''',
            '',
            '''"normal1","with newline \\n","normal3"''',
            '']
        try:
            process = self.helper.callHdbsql(self.helper.getHdbsqlConnectParameters() + " -a -fn -j -I " + filepath, expected=expected)
        finally:
            os.remove(filepath)

    @classification('no_asan')
    def testAttemptEncryptWarning(self):
        """test receiving a warning when encryption fails"""
        expected = ['',
                    'Welcome to the SAP HANA Database interactive terminal.',
                    '                                           ',
                    'Type:  \\h for help with commands          ',
                    '       \\q to quit                         ',
                    '',
                    '',
                    '==============================================================================',
                    '== SECURITY WARNING: The encrypted communication attempt failed. Retrying   ==',
                    '==                   the connection attempt without encryption options.     ==',
                    '==============================================================================',
                    '',
                    'DUMMY',
                    '"X"',
                    '1 row selected ' + self.helper.PROCESSING_TIME_PATTERN,
                    '',
                    '']
        self.helper.callHdbsql(commandline=self.helper.getHdbsqlConnectParameters() + ' -attemptencrypt -Z sslHostNameInCertificate=baddomain',
                        inputData="select * from dummy\n",
                        expected=expected)

if __name__ == '__main__':
    SqlTestCase.runTest(Hdbsql)

from lib.testHaDR_Utils import testHaDR_Site1, testHaDR_Site2, testHaDR_Site3, dtMode
from lib.haSqlTest import HaTestRealMachines
from lib.sqlTest import classification
import os, time, sys, re, datetime
import threading
from lib.nameserver.nameserverUtil import setHooks, removeHooks
import pdb
import random

DBG = True

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

siteTypes = enum('NONE', 'PRIMARY', 'SECONDARY', 'SECONDARY1')
siteTypeStr = ["", "Primary Site", "Tier 2 Secondary Site", "Tier 3 Secondary Site"]

def printSite(f):
    def wrapper(self, *args, **kwargs):
        print "\n%s  ### Running now on  %s  => %s.%s()" % (time.ctime(), siteTypeStr[self.itsDRRole], self.__class__.__name__, f.__name__)
        return f(self, *args, **kwargs)
    return wrapper

def printTimeConsuming(f):
    def wrapper(self, *args, **kwargs):
        start = datetime.datetime.now()
        ret = f(self, *args, **kwargs)
        end = datetime.datetime.now()
        delta = end - start
        print "\n%s  ### %s takes %s mins %s secs %s days \n" % (time.ctime(), f.__name__, delta.seconds/60, delta.seconds%60, delta.days)
        return ret
    return wrapper

class testHaDR_AdvancedParameters_MultiTier(HaTestRealMachines):
    """Test container for ordinary failover scenarios"""
    #pdb.set_trace()
    # clean test run flag
    testRunSuccess = False
    testTearDownSuccess = False

    site1 = None
    site2 = None
    site3 = None
    isFirstCase = True
    skipRegister = False
    allSetupsDone = False
    hooksSet = False

    # be careful to change the default value, since config method may not specify the para
    defaultAdParas = {
                      'sync_mode':'sync',
                      'sync_mode_1':'async',
                      'op_mode':'logreplay',
                      'op_mode_1':'logreplay',
                      'withInitTenant':True,
                      'withCustomTenant':False,
                      'persistence_encryption':'off',
                      'log_encryption':'off',
                      'data_log_compression':'off',
                      'datashipping_parallel':'off',
                      'ssl':'off',
                      'full_sync':'off',
                      'systempki':'off',
                     }

    def initSites(self):
        print time.ctime(), '----> init sites'
        self.site1 = testHaDR_Site1()
        self.site2 = testHaDR_Site2(landscape = self._SRSelectHosts(1))
        self.site3 = testHaDR_Site3(landscape = self._SRSelectHosts(2))

        self.globalCfg['site1'] = self.site1
        self.globalCfg['site2'] = self.site2
        self.globalCfg['site3'] = self.site3

        # make clean instances
        # set self.globalCfg['withInitTenant']
        self.globalCfg['multiDB'] = self.site1._multiDBInstance
        if self.globalCfg['multiDB']:
            self.site1.setConnectionDatabaseName("SYSTEMDB")
            self.site2.setConnectionDatabaseName("SYSTEMDB")
            self.site3.setConnectionDatabaseName("SYSTEMDB")
            with open('%s/daemon.ini' % os.environ["SAP_RETRIEVAL_PATH"], 'r') as f:
                if 'indexserver.%s' % self.site1.getInstance()["instance_name"] in f.read():
                    self.globalCfg['withInitTenant'] = True
                    self.site1.setConnectionDatabaseName(self.site1.getInstance()["instance_name"])
                    self.site2.setConnectionDatabaseName(self.site2.getInstance()["instance_name"])
                    self.site3.setConnectionDatabaseName(self.site3.getInstance()["instance_name"])
                else:
                    self.globalCfg['withInitTenant'] = False

        if not testHaDR_AdvancedParameters_MultiTier.isFirstCase:
            return

        cleanInstance = True

        # test instance alive
        def checkDatabaseOnline(site):
            dbName=None if self.globalCfg['withInitTenant'] else 'SYSTEMDB'
            conn = site.setUpConnection(site.getHost("WORKER1"), dbname=dbName)

            tryNo = 2
            if conn != None:
                while tryNo > 0:
                    cur = conn.cursor()
                    cur.execute("SELECT * FROM SYS.DUMMY")
                    result = cur.fetchall()
                    if len(result) != 0 and result[0][0] == "X":
                        print time.ctime(), '----> %s is online' % site.getSiteName()
                        return True
                    else:
                        tryNo -= 1
                        print time.ctime(), '----> try to start %s ...' % site.getSiteName()
                        site.startDatabaseLandscapeAsWhole()
                        try:
                            site.waitForDatabaseLandscapeStartedByPY()
                        except:
                            pass
            return False

        if checkDatabaseOnline(self.site1) and checkDatabaseOnline(self.site2) and checkDatabaseOnline(self.site3):
            cleanInstance = False
        else:
            print time.ctime(), '----> not all sites are online.'

        # set cleanInstance if extra tenants exist
        conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
        if conn != None and not cleanInstance:
            cur = conn.cursor()
            cur.execute("SELECT DATABASE_NAME FROM SYS.M_DATABASES_")
            result = cur.fetchall()
            if len(result) != 0:
                for db in result:
                    if db[0] != 'SYSTEMDB' and db[0] != self.site1.getInstance()["instance_name"]:
                        cleanInstance = True
                        break

        # cleanup as necessary
        if testHaDR_AdvancedParameters_MultiTier.isFirstCase and cleanInstance:
            def cleanupAtBeginning(site):
                if site._multiDBInstance:
                    try:
                        para = "--cleanAll --force"
                        if not self.globalCfg['withInitTenant']:
                            para += " --noInitialTenant"
                        site.cleanInstance(site.getHost("WORKER1"), params=para)
                    except Exception as e:
                        print e
                site.startDatabaseLandscapeAsWhole()
                self.waitForDatabaseLandscapeStartedByPY(site)

            t1 = threading.Thread(target=cleanupAtBeginning, args=(self.site1,))
            t2 = threading.Thread(target=cleanupAtBeginning, args=(self.site2,))
            t3 = threading.Thread(target=cleanupAtBeginning, args=(self.site3,))
            t1.start()
            t2.start()
            t3.start()
            t1.join()
            t2.join()
            t3.join()
        # make clean instances end

    @printTimeConsuming
    def advancedParametersSetupBeforeHSR(self, adParas):
        '''these para are set before HSR setup, so HANA not restart even if the para changing needs HANA restarting within HSR
        '''
        defaultAdParas = testHaDR_AdvancedParameters_MultiTier.defaultAdParas
        def setAdPara(paraName):
            return adParas.get(paraName, defaultAdParas[paraName])

        self.globalCfg['sync_mode'] = setAdPara('sync_mode').lower()
        self.globalCfg['sync_mode_1'] = setAdPara('sync_mode_1').lower()
        self.globalCfg['op_mode'] = setAdPara('op_mode').lower()
        self.globalCfg['op_mode_1'] = setAdPara('op_mode_1').lower()

        self.globalCfg['dbname1'] = 'TE1'
        self.globalCfg['dbname2'] = 'TE2'

        self.globalCfg['withCustomTenant'] = setAdPara('withCustomTenant')

        if setAdPara('persistence_encryption').lower() == 'on':
            self.site1.enablePersistenceEncryption()
            self.site2.enablePersistenceEncryption()
            self.site3.enablePersistenceEncryption()
        else:
            self.site1.disablePersistenceEncryption()
            self.site2.disablePersistenceEncryption()
            self.site3.disablePersistenceEncryption()

        if setAdPara('log_encryption').lower() == 'on':
            self.site1.enableLogEncryption()
            self.site2.enableLogEncryption()
            self.site3.enableLogEncryption()
        else:
            self.site1.disableLogEncryption()
            self.site2.disableLogEncryption()
            self.site3.disableLogEncryption()

        if setAdPara('data_log_compression').lower() == 'on':
            self.site1.enableDataLogCompression()
            self.site2.enableDataLogCompression()
            self.site3.enableDataLogCompression()
        else:
            self.site1.disableDataLogCompression()
            self.site2.disableDataLogCompression()
            self.site3.disableDataLogCompression()

        if setAdPara('datashipping_parallel').lower() == 'on':
            parallelity = random.randint(1, 33)
            self.site1.enableDatashippingParallel(parallelity)
            self.site2.enableDatashippingParallel(parallelity)
            self.site3.enableDatashippingParallel(parallelity)
        else:
            self.site1.disableDatashippingParallel()
            self.site2.disableDatashippingParallel()
            self.site3.disableDatashippingParallel()

        if setAdPara('ssl').lower() == 'on':
            if setAdPara('systempki').lower() == 'off':
                self.site1.setupPSEFile()
                self.site2.setupPSEFile()
                self.site3.setupPSEFile()
            self.site1.setSSLConnection('on')
            self.site2.setSSLConnection('on')
            self.site3.setSSLConnection('on')
            self.globalCfg['srSSLEnabled'] = True
        # For 'ssl' setting 'source'/'target', assume the value is for Tier2 & infer the values for Tier1 & Tier3
        elif setAdPara('ssl').lower() == 'source':
            self.site1.setSSLConnection('off')
            self.site2.setSSLConnection('source')
            self.site3.setSSLConnection('target')
            self.globalCfg['srSSLEnabled'] = True
        elif setAdPara('ssl').lower() == 'target':
            self.site1.setSSLConnection('source')
            self.site2.setSSLConnection('target')
            self.site3.setSSLConnection('off')
            self.globalCfg['srSSLEnabled'] = True
        else:
            self.site1.setSSLConnection('off')
            self.site2.setSSLConnection('off')
            self.site3.setSSLConnection('off')
            self.globalCfg['srSSLEnabled'] = False

        if setAdPara('systempki').lower() == 'on':
            self.site1.setSSLCommunication('systemPKI')
            self.site2.setSSLCommunication('systemPKI')
            self.site3.setSSLCommunication('systemPKI')

        if self.globalCfg.get('srSSLEnabled') or setAdPara('systempki').lower() == 'on':
            # Restart systems for SSL settings to be effective
            self.site1.stopDatabaseLandscapeAsWhole()
            self.site2.stopDatabaseLandscapeAsWhole()
            self.site3.stopDatabaseLandscapeAsWhole()
            self.site1.waitForDatabaseLandscapeStopped()
            self.site2.waitForDatabaseLandscapeStopped()
            self.site3.waitForDatabaseLandscapeStopped()
            self.site1.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)

    @printTimeConsuming
    def advancedParametersSetupAfterHSR(self, adParas):
        '''these para are set after HSR setup
        '''
        defaultAdParas = testHaDR_AdvancedParameters_MultiTier.defaultAdParas
        def setAdPara(paraName):
            return adParas.get(paraName, defaultAdParas[paraName])

        if setAdPara('full_sync').lower() == 'on':
            self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = True
            self.globalCfg['full_sync'] = True
        else:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False
            self.globalCfg['full_sync'] = False

        self.site1.srState(self.site1.getHost("WORKER1"))
        self.site2.srState(self.site2.getHost("WORKER1"))
        self.site3.srState(self.site3.getHost("WORKER1"))

        self.site1.checkSecondaryActive()
        (self.site1, self.site2) = self.updateHostRoles(self.site1, self.site2)
        (self.site1, self.site3) = self.updateHostRoles(self.site1, self.site3)

        if self.globalCfg['withCustomTenant']:
            self.createDefaultTestTenant()

        isMultiDB = True if self.globalCfg['multiDB'] else False
        if setAdPara('persistence_encryption').lower() == 'on' and isMultiDB == False:
            self.site1.checkPersistenceEncryptionEnabled(isMultiDB)
            self.site2.checkPersistenceEncryptionEnabled(isMultiDB)
            self.site3.checkPersistenceEncryptionEnabled(isMultiDB)

    @printTimeConsuming
    def createDefaultTestTenant(self):
        # to save time when creating tenant by waitImportUpdateContentFinished=False, then wait at the end
        if self.site1.isMultiHostSystem():
            self.site1.createTenantDBInRep(self.globalCfg['dbname1'], multiNode=True, waitImportUpdateContentFinished=False)
        else:
            self.site1.createTenantDBInRep(self.globalCfg['dbname1'], waitImportUpdateContentFinished=False)
        self.site1.createTenantDBInRep(self.globalCfg['dbname2'], hostno=self.site1.getHost("WORKER1"), waitImportUpdateContentFinished=False)

        self.site1.checkSecondaryActive()

        print time.ctime(), '----> wait for ImportUpdateContentFinished'
        conman = self.site1.createSystemDBConnectionManager()
        cursor = conman.createConnection().cursor()
        dbConman1 = self.site1.createUserDBConnectionManager(self.globalCfg['dbname1'])
        dbcursor1 = dbConman1.createConnection().cursor()
        dbConman2 = self.site1.createUserDBConnectionManager(self.globalCfg['dbname2'])
        dbcursor2 = dbConman2.createConnection().cursor()
        self.site1.waitImportUpdateContentFinished(self.globalCfg['dbname1'], cursor, dbcursor1)
        self.site1.waitImportUpdateContentFinished(self.globalCfg['dbname2'], cursor, dbcursor2)

    @printTimeConsuming
    def commonSetUp(self):
        """common setup"""
        print time.ctime(), '----> enter common setup'
        #pdb.set_trace()

        def siteSetUp(site):
            HaTestRealMachines._cleanupDone = testHaDR_AdvancedParameters_MultiTier.allSetupsDone
            site.setUp()
            site.setUpTestCase()
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "sr_nameserver", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "indexserver", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "sr_dataaccess", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "sr_log_retention", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "pitrestart", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "warm_upper", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "sr_spcoordinator", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "global.ini", "persistencelayer", "debug")
            site.setTraceLevel(site.getHost("WORKER1"), "nameserver.ini", "nameserver", "info")
            site.setConfigParameter(site.getHost("WORKER1"), "global.ini", "ConfigMgrPy.CUSTOMER", "system_replication", "replication_port_offset", "50")
            site.setConfigParameter(site.getHost("WORKER1"), "global.ini", "ConfigMgrPy.CUSTOMER", "system_replication", "enable_assert_repository_update", "true")
            site.resetStatXSToMaster(self.globalCfg['multiDB'])
            site.setConfigParameter(site.getHost("WORKER1"), "global.ini", "ConfigMgrPy.CUSTOMER", "system_replication_communication", "allowed_sender", None)
            site.setConfigParameter(site.getHost("WORKER1"), "nameserver.ini", "ConfigMgrPy.CUSTOMER", "system_replication", "takeover_timeout", "1200000")
            site.setConfigParameter(site.getHost("WORKER1"), "global.ini", "ConfigMgrPy.CUSTOMER", "trace", "maxfiles", "50")

        self.site2.setConfigParameter(self.site2.getHost("WORKER1"), "indexserver.ini", "ConfigMgrPy.CUSTOMER", "sql", "reload_tables", "false")
        self.site2.setRemoteSystem(self.site1.getInstance()["instance_id"], self.site1.getLandscape()[(self.site1.getHost("WORKER1"), "hostname")].split(".")[0])

        self.site3.setConfigParameter(self.site3.getHost("WORKER1"), "indexserver.ini", "ConfigMgrPy.CUSTOMER", "sql", "reload_tables", "false")
        self.site3.setRemoteSystem(self.site2.getInstance()["instance_id"], self.site2.getLandscape()[(self.site2.getHost("WORKER1"), "hostname")].split(".")[0])

        siteSetUp(self.site1)
        siteSetUp(self.site2)
        siteSetUp(self.site3)

        if self.skipRegister:
            print "Skipping register"
            return

        # stop secondary here to speed up HSR setup
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site3.stopDatabaseLandscapeAsWhole()
        srCleanUp=True
        if testHaDR_AdvancedParameters_MultiTier.isFirstCase:
            srCleanUp=False
        self.site1.postSetUp(srCleanUp)
        self.site2.postSetUp()
        self.checkSecondaryActive(self.site1)

        self.site2.srEnable(self.site2.getHost("WORKER1"), self.site2.getSiteName())
        self.site3.postSetUp()

        self.checkSecondaryActive(self.site1)

        testHaDR_AdvancedParameters_MultiTier.allSetupsDone = True

        # for later "takeback"
        self.site1.setRemoteSystem(self.site2.getInstance()["instance_id"], self.site2.getLandscape()[(self.site2.getHost("WORKER1"), "hostname")].split(".")[0])
        self.site2.setRemoteSystem(self.site3.getInstance()["instance_id"], self.site3.getLandscape()[(self.site3.getHost("WORKER1"), "hostname")].split(".")[0])

    @printTimeConsuming
    def configSetup1(self):
        adPara = {
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup1(self):
        adPara = {
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup1(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup2(self):
        adPara = {
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup2(self):
        adPara = {
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup2(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup3(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'data_log_compression':'on',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup3(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'data_log_compression':'on',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup3(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'syncmem',
                  'data_log_compression':'on',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup4(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup4(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup4(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'syncmem',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configSetup5(self):
        adPara = {
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'ssl':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup5(self):
        adPara = {
                  'ssl':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup5(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'ssl':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configSetup6(self):
        adPara = {
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'ssl':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup6(self):
        adPara = {
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'ssl':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup6(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'ssl':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configParallelSetup6(self):
        adPara = {
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'datashipping_parallel':'on',
                  'ssl':'on',
                  'full_sync':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configSetup7(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup7(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup7(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'syncmem',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configParallelSetup7(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                  'datashipping_parallel':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configSetup8(self):
        adPara = {
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

        self.globalCfg['dbname3'] = 'TE3'

    @printTimeConsuming
    def configHotSetup8(self):
        adPara = {
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)
        self.globalCfg['dbname3'] = 'TE3'

    @printTimeConsuming
    def configAASetup8(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'withCustomTenant':True,
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)
        self.globalCfg['dbname3'] = 'TE3'


    @printTimeConsuming
    def configSetup10(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'sync_mode_1':'sync',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup10(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'sync_mode_1':'sync',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup10(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'syncmem',
                  'sync_mode_1':'sync',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup11(self):
        adPara = {
                  'sync_mode':'async',
                  'sync_mode_1':'async',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup11(self):
        adPara = {
                  'sync_mode':'async',
                  'sync_mode_1':'async',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup11(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'async',
                  'sync_mode_1':'async',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup12(self):
        adPara = {
                  'sync_mode':'sync',
                  'sync_mode_1':'sync',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup12(self):
        adPara = {
                  'sync_mode':'sync',
                  'sync_mode_1':'sync',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup12(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'sync',
                  'sync_mode_1':'sync',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup13(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'sync_mode_1':'syncmem',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup13(self):
        adPara = {
                  'sync_mode':'syncmem',
                  'sync_mode_1':'syncmem',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup13(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'syncmem',
                  'sync_mode_1':'syncmem',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)


    @printTimeConsuming
    def configSetup14(self):
        adPara = {
                  'sync_mode':'sync',
                  'sync_mode_1':'syncmem',
                  'op_mode':'delta_datashipping',
                  'op_mode_1':'delta_datashipping',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configHotSetup14(self):
        adPara = {
                  'sync_mode':'sync',
                  'sync_mode_1':'syncmem',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    @printTimeConsuming
    def configAASetup14(self):
        adPara = {
                  'op_mode':'logreplay_readaccess',
                  'sync_mode':'sync',
                  'sync_mode_1':'syncmem',
                  'persistence_encryption':'on',
                  'log_encryption':'on',
                  'data_log_compression':'on',
                 }

        self.initSites()
        self.advancedParametersSetupBeforeHSR(adPara)
        self.commonSetUp()
        self.advancedParametersSetupAfterHSR(adPara)
        self.checkSecondaryActive(self.site1)

    def setUpConfig(self):
        """Set up for config"""
        pass

    def tearDownConfig(self):
        """Tear down for config"""
        print time.ctime(), 'enter tearDownConfig'

        self.site1 = self.globalCfg['site1']
        self.site2 = self.globalCfg['site2']
        self.site3 = self.globalCfg['site3']

        self.site1.databaseLandscapeInfo()
        self.site2.databaseLandscapeInfo()
        self.site3.databaseLandscapeInfo()
        self.site1.systemReplicationStatus()

        if self.globalCfg['sync_mode'] == 'sync' and self.site1.fullSync:
            try:
                self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = False
            except Exception, e:
                print 'disable full_sync in tearDownConfig failed: %s' % e

        for h in range(1, self.site1.getHostNo()):
            self.site1.setConfigParameter(h, "daemon.ini", "ConfigMgrPy.HOST", "indexserver.c", "instanceids", None)
        for h in range(1, self.site2.getHostNo()):
            self.site2.setConfigParameter(h, "daemon.ini", "ConfigMgrPy.HOST", "indexserver.c", "instanceids", None)
        for h in range(1, self.site3.getHostNo()):
            self.site3.setConfigParameter(h, "daemon.ini", "ConfigMgrPy.HOST", "indexserver.c", "instanceids", None)

        self.site1.resetStatXSToMaster(self.globalCfg['multiDB'])
        self.site2.resetStatXSToMaster(self.globalCfg['multiDB'])
        self.site3.resetStatXSToMaster(self.globalCfg['multiDB'])

        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "sr_nameserver", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "sr_dataaccess", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "sr_log_retention", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "pitrestart", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "warm_upper", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "sr_spcoordinator", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "global.ini", "persistencelayer", None)
        self.site1.setTraceLevel(self.site1.getHost("WORKER1"), "nameserver.ini", "nameserver", None)

        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "sr_nameserver", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "sr_dataaccess", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "sr_log_retention", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "pitrestart", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "warm_upper", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "sr_spcoordinator", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "global.ini", "persistencelayer", None)
        self.site2.setTraceLevel(self.site2.getHost("WORKER1"), "nameserver.ini", "nameserver", None)

        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "sr_nameserver", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "sr_dataaccess", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "sr_log_retention", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "pitrestart", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "warm_upper", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "sr_spcoordinator", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "global.ini", "persistencelayer", None)
        self.site3.setTraceLevel(self.site3.getHost("WORKER1"), "nameserver.ini", "nameserver", None)

        # for normal tear down(unregister/disable), the steps should be in order
        # the primary cannot be disabled if there's secondary attached
        # so there's no need to use multi-thread
        # executing here means the landscape has been resorded to site1--(sync/syncmem)--site2--(async)--site3
        #pdb.set_trace()
        self.site3.tearDown()
        self.site2.tearDown()
        self.site1.tearDown()

    def setUpTestCase(self):
        """Sets up the test case."""
        pass

    def setUp(self):
        """Sets up a test."""
        print time.ctime(), "enter setUp"
        #pdb.set_trace()
        self.site1 = self.globalCfg['site1']
        self.site2 = self.globalCfg['site2']
        self.site3 = self.globalCfg['site3']

        # if the former case or teardown failed, re-exec the config, setup sys-rep
        if not testHaDR_AdvancedParameters_MultiTier.isFirstCase:
            if not testHaDR_AdvancedParameters_MultiTier.testRunSuccess or not testHaDR_AdvancedParameters_MultiTier.testTearDownSuccess:
                print time.ctime(), "----> cleanup and re-setup system replication since last case/teardown failed..."
                # in case the previous case's teardown failed, and the primary is happened to be host2(there is a failback before),
                # there's no chance to disable full_sync anymore, maybe lead to the next case(if failback) failed
                if self.globalCfg['sync_mode'] == 'sync' and self.site1.fullSync:
                    self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                    self.site1.fullSync = False
                t1 = threading.Thread(target = self.site1.cleanUp)
                t2 = threading.Thread(target = self.site2.cleanUp)
                t3 = threading.Thread(target = self.site3.cleanUp)
                t1.start()
                t2.start()
                t3.start()
                t1.join()
                t2.join()
                t3.join()
                self.site1.startDatabaseLandscapeAsWhole()
                self.site2.startDatabaseLandscapeAsWhole()
                self.site3.startDatabaseLandscapeAsWhole()
                self.waitForDatabaseLandscapeStartedByPY(self.site1)
                self.waitForDatabaseLandscapeStartedByPY(self.site2)
                self.waitForDatabaseLandscapeStartedByPY(self.site3)
                getattr(self, self.getCurCfg())()


        if self._testMethodName == 'test220INIParaReplication':
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "persistence", "savepoint_interval_s", "200")
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "global.ini", "CUSTOMER", "persistence", "savepoint_interval_s", "200")
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "global.ini", "CUSTOMER", "persistence", "savepoint_interval_s", "200")
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER2"), "indexserver.ini", "HOST", "authorization", "internal_support_user_limit", "2")
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER2"), "indexserver.ini", "HOST", "authorization", "internal_support_user_limit", "2")
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER2"), "indexserver.ini", "HOST", "authorization", "internal_support_user_limit", "2")
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER2"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", "400")
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER2"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", "400")
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER2"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", "400")
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER2"), "preprocessor.ini", "HOST", "lexicon", "abort_time", "200")
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER2"), "preprocessor.ini", "HOST", "lexicon", "abort_time", "200")
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER2"), "preprocessor.ini", "HOST", "lexicon", "abort_time", "200")
            self.site1.setConfigParameterPerLayer(self.site1.getHost("STANDBY1"), "xsengine.ini", "HOST", "httpserver", "maxthreads", "300")
            self.site2.setConfigParameterPerLayer(self.site2.getHost("STANDBY1"), "xsengine.ini", "HOST", "httpserver", "maxthreads", "300")
            self.site3.setConfigParameterPerLayer(self.site3.getHost("STANDBY1"), "xsengine.ini", "HOST", "httpserver", "maxthreads", "300")
            if self._multiDBInstance and not self.globalCfg['withInitTenant']:
               self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "2", self.globalCfg['dbname1'])
               self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "2", self.globalCfg['dbname1'])
               self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "2", self.globalCfg['dbname1'])
               self.site1.setConfigParameterPerLayer(self.site1.getHost("STANDBY1"), "scriptserver.ini", "CUSTOMER", "adapter_operation_cache", "geocode", "15", self.globalCfg['dbname1'])
               self.site2.setConfigParameterPerLayer(self.site2.getHost("STANDBY1"), "scriptserver.ini", "CUSTOMER", "adapter_operation_cache", "geocode", "15", self.globalCfg['dbname1'])
               self.site3.setConfigParameterPerLayer(self.site3.getHost("STANDBY1"), "scriptserver.ini", "CUSTOMER", "adapter_operation_cache", "geocode", "15", self.globalCfg['dbname1'])
               self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "xsengine.ini", "CUSTOMER", "transaction", "table_lock_array_size", "2", self.globalCfg['dbname1'])
               self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "xsengine.ini", "CUSTOMER", "transaction", "table_lock_array_size", "2", self.globalCfg['dbname1'])
               self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "xsengine.ini", "CUSTOMER", "transaction", "table_lock_array_size", "2", self.globalCfg['dbname1'])

        if testHaDR_AdvancedParameters_MultiTier.isFirstCase:
            testHaDR_AdvancedParameters_MultiTier.isFirstCase = False

    def tearDown(self):
        """Tears down a test."""
        # check clean test run flag
        print time.ctime(), '----> enter %s tearDown' % (self._testMethodName)
        #pdb.set_trace()

        testHaDR_AdvancedParameters_MultiTier.testTearDownSuccess = False

        if self._testMethodName == 'test201ErrorProvokeDVEKeyT2':
            (c, o) = self.site2.runProgramInGuest(self.site2.getHost("WORKER1"), "chmod uog+w /usr/sap/$SAPSYSTEMNAME/SYS/global/hdb/security/ssfs/SSFS_$SAPSYSTEMNAME.DAT", siduser = True, returnOutput = True)
            print time.ctime(), "rc=%s" % c, o
        elif self._testMethodName == 'test202ErrorProvokeDVEKeyT3':
            (c, o) = self.site3.runProgramInGuest(self.site3.getHost("WORKER1"), "chmod uog+w /usr/sap/$SAPSYSTEMNAME/SYS/global/hdb/security/ssfs/SSFS_$SAPSYSTEMNAME.DAT", siduser = True, returnOutput = True)
            print time.ctime(), "rc=%s" % c, o
        elif self._testMethodName == 'test220INIParaReplication':
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "enable_tier_3", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "enable_tier_3", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "enable_tier_3", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate_tier_3", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate_tier_3", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate_tier_3", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "interval", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "interval", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "interval", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "indexserver.ini", "CUSTOMER", "communication", "maxchannels", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "indexserver.ini", "CUSTOMER", "communication", "maxchannels", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "indexserver.ini", "CUSTOMER", "communication", "maxchannels", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "HOST", "expensive_statement", "maxfilesize", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "global.ini", "HOST", "expensive_statement", "maxfilesize", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "global.ini", "HOST", "expensive_statement", "maxfilesize", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", None)
            self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "preprocessor.ini", "HOST", "lexicon", "abort_time", None)
            self.site2.setConfigParameterPerLayer(self.site2.getHost("WORKER1"), "preprocessor.ini", "HOST", "lexicon", "abort_time", None)
            self.site3.setConfigParameterPerLayer(self.site3.getHost("WORKER1"), "preprocessor.ini", "HOST", "lexicon", "abort_time", None)



        # clean up if the case failed
        if not testHaDR_AdvancedParameters_MultiTier.testRunSuccess:
            self.site1.databaseLandscapeInfo()
            self.site2.databaseLandscapeInfo()
            self.site3.databaseLandscapeInfo()
            self.site1.systemReplicationStatus()
            self.site2.systemReplicationStatus()
            self.site3.systemReplicationStatus()
            print time.ctime(), "    #####################################"
            print time.ctime(), "    ### clean up after test execution ###"
            print time.ctime(), "    #####################################"
            # in case the last case failed, and the primary is happened to be host2,
            # there's no chance to disable full_sync anymore, maybe lead to the next profile failed
            if self.globalCfg['sync_mode'] == 'sync' and self.site1.fullSync:
                self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = False
            t1 = threading.Thread(target = self.site1.cleanUp)
            t2 = threading.Thread(target = self.site2.cleanUp)
            t3 = threading.Thread(target = self.site3.cleanUp)
            t1.start()
            t2.start()
            t3.start()
            t1.join()
            t2.join()
            t3.join()
            self.site1.startDatabaseLandscapeAsWhole()
            self.site2.startDatabaseLandscapeAsWhole()
            self.site3.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)
            self.waitForDatabaseLandscapeStartedByPY(self.site2)
            self.waitForDatabaseLandscapeStartedByPY(self.site3)
            print time.ctime(), "    #####################################"
            print time.ctime(), "    ############### done ################"
            print time.ctime(), "    #####################################"
            return

        # restore the sys-rep
        if self._testMethodName == 'test070Disaster1Tier2Takeover' or self._testMethodName == 'test075OfflineTakeover260' or self._testMethodName == 'test180Recovery' or self._testMethodName == 'test203RootKeyVersionConsistencyAfterTakeOver' or self._testMethodName == 'test230DisasterTakeover' or self._testMethodName == 'test450OfflineTakeover190':
            # restore to s1 -- s2 -- s3
            #pdb.set_trace()
            if self.globalCfg['full_sync']:
                self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = False

            if self._testMethodName == 'test180Recovery':
                self.site3.stopDatabaseLandscapeAsWhole()
                self.site3.waitForDatabaseLandscapeStopped()
                self.site3.srUnregister(self.site3.getHost("WORKER1"))
                self.site3.startDatabaseLandscapeAsWhole()
                self.site3.waitForDatabaseLandscapeStartedByPY()

            self.site2.stopDatabaseLandscapeAsWhole()
            self.site3.stopDatabaseLandscapeAsWhole()
            self.site1.srCleanUp(self.site1.getHost("WORKER1"), "--force")

            self.site1.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)
            self.site1.srEnable(self.site1.getHost("WORKER1"), self.site1.getSiteName())
            self.site2.waitForDatabaseLandscapeStopped()
            self.site2.srRegister(self.site2.getHost("WORKER1"), self.site2.getSiteName(), self.site2.remoteInstance, self.site2.remoteHost, self.globalCfg['sync_mode'], self.globalCfg['op_mode'])
            self.site2.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site2)

            #Temporary fix
            if self._testMethodName == 'test230DisasterTakeover' or self._testMethodName == 'test450OfflineTakeover190':
                self.checkSecondaryActive(self.site1)
            self.site2.srEnable(self.site2.getHost("WORKER1"), self.site2.getSiteName())
            self.site3.waitForDatabaseLandscapeStopped()
            self.site3.srRegister(self.site3.getHost("WORKER1"), self.site3.getSiteName(), self.site3.remoteInstance, self.site3.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode_1'])
            self.site3.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site3)

            if self.globalCfg['full_sync']:
                self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = True
        elif self._testMethodName == 'test080Disaster2Tier3Takeover' or self._testMethodName == 'test130DisasterTakeover':
            self.site3.stopDatabaseLandscapeAsWhole()
            self.site1.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)
            self.site2.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site2)
            self.site3.waitForDatabaseLandscapeStopped()
            self.site3.srRegister(self.site3.getHost("WORKER1"), self.site3.getSiteName(), self.site3.remoteInstance, self.site3.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode_1'])
            self.site3.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site3)
        elif self._testMethodName == 'test090Failback1':
            # must restore to the original site and role relationship, since there will be recovery test followed,
            # in which the data/log copying will only can be done through pre-configured ssh access without passwd
            #pdb.set_trace()
            if self.globalCfg['full_sync']:
                self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = False

            self.site3.srTakeover(self.site3.getHost("WORKER1"))
            self.site3.stopDatabaseLandscapeAsWhole()
            self.site3.waitForDatabaseLandscapeStopped()
            self.site3.srCleanUp(self.site3.getHost("WORKER1"), "--force")



            self.site1.stopDatabaseLandscapeAsWhole()
            self.site2.stopDatabaseLandscapeAsWhole()
            self.site3.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site3)
            self.site3.srEnable(self.site3.getHost("WORKER1"), self.site3.getSiteName())

            self.site1.waitForDatabaseLandscapeStopped()
            self.site1.srRegister(self.site1.getHost("WORKER1"), self.site1.getSiteName(), self.site1.remoteInstance, self.site1.remoteHost, self.globalCfg['sync_mode'], self.globalCfg['op_mode'])
            self.site1.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)

            self.site1.srEnable(self.site1.getHost("WORKER1"), self.site1.getSiteName())
            self.site2.waitForDatabaseLandscapeStopped()
            self.site2.srRegister(self.site2.getHost("WORKER1"), self.site2.getSiteName(), self.site2.remoteInstance, self.site2.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode_1'])
            self.site2.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site2)
            (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site3, self.site1, self.site2)

            if self.globalCfg['full_sync']:
                self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = True
        elif self._testMethodName == 'test240Failback':
            # must restore to the original site and role relationship, since there will be recovery test followed,
            # in which the data/log copying will only can be done through pre-configured ssh access without passwd
            #pdb.set_trace()
            if self.globalCfg['full_sync']:
                self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = False

            self.site3.srTakeover(self.site3.getHost("WORKER1"))
            (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site3, self.site1, self.site2)

            self.site2.stopDatabaseLandscapeAsWhole()
            self.site3.stopDatabaseLandscapeAsWhole()
            self.site2.waitForDatabaseLandscapeStopped()
            self.site2.srRegister(self.site2.getHost("WORKER1"), self.site2.getSiteName(), self.site2.remoteInstance, self.site2.remoteHost, self.globalCfg['sync_mode'], self.globalCfg['op_mode'])
            self.site2.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site2)

            self.site2.srEnable(self.site2.getHost("WORKER1"), self.site2.getSiteName())
            self.site3.waitForDatabaseLandscapeStopped()
            self.site3.srRegister(self.site3.getHost("WORKER1"), self.site3.getSiteName(), self.site3.remoteInstance, self.site3.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode'])
            self.site3.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site3)

            if self.globalCfg['full_sync']:
                self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = True
        elif self._testMethodName == 'test190HAFStopWorker' or self._testMethodName == 'test200HAFStopMaster' or self._testMethodName == 'test290HAFStopWorker' or self._testMethodName == 'test300HAFStopMaster':
            # restart to make sure real role is the same with config role
            #pdb.set_trace()
            if self.globalCfg['full_sync']:
                self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = False

            self.site1.stopDatabaseLandscapeAsWhole()
            self.site1.waitForDatabaseLandscapeStopped()
            self.site1.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)

            if self.globalCfg['full_sync']:
                self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
                self.site1.fullSync = True

        elif self._testMethodName == 'test01LogshippingTier3' or self._testMethodName == 'test02LogshippingTier2' or  self._testMethodName == 'test03LogshippingGenScenarios':
            print '---> Unset ES_LOG_BACKUP_INTERVAL'
            self.site1.setLogRetentionOptions(None,None)
            self.setConfigParameter(self.getHost("WORKER1"), "esserver.ini", "ConfigMgrPy.CUSTOMER", "database", "ES_LOG_BACKUP_INTERVAL", None)
            # Restart primary for options to take effect
            print time.ctime(), '----> stopping primary...'
            self.site1.stopDatabaseLandscapeAsWhole()
            self.site1.waitForDatabaseLandscapeStopped()
            print time.ctime(), '----> Restarting primary...'
            self.site1.startDatabaseLandscapeAsWhole()
            self.waitForDatabaseLandscapeStartedByPY(self.site1)

        self.checkSecondaryActive(self.site1)

        print time.ctime(), 'leaving tearDown...'
        print time.ctime(), "host-role-mappings site1: "
        for h in range(1, self.site1.getHostNo()):
            print time.ctime(), "  ", self.site1.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site1.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings site2: "
        for h in range(1, self.site2.getHostNo()):
            print time.ctime(), "  ", self.site2.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site2.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings site3: "
        for h in range(1, self.site3.getHostNo()):
            print time.ctime(), "  ", self.site3.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site3.getLandscape()[(h, "role")]

        self.site1.databaseLandscapeInfo()
        self.site2.databaseLandscapeInfo()
        self.site3.databaseLandscapeInfo()

        testHaDR_AdvancedParameters_MultiTier.testTearDownSuccess = True

    def tearDownTestCase(self):
        """Tears down the testcase."""
        #self.site2._copyAllTraceFiles(self.__class__.__name__)
        pass

    def restoreSiteRoles(self, primary, secondary, secondary1):
        ''' restore to that object site1 always represents the primary,
            object site2 always represents the secondary
            object site3 always represents the secondary1
            update with the real site name
        '''
        reinitializeTier2DT = False
        if self.site1 == secondary and self.site2 == secondary1:
            reinitializeTier2DT = True

        print time.ctime(), "host-role-mappings site1: "
        for h in range(1, self.site1.getHostNo()):
            print time.ctime(), "  ", self.site1.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site1.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings site2: "
        for h in range(1, self.site2.getHostNo()):
            print time.ctime(), "  ", self.site2.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site2.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings site3: "
        for h in range(1, self.site3.getHostNo()):
            print time.ctime(), "  ", self.site3.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site3.getLandscape()[(h, "role")]

        self.site1 = primary
        self.site1.secondary = secondary
        self.site1.secondary1 = secondary1
        self.site2 = secondary
        self.site3 = secondary1

        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.SECONDARY
        self.site3.itsDRRole  = siteTypes.SECONDARY1

        self.site1.setRemoteSystem(self.site3.getInstance()["instance_id"], self.site3.getLandscape()[(self.site3.getHost("WORKER1"), "hostname")].split(".")[0])
        self.site2.setRemoteSystem(self.site1.getInstance()["instance_id"], self.site1.getLandscape()[(self.site1.getHost("WORKER1"), "hostname")].split(".")[0])
        self.site3.setRemoteSystem(self.site2.getInstance()["instance_id"], self.site2.getLandscape()[(self.site2.getHost("WORKER1"), "hostname")].split(".")[0])

        self.globalCfg['site1'] = self.site1
        self.globalCfg['site2'] = self.site2
        self.globalCfg['site3'] = self.site3

        print time.ctime(), "----> after restore: "
        print time.ctime(), "host-role-mappings site1: "
        for h in range(1, self.site1.getHostNo()):
            print time.ctime(), "  ", self.site1.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site1.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings site2: "
        for h in range(1, self.site2.getHostNo()):
            print time.ctime(), "  ", self.site2.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site2.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings site3: "
        for h in range(1, self.site3.getHostNo()):
            print time.ctime(), "  ", self.site3.getLandscape()[(h, "hostname")].split(".")[0], "=>", self.site3.getLandscape()[(h, "role")]

        self.site1.tier1 = None
        self.site1.tier2 = None
        self.site2.tier1 = self.site1
        self.site2.tier2 = None
        self.site3.tier1 = self.site1
        self.site3.tier2 = self.site2

        return (primary, secondary, secondary1)

    def updateHostRoles(self, primary, secondary):
        if primary._multiDBInstance and primary.globalCfg.get('withCustomTenant'):
            conn = primary.connectToNextBestHost(dbname="SYSTEMDB")
        else:
            conn = primary.connectToNextBestHost()
        cur = conn.cursor()
        cur.execute("SELECT HOST, SECONDARY_HOST FROM M_SERVICE_REPLICATION WHERE SECONDARY_SITE_NAME='%s' GROUP BY HOST, SECONDARY_HOST" % secondary.siteName)
        e = cur.fetchall()

        for it in e:
            for h in range(1, primary.getHostNo()):
                if primary.getLandscape()[(h, "hostname")].split(".")[0] == it[0]:
                    secondary.getLandscape()[(h, "hostname")] = it[1]
                    secondary.getLandscape()[(h, "role")] = primary.getLandscape()[(h, "role")]
                    continue

        secondary._hosts = primary._hosts.copy()

        print time.ctime(), "host-role-mappings primary: "
        for h in range(1, primary.getHostNo()):
            print time.ctime(), "  ", primary.getLandscape()[(h, "hostname")].split(".")[0], "=>", primary.getLandscape()[(h, "role")]
        print time.ctime(), "host-role-mappings secondary: "
        for h in range(1, secondary.getHostNo()):
            print time.ctime(), "  ", secondary.getLandscape()[(h, "hostname")].split(".")[0], "=>", secondary.getLandscape()[(h, "role")]

        return (primary, secondary)

    def checkSecondaryActive(self, primary, *args, **kwargs):
        try:
            primary.checkSecondaryActive(*args, **kwargs)
        except Exception, e:
            self.site1.collectRuntimeDumps()
            self.site2.collectRuntimeDumps()
            self.site3.collectRuntimeDumps()
            self.fail('check secondary active failed, error message:\n%s' % e)

    def waitForDatabaseLandscapeStartedByPY(self, site, *args, **kwargs):
        try:
            site.waitForDatabaseLandscapeStartedByPY(*args, **kwargs)
        except Exception, e:
            self.site1.collectRuntimeDumps()
            self.site2.collectRuntimeDumps()
            self.site3.collectRuntimeDumps()
            self.fail('wait for site %s started failed, error message:\n%s' % (site.getSiteName(), e))

    @printTimeConsuming
    def copyDatabaseTrace(self, databaseName):
        self.site1.copyDatabaseTrace(databaseName)
        self.site2.copyDatabaseTrace(databaseName)
        self.site3.copyDatabaseTrace(databaseName)

    def checkDVEKeyChangeErrorProvoke(self, site):
        '''check the rootkey is failed to change when depriving the write permission of ssfs DAT file.
        '''
        # get the original rootkeys in all tiers
        print time.ctime(), '----> get the hashed rootkeys on all tiers'
        originKeysSite1 = self.site1.getHashedRootKeys()
        print time.ctime(), 'original Hashed RootKeys on site 1 is %s' % originKeysSite1
        originKeysSite2 = self.site2.getHashedRootKeys()
        print time.ctime(), 'original Hashed RootKeys on site 2 is %s' % originKeysSite2
        originKeysSite3 = self.site3.getHashedRootKeys()
        print time.ctime(), 'original Hashed RootKeys on site 3 is %s' % originKeysSite3

        print time.ctime(), '----> deprive the write permission of SSFS DAT file '
        (c, o) = site.runProgramInGuest(site.getHost("WORKER1"), "chmod uog-w /usr/sap/$SAPSYSTEMNAME/SYS/global/hdb/security/ssfs/SSFS_$SAPSYSTEMNAME.DAT", siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c, o
        self.assertTrue(c == 0 , "Change ssfs DAT file permission failed!")

        print time.ctime(), '----> Try to change persistence rootkeys'
        try:
           self.site1.changePersistenceRootKey()
        except Exception, e:
           self.assertTrue("Sending and Activating DVE root key to secondary failed" in e.message , e.message + " Not expect error!")
        else:
           self.fail("change Persistence key still succeed! FAIL")
        print time.ctime(), '----> Try to change DpApi rootkeys'

        try:
           self.site1.changeDpApiRootKey()
        except Exception, e:
           self.assertTrue("Sending and Activating Dpapi root key to secondary failed" in e.message , e.message + " Not expect error!")
        else:
           self.fail("change DpApi key still succeed! FAIL")

        print time.ctime(), '----> Try to change LOG  root keys'
        try:
           self.site1.changeLogRootKey()
        except Exception, e:
           self.assertTrue("Sending Redo log root key to secondary failed" in e.message , e.message + " Not expect error!")
        else:
           self.fail("change Log key still succeed! FAIL")

        newKeysSite1 = self.site1.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 1 is %s' % newKeysSite1
        newKeysSite2 = self.site2.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 2 is %s' % newKeysSite2
        newKeysSite3 = self.site3.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 3 is %s' % newKeysSite3

        print time.ctime(), '----> verify the rootkeys is not changed on all tiers'
        if newKeysSite1.has_key('PERSISTENCE Root Key') and newKeysSite1.has_key('DPAPI Root Key'):
            self.assertTrue(originKeysSite1['PERSISTENCE Root Key'] == newKeysSite1['PERSISTENCE Root Key'], "the persistence rootkeys on tier 1 changed!")
            self.assertTrue(originKeysSite2['PERSISTENCE Root Key'] == newKeysSite2['PERSISTENCE Root Key'], "the persistence rootkeys on tier 2 changed!")
            self.assertTrue(originKeysSite3['PERSISTENCE Root Key'] == newKeysSite3['PERSISTENCE Root Key'], "the persistence rootkeys on tier 3 changed!")
            self.assertTrue(originKeysSite1['DPAPI Root Key'] == newKeysSite1['DPAPI Root Key'], "the DpApi rootkeys on tier 1 changed!")
            self.assertTrue(originKeysSite2['DPAPI Root Key'] == newKeysSite2['DPAPI Root Key'], "the DpApi rootkeys on tier 2 changed!")
            self.assertTrue(originKeysSite3['DPAPI Root Key'] == newKeysSite3['DPAPI Root Key'], "the DpApi rootkeys on tier 3 changed!")
            print time.ctime(), '----> verify LOG rootkey is not changed on all tiers'
            self.assertTrue(originKeysSite1['LOG Root Key'] == newKeysSite1['LOG Root Key'], "the Log rootkeys on tier 1 changed!")
            self.assertTrue(originKeysSite2['LOG Root Key'] == newKeysSite2['LOG Root Key'], "the Log rootkeys on tier 2 changed!")
            self.assertTrue(originKeysSite3['LOG Root Key'] == newKeysSite3['LOG Root Key'], "the Log rootkeys on tier 3 changed!")
        else:
            raise Exception("please check the return value pattern changes of 'hdbnsutil -printHashedRootKeys'")
        print time.ctime(), '----> verify the new rootkeys are the same on all tiers'
        self.assertTrue(newKeysSite1 == newKeysSite2 and newKeysSite2 == newKeysSite3, "new keys are different between tiers!")




    @classification("with_restart", "barrier")
    def test030StopTier3Secondary(self):
        '''stop tier-3 secondary, check that primary and tier-2 secondary are running and reconnect tier-3 secondary
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        print time.ctime(), '----> stop db...'
        self.site3.stopDatabaseLandscapeAsWhole()
        print time.ctime(), '----> wait for stoping db...'
        self.site3.waitForDatabaseLandscapeStopped()

        #pdb.set_trace()
        print time.ctime(), '----> check tier1--tier2 sync...'
        self.checkSecondaryActive(self.site1, self.site2)

        print time.ctime(), '----> start db...'
        self.site3.startDatabaseLandscapeAsWhole()
        print time.ctime(), '----> wait for starting db...'
        self.waitForDatabaseLandscapeStartedByPY(self.site3)

        # check the sys rep status
        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site1)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True


    @classification("with_restart", "barrier")
    def test050StopTier2Secondary(self):
        '''stop tier-2 secondary, check that fullsync option prohibites write transactions on primary and reconnect tier-2 secondary
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        # check if transaction frozen
        if self.globalCfg['sync_mode'] == 'sync' and self.site1.fullSync:
            print time.ctime(), '----> testing transaction frozen'
            self.site1.isTransactionFrozen(self.site2, mode='stop')
        else:
            #pdb.set_trace()
            print time.ctime(), '----> stop db...'
            self.site2.stopDatabaseLandscapeAsWhole()
            print time.ctime(), '----> wait for stoping db...'
            self.site2.waitForDatabaseLandscapeStopped()

            print time.ctime(), '----> start db...'
            self.site2.startDatabaseLandscapeAsWhole()
            print time.ctime(), '----> wait for starting db...'
            self.waitForDatabaseLandscapeStartedByPY(self.site2)

        # check the sys rep status
        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site1)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test055TenantDBTransactionFrozen270(self):
        '''check tenant database frozen on full sync mode
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        # check if transaction frozen
        if self.globalCfg['sync_mode'] == 'sync' and self.site1.fullSync:
            print time.ctime(), '----> testing tenantDB transaction frozen'
            self.site1.isTransactionFrozen(self.site2, mode='kill_tdb', tenantDBName=self.globalCfg['dbname1'])
            # bug 100793, give secondary more time , sleep 2mins
            time.sleep(120)

        # check the sys rep status
        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site1, None, True)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test070Disaster1Tier2Takeover(self):
        '''Disaster-Scenario I: Primary (Site1) failed, Tier-2 Secondary (Site2) takes over
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        pdb.set_trace()
        testTab1 = "test070Disaster1Tier2Takeover1"
        testTab2 = "test070Disaster1Tier2Takeover2"

        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            self.site1.createTestTab(conn1, testTab1, self.site1.getHost("WORKER1"))
            tab1_t1e1 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            self.site1.createTestTab(conn2, testTab2, self.site1.getHost("WORKER1"))
            tab2_t2e1 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            self.site1.createTestTab(conn, testTab1, self.site1.getHost("WORKER1"))
            tab1_e1 = self.site1.selectFromTestTab(conn, testTab1)

        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        print time.ctime(), '----> secondary(site2) takeover ...'
        self.site2.srTakeover(self.site2.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.PRIMARY

        if self.site1.fullSync:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False

        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site2)

        #pdb.set_trace()
        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e2 = self.site2.selectFromTestTab(conn1, testTab1)

            conn2 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e2 = self.site2.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site2.setUpConnection(self.site2.getHost("WORKER1"))
            tab1_e2 = self.site2.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e2, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e2, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e2, "table was not transferred correctly")

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site1, self.site2, self.site3)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test075OfflineTakeover260(self):
        '''Offline Takeover
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        testTab1 = "test260OfflineTakeover1"
        testTab2 = "test260OfflineTakeover2"

        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            self.site1.createTestTab(conn1, testTab1, self.site1.getHost("WORKER1"))
            tab1_t1e1 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            self.site1.createTestTab(conn2, testTab2, self.site1.getHost("WORKER1"))
            tab2_t2e1 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            self.site1.createTestTab(conn, testTab1, self.site1.getHost("WORKER1"))
            tab1_e1 = self.site1.selectFromTestTab(conn, testTab1)

        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        self.site2.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()

        print time.ctime(), '----> secondary(site2) takeover ...'
        self.site2.srTakeover(self.site2.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.PRIMARY

        if self.site1.fullSync:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False

        self.site2.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site2)

        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site2)

        #pdb.set_trace()
        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e2 = self.site2.selectFromTestTab(conn1, testTab1)

            conn2 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e2 = self.site2.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site2.setUpConnection(self.site2.getHost("WORKER1"))
            tab1_e2 = self.site2.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e2, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e2, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e2, "table was not transferred correctly")

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site1, self.site2, self.site3)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test080Disaster2Tier3Takeover(self):
        '''Disaster-Scenario II: Primary (Site1) and Tier-2 Secondary (Site2) failed, Tier-3 Secondary takes over
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        testTab1 = "test080Disaster2Tier3Takeover1"
        testTab2 = "test080Disaster2Tier3Takeover2"

        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            self.site1.createTestTab(conn1, testTab1, self.site1.getHost("WORKER1"))
            tab1_t1e1 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            self.site1.createTestTab(conn2, testTab2, self.site1.getHost("WORKER1"))
            tab2_t2e1 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            self.site1.createTestTab(conn, testTab1, self.site1.getHost("WORKER1"))
            tab1_e1 = self.site1.selectFromTestTab(conn, testTab1)

        #self.site1.stopDatabaseLandscape(2) # kill
        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        self.site2.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()

        print time.ctime(), '----> secondary(site3) takeover ...'
        self.site3.srTakeover(self.site3.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.SECONDARY
        self.site3.itsDRRole  = siteTypes.PRIMARY

        if self.site1.fullSync:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False

        #pdb.set_trace()
        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site3.setUpConnection(self.site3.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e2 = self.site3.selectFromTestTab(conn1, testTab1)

            conn2 = self.site3.setUpConnection(self.site3.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e2 = self.site3.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site3.setUpConnection(self.site3.getHost("WORKER1"))
            tab1_e2 = self.site3.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e2, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e2, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e2, "table was not transferred correctly")

        #pdb.set_trace()
        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site1, self.site2, self.site3)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test090Failback1(self):
        '''Failback scenario I: After takeover from Primary (Site1) to Tier-2 Secondary (Site2) re-attach Site1 as ASYNC Tier-3 secondary
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        testTab1 = "test090Failback11"
        testTab2 = "test090Failback12"

        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            self.site1.createTestTab(conn1, testTab1, self.site1.getHost("WORKER1"))
            tab1_t1e1 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            self.site1.createTestTab(conn2, testTab2, self.site1.getHost("WORKER1"))
            tab2_t2e1 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            self.site1.createTestTab(conn, testTab1, self.site1.getHost("WORKER1"))
            tab1_e1 = self.site1.selectFromTestTab(conn, testTab1)

        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        print time.ctime(), '----> secondary(site2) takeover ...'
        self.site2.srTakeover(self.site2.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.PRIMARY

        if self.site1.fullSync:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False

        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site2)

        #pdb.set_trace()
        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e2 = self.site2.selectFromTestTab(conn1, testTab1)

            conn2 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e2 = self.site2.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site2.setUpConnection(self.site2.getHost("WORKER1"))
            tab1_e2 = self.site2.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e2, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e2, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e2, "table was not transferred correctly")

        #pdb.set_trace()
        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site2, self.site3, self.site1)

        print time.ctime(), '----> fail back ...'
        self.site2.srChangeMode(self.site2.getHost("WORKER1"), 'sync')
        self.site2.srEnable(self.site2.getHost("WORKER1"), self.site2.getSiteName())
        self.site3.srRegister(self.site3.getHost("WORKER1"), self.site3.getSiteName(), self.site3.remoteInstance, self.site3.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode_1'])
        self.site3.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site3)
        self.checkSecondaryActive(self.site1)

        # bug 100675, 102700, restart middle tier, wait until started, check sync, restart tier3
        print time.ctime(), '----> restart middler tier, and tier3 ...'
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()
        self.site2.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site2)
        self.checkSecondaryActive(self.site1, self.site2)

        self.site3.stopDatabaseLandscapeAsWhole()
        self.site3.waitForDatabaseLandscapeStopped()
        self.site3.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site3)
        self.checkSecondaryActive(self.site1)

        if self.globalCfg['full_sync']:
            self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = True

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test100Failback2(self):
        '''Failback scenario II: After takeover from Primary (Site1) to Tier-2 Secondary (Site2) restore original setup Site1 - Site2 - Site3
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        testTab1 = "test090Failback21"
        testTab2 = "test090Failback22"

        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            self.site1.createTestTab(conn1, testTab1, self.site1.getHost("WORKER1"))
            tab1_t1e1 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            self.site1.createTestTab(conn2, testTab2, self.site1.getHost("WORKER1"))
            tab2_t2e1 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            self.site1.createTestTab(conn, testTab1, self.site1.getHost("WORKER1"))
            tab1_e1 = self.site1.selectFromTestTab(conn, testTab1)

        #self.site1.stopDatabaseLandscape(2) # kill
        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        print time.ctime(), '----> secondary(site2) takeover ...'
        self.site2.srTakeover(self.site2.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.PRIMARY

        if self.site1.fullSync:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False

        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site2)

        #pdb.set_trace()
        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e2 = self.site2.selectFromTestTab(conn1, testTab1)

            conn2 = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e2 = self.site2.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site2.setUpConnection(self.site2.getHost("WORKER1"))
            tab1_e2 = self.site2.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e2, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e2, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e2, "table was not transferred correctly")

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site2, self.site1, self.site3)

        #pdb.set_trace()
        print time.ctime(), '----> site3 unregister ...'
        self.site3.stopDatabaseLandscapeAsWhole()
        self.site3.waitForDatabaseLandscapeStopped()
        self.site3.srUnregister(self.site3.getHost("WORKER1"))
        # start to finish unregister, necessary
        self.site3.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site3)

        print time.ctime(), '----> site1 register to site2 ...'
        self.site2.setRemoteSystem(self.site1.getInstance()["instance_id"], self.site1.getLandscape()[(self.site1.getHost("WORKER1"), "hostname")].split(".")[0])
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()
        self.site2.srRegister(self.site2.getHost("WORKER1"), self.site2.getSiteName(), self.site2.remoteInstance, self.site2.remoteHost, self.globalCfg['sync_mode'], self.globalCfg['op_mode'])
        self.site2.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site2)

        self.checkSecondaryActive(self.site1)

        print time.ctime(), '----> secondary(site2) takeover ...'
        self.site2.srTakeover(self.site2.getHost("WORKER1"))

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site2, self.site1, self.site3)

        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e3 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e3 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            tab1_e3 = self.site1.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e3, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e3, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e3, "table was not transferred correctly")

        print time.ctime(), '----> site2 register to site1 ...'
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site3.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()
        self.site2.srRegister(self.site2.getHost("WORKER1"), self.site2.getSiteName(), self.site2.remoteInstance, self.site2.remoteHost, self.globalCfg['sync_mode'], self.globalCfg['op_mode'])
        self.site2.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site2)

        print time.ctime(), '----> site3 register to site2 ...'
        if self.site1.dtMode != dtMode.NONE:
            self.checkSecondaryActive(self.site1)
        self.site2.srEnable(self.site2.getHost("WORKER1"), self.site2.getSiteName())
        self.site3.waitForDatabaseLandscapeStopped()
        self.site3.srRegister(self.site3.getHost("WORKER1"), self.site3.getSiteName(), self.site3.remoteInstance, self.site3.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode_1'])

        self.site3.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site3)

        if self.globalCfg['full_sync']:
            self.site1.srEnableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = True

        print time.ctime(), '----> check table sync ...'
        if self.globalCfg['withCustomTenant']:
            conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
            tab1_t1e4 = self.site1.selectFromTestTab(conn1, testTab1)

            conn2 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname2'])
            tab2_t2e4 = self.site1.selectFromTestTab(conn2, testTab2)
        else:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"))
            tab1_e4 = self.site1.selectFromTestTab(conn, testTab1)

        if self.globalCfg['withCustomTenant']:
            self.assertExpected(tab1_t1e1, tab1_t1e4, "table was not transferred correctly")
            self.assertExpected(tab2_t2e1, tab2_t2e4, "table was not transferred correctly")
        else:
            self.assertExpected(tab1_e1, tab1_e4, "table was not transferred correctly")

        self.checkSecondaryActive(self.site1)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test105RestartAllTiers280(self):
        '''Just one after the other restart each tier and see, if system replication gets in sync again after each restart.
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        print time.ctime(), '----> restarting tier3 and check'
        self.site3.stopDatabaseLandscapeAsWhole()
        self.site3.waitForDatabaseLandscapeStopped()
        self.site3.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site3)
        self.checkSecondaryActive(self.site1)

        print time.ctime(), '----> restarting tier2 and check'
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()
        self.site2.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site2)
        self.checkSecondaryActive(self.site1)

        print time.ctime(), '----> restarting tier1 and check'
        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()
        self.site1.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site1)
        self.checkSecondaryActive(self.site1)

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site1, self.site2, self.site3)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test106CheckSecondaryAlerts(self):
        '''stop tier-3 secondary, verify that alerts have been created on tier-2 secondary by selecting from proxy view on primary and reconnect tier-3 secondary,check that the alerts disappeard
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        print time.ctime(), '----> Set check interval for Alert 78  to a high value'
        conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
        cur = conn.cursor()
        cur.execute("UPDATE  _SYS_STATISTICS.STATISTICS_SCHEDULE SET INTERVALLENGTH=30000 WHERE ID =78")

        print time.ctime(), '----> stopping tier-3 secondary create connection closed alerts on tier-2'
        self.site3.stopDatabaseLandscape(2)
        print time.ctime(), '----> wait for stoping db...'
        self.site3.waitForDatabaseLandscapeStopped()
        print time.ctime(), '----> Verify that alerts have been created on tier-2 secondary by selecting from proxy view on primary '
        sql = '''SELECT STATE,ACKNOWLEDGED  FROM "_SYS_SR_SITE_''' +  self.site2.getSiteName() + '''".M_EVENTS WHERE TYPE = 'SRConnectionClosed' '''
        conn = self.site1.connectToNextBestHost()
        cur = conn.cursor()
        cur.execute(sql)
        #for each service there should be an event with STATE NEW
        e = cur.fetchall()
        print e
        self.assertTrue(len(e) > 0,"Alert with id = 78 is not find !")
        for it in e:
            self.assertTrue(it[0] == "NEW","Alerts state should be NEW")

        if self.globalCfg['withInitTenant']:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
            cur = conn.cursor()
            cur.execute(sql)
            #for each service there should be an event with STATE NEW and ACKNOWLEDGED FALSE
            e = cur.fetchall()
            print e
            self.assertTrue(len(e) > 0,"Alert with id = 78 is not find !")
            for it in e:
                self.assertTrue(it[0] == "NEW","Alerts state should be NEW")

        print "Set check interval of alert to a small value to make it run in a few seconds"
        #Set check interval of alert to a small value to make it run in a few seconds
        conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
        cur = conn.cursor()
        cur.execute(''' UPDATE  _SYS_STATISTICS.STATISTICS_SCHEDULE SET INTERVALLENGTH=5 WHERE ID =78 ''')

        alertErrorMsg = "Success"
        alert_Timeout = self.site1.getTimeout("databasestart")
        t0 = t1 = time.time()
        while t1 - t0 < alert_Timeout:
            time.sleep(2)
            t1 = time.time()
            conn = self.site1.connectToNextBestHost()
            cur = conn.cursor()
            cur.execute(sql)
            #for each service there should be an event with STATE 'NEW' and ACKNOWLEDGED 'TRUE'
            e = cur.fetchall()
            if len(e) == 0:
                alertErrorMsg = "Alert with id = 78 is not find !"
                continue
            for it in e:
                if it[0] != "NEW":
                    alertErrorMsg = "Alerts state should be NEW"
                    continue
                if it[1] != "TRUE":
                    alertErrorMsg = "Alerts ACKNOWLEDGED should be TRUE"
                    continue
                else:
                    alertErrorMsg = "Success"
            if alertErrorMsg != "Success":
                continue

            if self.globalCfg['withInitTenant']:
                print "check withInitTenant..."
                conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
                cur = conn.cursor()
                cur.execute(sql)
                e = cur.fetchall()
                if len(e) == 0:
                    alertErrorMsg = "Alert with id = 78 is not find !"
                    continue
                for it in e:
                    if it[0] != "NEW":
                        alertErrorMsg = "Alerts state should be NEW"
                        continue
                    if it[1] != "TRUE":
                        alertErrorMsg = "Alerts ACKNOWLEDGED should be TRUE"
                        continue
                    else:
                        alertErrorMsg = "Success"
            if alertErrorMsg == "Success":
                break
        if (t1 - t0) >= alert_Timeout:
            sys.stdout.write(" Check alerts failed [%s secs]\n" % int(t1 - t0))
            sys.stdout.flush()

        self.assertTrue(alertErrorMsg == "Success",alertErrorMsg)

        #There should be also an entry in _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS with the newly created alert
        conn = self.site1.connectToNextBestHost()
        cur = conn.cursor()
        cur.execute("SELECT * FROM  _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS WHERE alert_id = 78")
        #current alerts contain the alerts, that have been created in the last check run
        e = cur.fetchall()
        self.assertTrue(len(e) > 0,"current alerts should contain the alerts with id = 78 ,but not find")

        if self.globalCfg['withInitTenant']:
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
            cur = conn.cursor()
            cur.execute("SELECT * FROM  _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS WHERE alert_id = 78")
            e = cur.fetchall()
            self.assertTrue(len(e) > 0,"current alerts should contain the alerts with id = 78 ,but not find")

        print time.ctime(), '----> restart tier3 ...'
        self.site3.startDatabaseLandscapeAsWhole()
        print time.ctime(), '----> wait for starting db...'
        self.waitForDatabaseLandscapeStartedByPY(self.site3)
        print time.ctime(), '----> Check alerts by polling m_events and later current alerts'

        t0 = t1 = time.time()
        while t1 - t0 < alert_Timeout:
            time.sleep(2)
            t1 = time.time()
            conn = self.site1.connectToNextBestHost()
            cur = conn.cursor()
            cur.execute(sql)
            #All connection closed events in M_EVENTS on tier-2 should be removed now; select should return no rows
            e = cur.fetchall()
            if len(e) != 0:
                alertErrorMsg = "All connection closed events in M_EVENTS should be removed, but not!"
                continue
            cur.execute("SELECT * FROM  _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS WHERE alert_id = 78")
            e = cur.fetchall()
            if len(e) != 0:
                alertErrorMsg = "All connection closed events in STATISTICS_CURRENT_ALERTS should be removed, but not!"
                continue
            else:
                alertErrorMsg = "Success"

            if self.globalCfg['withInitTenant']:
                print "check withInitTenant..."
                conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
                cur = conn.cursor()
                cur.execute("SELECT * FROM  _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS WHERE alert_id = 78")
                e = cur.fetchall()
                print e
                if len(e) != 0:
                    alertErrorMsg = "All connection closed events in STATISTICS_CURRENT_ALERTS should be removed, but not!"
                    continue
                else:
                    alertErrorMsg = "Success"
            if alertErrorMsg == "Success":
                break
        if (t1 - t0) >= alert_Timeout:
            sys.stdout.write(" Check remove alerts failed [%s secs]\n" % int(t1 - t0))
            sys.stdout.flush()

        self.assertTrue(alertErrorMsg == "Success",alertErrorMsg)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test180Recovery(self):
        '''Recovery with Data Backup from Site1 and Log Backups from Site1, Site2 and Site3 after Takeover
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        #pdb.set_trace()
        self.site1.runProgramInGuest(self.site1.getHost("WORKER1"), "rm $DIR_INSTANCE/work/recoverInstance.sem -f", siduser = True)
        self.site2.runProgramInGuest(self.site2.getHost("WORKER1"), "rm $DIR_INSTANCE/work/recoverInstance.sem -f", siduser = True)

        # test1: generate log
        conn = self.site1.connectToNextBestHost()
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE massdata")
        except:
            pass
        cur.execute("CREATE TABLE massdata (id INT, test VARCHAR(50))")
        cur.execute("INSERT INTO massdata VALUES (1, 'fgfdhh')")

        sys.stdout.write("%s generating data (2^14): " % time.ctime())
        sys.stdout.flush()
        for i in range(1, 15):
            sys.stdout.write("%s:" % i)
            sys.stdout.flush()
            cur.execute("INSERT INTO massdata SELECT * FROM massdata")
        sys.stdout.write("done\n")
        sys.stdout.flush()

        cur.execute("SELECT Count(*) FROM massdata")
        e = cur.fetchall()
        print time.ctime(), "inserted %s rows" % e[0][0]
        cur.execute("SELECT * FROM massdata")
        e_md1 = cur.fetchall()

        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        self.site2.srTakeover(self.site2.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.PRIMARY

        # copy data backup of site1
        command = "scp -r $DIR_INSTANCE/backup/data/* %s@%s:$DIR_INSTANCE/backup/data" % (self.site2.getInstance()["sidadm"], self.site2.getLandscape()[(self.site2.getHost("WORKER1"), "hostname")].split(".")[0])
        (c, o) = self.site1.runProgramInGuest(self.site1.getHost("WORKER1"), command, siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c
        self.assertTrue(c == 0, "copying of data backup files failed")
        # copy log backups of site1
        command = "scp -r $DIR_INSTANCE/backup/log/* %s@%s:$DIR_INSTANCE/backup/log" % (self.site2.getInstance()["sidadm"], self.site2.getLandscape()[(self.site2.getHost("WORKER1"), "hostname")].split(".")[0])
        (c, o) = self.site1.runProgramInGuest(self.site1.getHost("WORKER1"), command, siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c
        self.assertTrue(c == 0, "copying of log backup files failed")

        # generate log on tier3 seconary
        conn = self.site2.connectToNextBestHost()
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE massdata2")
        except:
            pass
        cur.execute("CREATE TABLE massdata2 (id INT, test VARCHAR(50))")
        cur.execute("INSERT INTO massdata2 VALUES (1, 'fgfdhh')")

        sys.stdout.write("%s generating data (2^14): " % time.ctime())
        sys.stdout.flush()
        for i in range(1, 15):
            sys.stdout.write("%s:" % i)
            sys.stdout.flush()
            cur.execute("INSERT INTO massdata2 SELECT * FROM massdata2")
        sys.stdout.write("done\n")
        sys.stdout.flush()

        cur.execute("SELECT Count(*) FROM massdata2")
        e = cur.fetchall()
        print time.ctime(), "inserted %s rows" % e[0][0]
        cur.execute("SELECT * FROM massdata2")
        e_md2 = cur.fetchall()

        # recovery
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site3.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()
        self.site3.waitForDatabaseLandscapeStopped()
        self.site3.srUnregister(self.site3.getHost("WORKER1"))
        tomorrow = (datetime.datetime.utcnow() + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        print time.ctime(), "recovery until up-to-date, timestamp in utc %s" % tomorrow
        (c, o) = self.site2.runProgramInGuest(self.site2.getHost("WORKER1"), "python $DIR_INSTANCE/exe/python_support/recoverSys.py --command=\"RECOVER DATABASE UNTIL TIMESTAMP '\\''%s'\\''\"" % tomorrow, siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c
        self.assertTrue(c == 0, "recovery failed")
        self.site2.waitForDatabaseLandscapeStartedByPY()

        if self.globalCfg['withInitTenant']:
            self.site2.stopDatabase(self.site2.getInstance()["instance_name"])
            connSys = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname='SYSTEMDB')
            curSys = connSys.cursor()
            sapmnt = os.environ["DIR_INSTANCE"]
            sql = '''RECOVER DATABASE FOR %s UNTIL TIMESTAMP '%s' ''' % (self.site2.getInstance()["instance_name"], tomorrow)
            print 'recover cmd:', sql
            try:
                curSys.execute(sql)
            except Exception, e:
                self.fail('recover %s failed, error message: %s' % (self.site2.getInstance()["instance_name"], e))
            self.site2.startDatabase(self.site2.getInstance()["instance_name"])

        # bug 118191
        #pdb.set_trace()
        if self.globalCfg['withCustomTenant']:
            self.site2.stopDatabase(self.globalCfg['dbname1'])
            connSys = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname='SYSTEMDB')
            curSys = connSys.cursor()
            sapmnt = os.environ["DIR_INSTANCE"]
            sql = '''RECOVER DATABASE FOR %s UNTIL TIMESTAMP '%s' ''' % (self.globalCfg['dbname1'], tomorrow)
            print 'recover cmd:', sql
            try:
                curSys.execute(sql)
            except Exception, e:
                self.fail('recover %s failed, error message: %s' % (self.globalCfg['dbname1'], e))
            self.site2.startDatabase(self.globalCfg['dbname1'])

            self.site2.stopDatabase(self.globalCfg['dbname2'])
            connSys = self.site2.setUpConnection(self.site2.getHost("WORKER1"), dbname='SYSTEMDB')
            curSys = connSys.cursor()
            sapmnt = os.environ["DIR_INSTANCE"]
            sql = '''RECOVER DATABASE FOR %s UNTIL TIMESTAMP '%s' ''' % (self.globalCfg['dbname2'], tomorrow)
            print 'recover cmd:', sql
            try:
                curSys.execute(sql)
            except Exception, e:
                self.fail('recover %s failed, error message: %s' % (self.globalCfg['dbname2'], e))
            self.site2.startDatabase(self.globalCfg['dbname2'])

        self.site2.waitForDatabaseLandscapeStartedByPY()


        self.site3.srRegister(self.site3.getHost("WORKER1"), self.site3.getSiteName(), self.site3.remoteInstance, self.site3.remoteHost, self.globalCfg['sync_mode_1'], self.globalCfg['op_mode_1'])
        self.site3.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site3)
        self.checkSecondaryActive(self.site2)

        conn = self.site2.connectToNextBestHost()
        cur = conn.cursor()
        cur.execute("SELECT * FROM massdata")
        e_md1_check = cur.fetchall()
        cur.execute("SELECT * FROM massdata2")
        e_md2_check = cur.fetchall()

        print time.ctime(), '----> check table recoverd ...'
        self.assertEqual(e_md1, e_md1_check, "not all data was recovered successfully")
        self.assertEqual(e_md2, e_md2_check, "not all data was recovered successfully")

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site1, self.site2, self.site3)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test190HAFStopWorker(self):
        '''Host Auto-Failover scenario I: Stop active worker host on primary
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        if self.globalCfg['withCustomTenant']:
            print time.ctime(), '----> create tenant db only on worker2'
            self.site1.createTenantDBInRep(self.globalCfg['dbname3'], hostno=self.site1.getHost("WORKER2"))

        # prmary worker failover
        print time.ctime(), '----> failover ...'
        # If DT is there execute DT failover
        if self.site1.dtMode != dtMode.NONE:
            esProcessid = self.site1.getEsPid(self.site1.getHost("EXTENDED_STORAGE_STANDBY1"))
            print "esProcessid : %s" %esProcessid
            self.site1.stopInstance(self.site1.getHost("EXTENDED_STORAGE_WORKER1"),2)  # kill
            self.site1.waitForFailover(self.site1.getHost("EXTENDED_STORAGE_WORKER1"),newMaster=self.site1.getHost("EXTENDED_STORAGE_STANDBY1"),service="ESSERVER",externalServicePid=esProcessid)
            self.checkSecondaryActive(self.site1)

            self.site1.startInstance(self.site1.getHost("EXTENDED_STORAGE_WORKER1"))
            self.site1.waitForDatabaseStarted(self.site1.getHost("EXTENDED_STORAGE_WORKER1"))

            self.site1.assertActualLandscapeRoles({self.site1.getHost("WORKER1") :  "MASTER",
                                                   self.site1.getHost("EXTENDED_STORAGE_WORKER1") :  "EXTENDED_STORAGE_STANDBY",
                                                   self.site1.getHost("EXTENDED_STORAGE_STANDBY1") : "EXTENDED_STORAGE_WORKER"}, "INDEXSERVER")
            self.site1.assertActualLandscapeRoles({self.site1.getHost("WORKER1") :  "MASTER",
                                                   self.site1.getHost("EXTENDED_STORAGE_WORKER1") :  "SLAVE",
                                                   self.site1.getHost("EXTENDED_STORAGE_STANDBY1") : "SLAVE"}, "NAMESERVER")
        else:
            self.site1.stopInstance(self.site1.getHost("WORKER2"), 2)  # kill
            self.site1.waitForFailover(self.site1.getHost("WORKER2"))
            self.checkSecondaryActive(self.site1)

            self.site1.startInstance(self.site1.getHost("WORKER2"))
            self.site1.waitForDatabaseStarted(self.site1.getHost("WORKER2"))

            self.site1.assertActualLandscapeRoles({self.site1.getHost("WORKER1") :  "MASTER",
                                               self.site1.getHost("WORKER2") :  "STANDBY",
                                               self.site1.getHost("STANDBY1") : "SLAVE"}, "INDEXSERVER")
            self.site1.assertActualLandscapeRoles({self.site1.getHost("WORKER1") :  "MASTER",
                                               self.site1.getHost("WORKER2") :  "SLAVE",
                                               self.site1.getHost("STANDBY1") : "SLAVE"}, "NAMESERVER")

        if self.globalCfg['withCustomTenant']:
            print time.ctime(), '----> drop tenant db only on worker2'
            self.copyDatabaseTrace(self.globalCfg['dbname3'])
            self.site1.dropDatabase(self.globalCfg['dbname3'])

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True


    @classification("with_restart", "barrier")
    def test200HAFStopMaster(self):
        '''In Host Auto-Failover HANA only: Stop master node on primary and auto-failover
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        # prmary master failover
        print time.ctime(), '----> failover ...'
        self.site1.waitForDatabaseLandscapeStartedByPY() # reassure that standby is fully started before stopping the master
        self.site1.stopInstance(self.site1.getHost("WORKER1"), 2)  # kill
        self.site1.waitForFailover(self.site1.getHost("WORKER1"), self.site1.getHost("STANDBY1"))
        self.checkSecondaryActive(self.site1)

        self.site1.startInstance(self.site1.getHost("WORKER1"))
        self.site1.waitForDatabaseStarted(self.site1.getHost("WORKER1"))

        self.site1.assertActualLandscapeRoles({self.site1.getHost("WORKER1") :  "STANDBY",
                                               self.site1.getHost("WORKER2") :  "SLAVE",
                                               self.site1.getHost("STANDBY1") : "MASTER"}, "INDEXSERVER")
        self.site1.assertActualLandscapeRoles({self.site1.getHost("WORKER1") :  "SLAVE",
                                               self.site1.getHost("WORKER2") :  "SLAVE",
                                               self.site1.getHost("STANDBY1") : "MASTER"}, "NAMESERVER")

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True


    @classification("with_restart", "barrier")
    def test160CreateTenantDB(self):
        '''2-Tier-System Replication is running; newly created tenant DBs are integrated
           in replication after a backup of the tenant was made
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        # create(drop if exists), start, backup, check
        if self.site1.isMultiHostSystem():
            self.site1.createTenantDBInRep(self.globalCfg['dbname1'], multiNode=True)
        else:
            self.site1.createTenantDBInRep(self.globalCfg['dbname1'])
        self.site1.createTenantDBInRep(self.globalCfg['dbname2'], hostno=self.site1.getHost("WORKER1"))

        self.site1.databaseLandscapeInfo()
        self.site2.databaseLandscapeInfo()

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test250RecoveryTenantDB(self):
        '''Data Recovery of tenant DB in MDC
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        assert self.site1._multiDBInstance

        #pdb.set_trace()
        testTab1 = "test250RecoveryTenantDB1"

        conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
        self.site1.createTestTab(conn1, testTab1, self.site1.getHost("WORKER1"))
        tab1_t1e1 = self.site1.selectFromTestTab(conn1, testTab1)

        print time.ctime(), '----> backup tenant db'
        bkFilePrefix = self.globalCfg['dbname1'] + '_TENANT_BK1'
        self.site1.backupTenantDB(self.globalCfg['dbname1'], bkFilePrefix)

        print time.ctime(), '----> drop table'
        cur = conn1.cursor()
        cur.execute("DROP TABLE %s" % testTab1)

        self.checkSecondaryActive(self.site1)

        print time.ctime(), '----> recover tenant db'
        self.site1.stopDatabase(self.globalCfg['dbname1'])
        connSys = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
        curSys = connSys.cursor()
        sapmnt = os.environ["DIR_INSTANCE"]
        sql = '''RECOVER DATA FOR %s USING FILE ('%s/backup/data/DB_%s/%s') CLEAR LOG''' % (self.globalCfg['dbname1'], sapmnt, self.globalCfg['dbname1'], bkFilePrefix)
        print 'recover cmd:', sql
        try:
            curSys.execute(sql)
        except Exception, e:
            self.fail('recover %s failed, error message: %s' % (self.globalCfg['dbname1'], e))
        self.site1.startDatabase(self.globalCfg['dbname1'])

        self.checkSecondaryActive(self.site1)

        print time.ctime(), '----> check table recovered ...'
        conn1 = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname=self.globalCfg['dbname1'])
        tab1_t1e2 = self.site1.selectFromTestTab(conn1, testTab1)

        self.assertExpected(tab1_t1e1, tab1_t1e2, "table was not recovered correctly")

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True


    @classification("with_restart", "barrier")
    def test300DropTenantDB(self):
        '''2-Tier-System Replication is running; dropped tenant DBs are removed from replication
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        hostList = []
        if self.site1.isMultiHostSystem():
            hostName1 = self.site1.getLandscape()[(self.site1.getHost("WORKER1"), "hostname")]
            hostName2 = self.site1.getLandscape()[(self.site1.getHost("WORKER2"), "hostname")]
            hostList = [hostName1, hostName2]

        # stop, check tenant db
        self.site1.stopDatabase(self.globalCfg['dbname1'])
        # if multihosts, tdb1 is distributed
        self.site1.checkTenantDBRepStatus(self.globalCfg['dbname1'], isActive=False, multihosts=hostList)
        self.site1.startDatabase(self.globalCfg['dbname1'])
        self.site1.checkTenantDBRepStatus(self.globalCfg['dbname1'], multihosts=hostList)
        # start, check tenant db
        self.site1.stopDatabase(self.globalCfg['dbname2'])
        self.site1.checkTenantDBRepStatus(self.globalCfg['dbname2'], isActive=False)
        self.site1.startDatabase(self.globalCfg['dbname2'])
        self.site1.checkTenantDBRepStatus(self.globalCfg['dbname2'])

        self.copyDatabaseTrace(self.globalCfg['dbname1'])
        self.copyDatabaseTrace(self.globalCfg['dbname2'])

        # stop and drop tenant db
        print time.ctime(), '----> stop and drop tenant db'
        self.site1.dropDatabase(self.globalCfg['dbname1'])
        self.site1.dropDatabase(self.globalCfg['dbname2'])

        # bug 100793, give secondary more time , sleep 2mins
        time.sleep(120)

        self.checkSecondaryActive(self.site1)

        #pdb.set_trace()
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test204RootKeyPropagation(self):
        '''checked the changed rootkey is propagated to all tiers in system replication.
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        # get the original rootkeys in all tiers
        print time.ctime(), '----> get the hashed rootkeys on all tiers'
        originKeysSite1 = self.site1.getHashedRootKeys()
        print time.ctime(), 'original Hashed RootKeys on site 1 is %s' % originKeysSite1
        originKeysSite2 = self.site2.getHashedRootKeys()
        print time.ctime(), 'original Hashed RootKeys on site 2 is %s' % originKeysSite2
        originKeysSite3 = self.site3.getHashedRootKeys()
        print time.ctime(), 'original Hashed RootKeys on site 3 is %s' % originKeysSite3

        print time.ctime(), '----> change the persistence/dbapi/log rootkeys and then get the new rootkeys'
        self.site1.changePersistenceRootKey()
        self.site1.changeDpApiRootKey()
        self.site1.changeLogRootKey()

        newKeysSite1 = self.site1.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 1 is %s' % newKeysSite1
        newKeysSite2 = self.site2.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 2 is %s' % newKeysSite2
        newKeysSite3 = self.site3.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 3 is %s' % newKeysSite3

        print time.ctime(), '----> verify the rootkeys propagation happened, i.e. rootkeys are changed on all tiers'
        if newKeysSite1.has_key('PERSISTENCE Root Key') and newKeysSite1.has_key('DPAPI Root Key'):
            self.assertTrue(originKeysSite1['PERSISTENCE Root Key'] != newKeysSite1['PERSISTENCE Root Key'], "the persistence rootkeys on tier 1 not changed!")
            self.assertTrue(originKeysSite2['PERSISTENCE Root Key'] != newKeysSite2['PERSISTENCE Root Key'], "the persistence rootkeys on tier 2 not changed!")
            self.assertTrue(originKeysSite3['PERSISTENCE Root Key'] != newKeysSite3['PERSISTENCE Root Key'], "the persistence rootkeys on tier 3 not changed!")
            self.assertTrue(originKeysSite1['DPAPI Root Key'] != newKeysSite1['DPAPI Root Key'], "the DpApi rootkeys on tier 1 not changed!")
            self.assertTrue(originKeysSite2['DPAPI Root Key'] != newKeysSite2['DPAPI Root Key'], "the DpApi rootkeys on tier 2 not changed!")
            self.assertTrue(originKeysSite3['DPAPI Root Key'] != newKeysSite3['DPAPI Root Key'], "the DpApi rootkeys on tier 3 not changed!")
            self.assertTrue(originKeysSite1['LOG Root Key'] != newKeysSite1['LOG Root Key'], "the LOG rootkeys on tier 1 not changed!")
            self.assertTrue(originKeysSite2['LOG Root Key'] != newKeysSite2['LOG Root Key'], "the LOG rootkeys on tier 2 not changed!")
            self.assertTrue(originKeysSite3['LOG Root Key'] != newKeysSite3['LOG Root Key'], "the LOG rootkeys on tier 3 not changed!")

        else:
            raise Exception("please check the return value pattern changes of 'hdbnsutil -printHashedRootKeys'")
        print time.ctime(), '----> verify the new rootkeys are the same on all tiers'
        self.assertTrue(newKeysSite1 == newKeysSite2 and newKeysSite2 == newKeysSite3, "new keys are different between tiers!")

        print time.ctime(), '----> restart the middle tier'
        self.site2.stopDatabaseLandscapeAsWhole()
        self.site2.waitForDatabaseLandscapeStopped()
        self.site2.startDatabaseLandscapeAsWhole()
        self.waitForDatabaseLandscapeStartedByPY(self.site2)
        self.checkSecondaryActive(self.site1, self.site2)

        print time.ctime(), '----> get the new keys after restart middle tier...'
        newKeysSite1 = self.site1.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 1 is %s' % newKeysSite1
        newKeysSite2 = self.site2.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 2 is %s' % newKeysSite2
        newKeysSite3 = self.site3.getHashedRootKeys()
        print time.ctime(), 'new Hashed RootKeys on site 3 is %s' % newKeysSite3

        print time.ctime(), '----> verify the new rootkeys is still different from the old rootkeys on all tiers'
        if newKeysSite1.has_key('PERSISTENCE Root Key'):
            self.assertTrue(originKeysSite1['PERSISTENCE Root Key'] != newKeysSite1['PERSISTENCE Root Key'], "the persistence rootkeys on tier 1 not changed!")
            self.assertTrue(originKeysSite2['PERSISTENCE Root Key'] != newKeysSite2['PERSISTENCE Root Key'], "the persistence rootkeys on tier 2 not changed!")
            self.assertTrue(originKeysSite3['PERSISTENCE Root Key'] != newKeysSite3['PERSISTENCE Root Key'], "the persistence rootkeys on tier 3 not changed!")
            self.assertTrue(originKeysSite1['DPAPI Root Key'] != newKeysSite1['DPAPI Root Key'], "the DpApi rootkeys on tier 1 not changed!")
            self.assertTrue(originKeysSite2['DPAPI Root Key'] != newKeysSite2['DPAPI Root Key'], "the DpApi rootkeys on tier 2 not changed!")
            self.assertTrue(originKeysSite3['DPAPI Root Key'] != newKeysSite3['DPAPI Root Key'], "the DpApi rootkeys on tier 3 not changed!")
            self.assertTrue(originKeysSite1['LOG Root Key'] != newKeysSite1['LOG Root Key'], "the LOG rootkeys on tier 1 not changed!")
            self.assertTrue(originKeysSite2['LOG Root Key'] != newKeysSite2['LOG Root Key'], "the LOG rootkeys on tier 2 not changed!")
            self.assertTrue(originKeysSite3['LOG Root Key'] != newKeysSite3['LOG Root Key'], "the LOG rootkeys on tier 3 not changed!")
        else:
            raise Exception("please check the return value pattern changes of 'hdbnsutil -printHashedRootKeys'")
        print time.ctime(), '----> verify the new rootkeys are the same on all tiers'
        self.assertTrue(newKeysSite1 == newKeysSite2 and newKeysSite2 == newKeysSite3, "new keys are different between tiers!")

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True


    @classification("with_restart", "barrier")
    def test201ErrorProvokeDVEKeyT2(self):
        '''check the rootkey is failed to change when depriving the write permission of ssfs DAT file in tier 2.
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False
        self.checkDVEKeyChangeErrorProvoke(self.site2)
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True



    @classification("with_restart", "barrier")
    def test202ErrorProvokeDVEKeyT3(self):
        '''check the rootkey is failed to change when depriving the write permission of ssfs DAT file in tier 3.
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False
        self.checkDVEKeyChangeErrorProvoke(self.site3)
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True

    @classification("with_restart", "barrier")
    def test203RootKeyVersionConsistencyAfterTakeOver(self):
        '''change rootkey on primary, then checked the rootkey version is the same on primary and secondaries after takeover
        '''
        print time.ctime(), '----> begin executing testcase'
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False

        print time.ctime(), '----> change rootkeys on all sites'
        self.site1.changePersistenceRootKey(dbname = 'TE1')
        self.site1.changePersistenceRootKey(dbname = 'TE2')
        self.site1.changeDpApiRootKey(dbname = 'TE1')
        self.site1.changeDpApiRootKey(dbname = 'TE2')

        print time.ctime(), '----> check the used rootkey version'
        rootKeyVersoin1DB1 = self.site1.getUsedRootKeyVersion(dbname = self.globalCfg['dbname1'])
        print time.ctime(), 'original used rootkey version of %s is %s' % (self.globalCfg['dbname1'], rootKeyVersoin1DB1)
        rootKeyVersoin1DB2 = self.site1.getUsedRootKeyVersion(dbname = self.globalCfg['dbname2'])
        print time.ctime(), 'original used rootkey version of %s is %s' % (self.globalCfg['dbname2'], rootKeyVersoin1DB2)


        self.site1.stopDatabaseLandscapeAsWhole()
        self.site1.waitForDatabaseLandscapeStopped()

        print time.ctime(), '----> secondary(site2) takeover ...'
        self.site2.srTakeover(self.site2.getHost("WORKER1"))
        self.site1.itsDRRole  = siteTypes.PRIMARY
        self.site2.itsDRRole  = siteTypes.PRIMARY

        if self.site2.fullSync:
            self.site2.srDisableFullSync(self.site2.getHost("WORKER1"))
            self.site2.fullSync = False
        if self.site1.fullSync:
            self.site1.srDisableFullSync(self.site1.getHost("WORKER1"))
            self.site1.fullSync = False

        print time.ctime(), '----> check secondary active'
        self.checkSecondaryActive(self.site2)

        print time.ctime(), '----> check the used rootkey version after take over'
        rootKeyVersoin2DB1 = self.site2.getUsedRootKeyVersion(dbname = self.globalCfg['dbname1'])
        print time.ctime(), 'used rootkey version of %s is %s' % (self.globalCfg['dbname1'], rootKeyVersoin2DB1)
        rootKeyVersoin2DB2 = self.site2.getUsedRootKeyVersion(dbname = self.globalCfg['dbname2'])
        print time.ctime(), 'used rootkey version of %s is %s' % (self.globalCfg['dbname2'], rootKeyVersoin2DB2)

        print time.ctime(), '----> check the rootkey version is not changed'
        self.assertTrue(rootKeyVersoin1DB1 == rootKeyVersoin2DB1, "before:the used rootkey of TE1 is changed!")
        self.assertTrue(rootKeyVersoin1DB2 == rootKeyVersoin2DB2, "the used rootkey of TE2 is changed!")
        print time.ctime(), '---OK'

        (self.site1, self.site2, self.site3) = self.restoreSiteRoles(self.site1, self.site2, self.site3)

        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True


    def eventParser(self, event, para, cur_ts):
        ''' check the ini parameter mismatch/replication event in m_events
            event = 'mismatch': check the mismatch log of the preset para between different sites, return [site2, site3]
            event = 'replicate': check the replicate log of the preset para between different sites, return [(site2, site3), (datetime1, datetime2)]
            cur_ts: current timestamp
        '''
        slist = []
        tlist = []
        if event == "mismatch":
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
            cur = conn.cursor()
            cur.execute('''select * from m_events where INFOTEXT like '%s' and INFOTEXT not like '%s' and STATE='NEW' and type='SRConfigurationParameterMismatch'  order by  CREATE_TIME DESC''' % ('%'+para+'%', '%[inifile_checker]/exclusion_%'))
            columns = [column[0] for column in cur.description]
            results = cur.fetchall()
            if results:
                for row in results:
                    res = dict(zip(columns, row))
                    ktime = res['CREATE_TIME']
                    ts = time.mktime(ktime.timetuple())
                    [site] = re.findall(r'.*Site (\S): parameter mismatch', res['INFOTEXT'])
                    if ts > cur_ts and site not in slist:
                       slist.append(site)
            return slist
        if event == "replicate":
            conn = self.site1.setUpConnection(self.site1.getHost("WORKER1"), dbname='SYSTEMDB')
            cur = conn.cursor()
            cur.execute('''select * from m_events where INFOTEXT like '%s' and INFOTEXT not like '%s' and type='SRConfigurationParameterMismatch'  order by  HANDLE_TIME DESC''' % ('%'+para+'%', '%[inifile_checker]/exclusion_%'))
            columns = [column[0] for column in cur.description]
            results = cur.fetchall()
            if results:
                for row in results:
                    res = dict(zip(columns, row))
                    if res['STATE'] == 'HANDLED':
                       ktime = res['HANDLE_TIME']
                       ts = time.mktime(ktime.timetuple())
                       [site] = re.findall(r'.*Site (\S): parameter mismatch', res['INFOTEXT'])
                       state = res['STATE']
                       if ts > cur_ts  and site not in slist:
                           slist.append(site)
                           tlist.append(ktime)
                return [slist, tlist]
            else:
                return True
        return None


    def eventChecker(self, event, para, cur_ts, target_sites):
        ''' check the ini parameter mismatch/replication is correctly traced in the nameserver trace
        '''
        parseResult = self.eventParser(event, para, cur_ts)
        errorMsg = ''
        if event == "mismatch":
           if parseResult:
              if set(parseResult)==set(target_sites):
                 print "the ini para %s inconsistency is checked on site %s" % (para, str(parseResult))
              else:
                 errorMsg = "the para '%s' inconsistency check is wrongly traced on at least one site, expected on %s, but on %s\n" % (para, target_sites, parseResult)
           else:
              if target_sites:
                 errorMsg = "found nothing relevant to para '%s' inconsistency info, but expect on sites %s\n" % (para, target_sites)
              else:
                 print "found nothing relevant to para '%s' inconsistency info as expected" % para
        if event == "replicate":
           if parseResult == True:
              print "if mismatch event is correctly traced for para %s, the event should be already handled and cleaned up" % para
           elif parseResult[0]:
              print "the ini para '%s' replication for site %s is traced at about %s" % (para, parseResult[0], parseResult[1])
              if set(parseResult[0])!=set(target_sites):
                 errorMsg = "the para '%s' replicate is wrongly traced on at least one site, expected on %s, but on %s\n" % (para, target_sites, parseResult[0])
           else:
              if target_sites:
                 print "there is nothing handled for para %s, but expect being handled on site %s" % (para, target_sites)
                 errorMsg = "there is nothing handled for para %s, but expect being handled on site %s\n" % (para, target_sites)
              else:
                 print "nothing handled as expected"
        return errorMsg


    def paraReplicateCheck(self, site, host, inifile, layer, section, para, expected, tdbname=None ):
        errorMsg = ''
        value = site.getConfigParameterPerLayer(site.getHost(host), inifile, layer, section, para, tdbname)
        if value != expected:
            errorMsg = "\"%s\" not correctly replicated on %s, expected value is %s, but get %s\n" % (para, site.__class__.__name__, expected, value)
        return errorMsg

    @classification("with_restart", "barrier")
    def test220INIParaReplication(self):
        '''
            triggerSite:  the site which trigger transaction frozen on primary
            mode: trigger mode, stop: stop secondary, disable_eth: disable secondary ethernet, kill_tdb: kill the tenant db on secondary
        '''
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = False
        interval = 30
        #checkDuration = 3*interval
        replicateTimeout = 2*interval
        result = {"inconsistentTraceCheck":False, "replicate":False, "errorMsg":''}
        cur_ts = time.time()

        #############################SYSTEM LEVEL############################################################
        print time.ctime(), '----> change/add some new entries in INI file on SYSTEM level...'
        ###########change the para value savepoint_interval_s from 200 to 300
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "persistence", "savepoint_interval_s", "300")
        ###########change the para value "abort_time" on CUSTOMER level from 400 to 450
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", "450")
        ###########add the para "maxchannels" and "async_free_threshold"
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "preprocessor.ini", "HOST", "lexicon", "abort_time", "300")
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "indexserver.ini", "CUSTOMER", "communication", "maxchannels", "5000")
        self.site1.setConfigParameterPerLayer(self.site1.getHost("STANDBY1"), "global.ini", "CUSTOMER", "memorymanager", "async_free_threshold", "50")
        ###########remove para "abort_time" on HOST level on site1.worker2
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER2"), "preprocessor.ini", "HOST", "lexicon", "abort_time", None)


        ###############################HOST LEVEL########################################################
        print time.ctime(), '----> add/remove entries in INI file on HOST level...'
        ######add the para "maxfilesize" on HOST level
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "HOST", "expensive_statement", "maxfilesize", "1500000")
        ######remove the para internal_support_user_limit
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER2"), "indexserver.ini", "HOST", "authorization", "internal_support_user_limit", None)
        ######change para "maxthreads"
        self.site1.setConfigParameterPerLayer(self.site1.getHost("STANDBY1"), "xsengine.ini", "HOST", "httpserver", "maxthreads", "400")

        if self._multiDBInstance and not self.globalCfg['withInitTenant']:
           ###############################TENANT DB LEVEL########################################################
           print time.ctime(), '----> add/remove entries in INI file on TENANT DB level...'
           ######add the para "flush_interval" on DATABASE level
           self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER2"), "indexserver.ini", "CUSTOMER", "sqltrace", "flush_interval", "10", self.globalCfg['dbname2'])
           ###########change the para value "table_lock_array_size" on DATABASE level from 500 to 550
           self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "xsengine.ini", "CUSTOMER", "transaction", "table_lock_array_size", "3",self.globalCfg['dbname1'])
           ######remove the para container_dop
           self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", None, self.globalCfg['dbname1'])
           ######change para "geocode"
           self.site1.setConfigParameterPerLayer(self.site1.getHost("STANDBY1"), "scriptserver.ini", "CUSTOMER", "adapter_operation_cache", "geocode", "20", self.globalCfg['dbname1'])



        ######add "savepoint_interval_s" (SYSTEM) and "async_free_threshold"(SYSTEM) to exclusion
        print time.ctime(), '----> add some paras to exclusion'
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "exclusion_global.ini/SYSTEM", "persistence/savepoint_interval_s, memorymanager/*")
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "exclusion_indexserver.ini/HOST", "authorization/internal_support_user_limit")
        if self._multiDBInstance and not self.globalCfg['withInitTenant']:
           self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "exclusion_indexserver.ini/DATABASE", "sqltrace/flush_interval")


        print time.ctime(), '----> set inifile cheker interval to %s s...' % interval
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "interval", interval)
        (c, o) = self.site1.runProgramInGuest(self.site1.getHost("WORKER1"), "hdbnsutil -reconfig", siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c, o

        print time.ctime(), '----> check ini parameter inconsistency events were traced in nameserver trace...'
        time.sleep(interval)
        errorMsg = self.eventChecker("mismatch", "savepoint_interval_s", cur_ts, [])
        errorMsg = errorMsg + self.eventChecker("mismatch", "async_free_threshold", cur_ts, [])
        errorMsg = errorMsg + self.eventChecker("mismatch", "maxchannels", cur_ts, ['2', '3'])
        errorMsg = errorMsg + self.eventChecker("mismatch", "abort_time", cur_ts, ['2', '3'])
        errorMsg = errorMsg + self.eventChecker("mismatch", "maxfilesize", cur_ts, ['2', '3'])
        errorMsg = errorMsg + self.eventChecker("mismatch", "internal_support_user_limit", cur_ts, [])
        errorMsg = errorMsg + self.eventChecker("mismatch", "maxthreads", cur_ts, ['2', '3'])
        if self._multiDBInstance and not self.globalCfg['withInitTenant']:
           errorMsg = errorMsg + self.eventChecker("mismatch", "container_dop", cur_ts, ['2', '3'])
           errorMsg = errorMsg + self.eventChecker("mismatch", "geocode", cur_ts, ['2', '3'])
           errorMsg = errorMsg + self.eventChecker("mismatch", "flush_interval", cur_ts, [])
        result['errorMsg'] = result['errorMsg'] +  errorMsg
        if not errorMsg:
           result["inconsistentTraceCheck"] = True

        print time.ctime(), '----> enable INI para repliation...'
        cur_ts = time.time()
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate", "true")
        (c, o) = self.site1.runProgramInGuest(self.site1.getHost("WORKER1"), "hdbnsutil -reconfig", siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c, o

        print time.ctime(), '----> get the parameters in other tiers and check the new added ones were correctly replicated...'
        t0 = t1 = time.time()
        while t1 - t0 < replicateTimeout:
            repErrorMsg = self.paraReplicateCheck(self.site2, "WORKER1", "global.ini", "CUSTOMER", "persistence", "savepoint_interval_s", "200")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "global.ini", "CUSTOMER", "persistence", "savepoint_interval_s", "200")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER1", "indexserver.ini", "CUSTOMER", "communication", "maxchannels", "5000")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "indexserver.ini", "CUSTOMER", "communication", "maxchannels", "5000")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "STANDBY1", "global.ini", "CUSTOMER", "memorymanager", "async_free_threshold", "")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "STANDBY1", "global.ini", "CUSTOMER", "memorymanager", "async_free_threshold", "")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER2", "preprocessor.ini", "HOST", "lexicon", "abort_time", "")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER2", "preprocessor.ini", "HOST", "lexicon", "abort_time", "")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER2", "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", "450")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER2", "preprocessor.ini", "CUSTOMER", "lexicon", "abort_time", "450")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER1", "preprocessor.ini", "HOST", "lexicon", "abort_time", "300")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "preprocessor.ini", "HOST", "lexicon", "abort_time", "300")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER1", "global.ini", "HOST", "expensive_statement", "maxfilesize", "1500000")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "global.ini", "HOST", "expensive_statement", "maxfilesize", "1500000")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER2", "indexserver.ini", "HOST", "authorization", "internal_support_user_limit", "2")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER2", "indexserver.ini", "HOST", "authorization", "internal_support_user_limit", "2")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "STANDBY1", "xsengine.ini", "HOST", "httpserver", "maxthreads", "400")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "STANDBY1", "xsengine.ini", "HOST", "httpserver", "maxthreads", "400")
            if self._multiDBInstance and not self.globalCfg['withInitTenant']:
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER2", "indexserver.ini", "CUSTOMER", "sqltrace", "flush_interval", "", self.globalCfg['dbname2'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER2", "indexserver.ini", "CUSTOMER", "sqltrace", "flush_interval", "", self.globalCfg['dbname2'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER1", "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "", self.globalCfg['dbname1'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "", self.globalCfg['dbname1'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "STANDBY1", "scriptserver.ini", "CUSTOMER", "adapter_operation_cache", "geocode", "20", self.globalCfg['dbname1'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "STANDBY1", "scriptserver.ini", "CUSTOMER", "adapter_operation_cache", "geocode", "20", self.globalCfg['dbname1'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER1", "xsengine.ini", "CUSTOMER", "transaction", "table_lock_array_size", "3", self.globalCfg['dbname1'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "xsengine.ini", "CUSTOMER", "transaction", "table_lock_array_size", "3", self.globalCfg['dbname1'])
            if not repErrorMsg:
               result["replicate"] = True
               break
            time.sleep(1)
            t1 = time.time()
        if t1 - t0 >= replicateTimeout:
            sys.stdout.write(" failed [%s secs]\n" % int(t1 - t0))
            sys.stdout.flush()
            result['errorMsg'] = result['errorMsg'] +  repErrorMsg

        print time.ctime(), '----> disable INI parameter inconsistency check on T3'
        cur_ts = time.time()
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate", "false")
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "enable_tier_3", "false")
        print time.ctime(), '----> change some paras and disable INI para replicate on one of the secondaries'
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "indexserver.ini", "CUSTOMER", "communication", "maxchannels", "4000")
        self.site1.setConfigParameterPerLayer(self.site1.getHost("STANDBY1"), "xsengine.ini", "HOST", "httpserver", "maxthreads", "300")
        if self._multiDBInstance and not self.globalCfg['withInitTenant']:
           self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "3", self.globalCfg['dbname1'])

        (c, o) = self.site1.runProgramInGuest(self.site1.getHost("WORKER1"), "hdbnsutil -reconfig", siduser = True, returnOutput = True)
        print time.ctime(), "rc=%s" % c, o

        print time.ctime(), '----> check ini parameter inconsistency events were only traced in T2...'
        time.sleep(interval)
        errorMsg = self.eventChecker("mismatch", "maxchannels", cur_ts, ['2'])
        errorMsg = errorMsg + self.eventChecker("mismatch", "maxthreads", cur_ts, ['2'])
        if self._multiDBInstance and not self.globalCfg['withInitTenant']:
           errorMsg = errorMsg + self.eventChecker("mismatch", "container_dop", cur_ts, ['2'])
        if  errorMsg:
           result['errorMsg'] = result['errorMsg'] + 'AFTER disabled inifilechecker on T3: \n' +  errorMsg
           result["inconsistentTraceCheck"] = False


        print time.ctime(), '----> re-enable INI parameter replicate but disable replicate on T3'
        cur_ts = time.time()
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate", "true")
        self.site1.setConfigParameterPerLayer(self.site1.getHost("WORKER1"), "global.ini", "CUSTOMER", "inifile_checker", "replicate_tier_3", "false")

        print time.ctime(), '----> get the parameters in other tiers and check the changed/added/removed ones were correctly replicated on only T2...'
        t0 = t1 = time.time()
        while t1 - t0 < replicateTimeout:
            repErrorMsg =  self.paraReplicateCheck(self.site2, "WORKER1", "indexserver.ini", "CUSTOMER", "communication", "maxchannels", "4000")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "indexserver.ini", "CUSTOMER", "communication", "maxchannels", "5000")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "STANDBY1", "xsengine.ini", "HOST", "httpserver", "maxthreads", "300")
            repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "STANDBY1", "xsengine.ini", "HOST", "httpserver", "maxthreads", "400")
            if self._multiDBInstance and not self.globalCfg['withInitTenant']:
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site2, "WORKER1", "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "3", self.globalCfg['dbname1'])
               repErrorMsg = repErrorMsg + self.paraReplicateCheck(self.site3, "WORKER1", "scriptserver.ini", "CUSTOMER", "row_engine", "container_dop", "", self.globalCfg['dbname1'])
            if not repErrorMsg:
               break
            time.sleep(1)
            t1 = time.time()
        if t1 - t0 >= replicateTimeout:
            sys.stdout.write(" failed [%s secs]\n" % int(t1 - t0))
            sys.stdout.flush()
            result['errorMsg'] = result['errorMsg'] + 'AFTER disabled inifile replicate on T3: \n'+ repErrorMsg



        if not result["inconsistentTraceCheck"] or not result["replicate"]:
            self.fail("Test FAILED, check the details:\n %s" % result['errorMsg'])
        testHaDR_AdvancedParameters_MultiTier.testRunSuccess = True



if __name__ == '__main__':
    HaTestRealMachines.runTest(testHaDR_AdvancedParameters_MultiTier)


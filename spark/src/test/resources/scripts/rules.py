from datetime import timedelta, date, datetime
import re
from KanariDigital import KanariDataHandler

def depth3Test(context, str):
    return str
    

def depth2Test(context, str):
    depth3Test(context, str)
    return str

def UnitTestMethod(context, str):
  depth2Test(context, str)
  return str

KanariDataHandler.init(spark, "OrderObject")

wd = KanariDataHandler.Loader.loadPythonFile("/modules/WorkingDays.py")
wd.workday(datetime.utcnow(), 9)

def SourceRegionRule(hliDictObj, stateObj):
    
    # -------------------------------------------------------------------------
    #  Set the Varaibles
    # -------------------------------------------------------------------------
    # Load the ruleMap that is in hdfs://kanari-user-storage
    ruleMap = KanariDataHandler.Loader.loadDictionary("ruleMap.json")
    S4EMEA = ruleMap["S4RegionPlants"]["EMEA"]
    str = "sourceregion"
    # -------------------------------------------------------------------------
    #  S4
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        # EMEA SalesPlants
        if hliDictObj.get("plant") in S4EMEA:
            hliDictObj[str] = "EMEA"
        # AMS SalesPlants  no final list yet
        # APJ SalesPlants  no final list yet
    # -------------------------------------------------------------------------
    #  fusion
    # -------------------------------------------------------------------------
    elif hliDictObj.get("client") == '010':
        hliDictObj[str] = "EMEA"
    elif hliDictObj.get("client") == '007':
        hliDictObj[str] = "AMS"
    elif hliDictObj.get("client") == '016':
        hliDictObj[str] = "APJ"
    return hliDictObj


def FlowTypeRule(hliDictObj, stateObj):
    flowstr = "flowtype"
    # Load the ruleMap that is in hdfs://kanari-user-storage
    ruleMap = KanariDataHandler.Loader.loadDictionary("ruleMap.json")
    Services = ruleMap["itemcategory"]["Services"]
    ESW = ruleMap["itemcategory"]["E-SW"]
    OtherNP = ruleMap["itemcategory"]["OtherNP"]
    # -------------------------------------------------------------------------
    #  Set the FusionBridge Varaibles
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '010':
        FusionBridge = ruleMap["FusionBridgeFulfillment"]["emea"]
    elif hliDictObj.get("client") == '016':
        FusionBridge = ruleMap["FusionBridgeFulfillment"]["apj"]
    elif hliDictObj.get("client") == '007':
        FusionBridge = ruleMap["FusionBridgeFulfillment"]["ams"]
    # -------------------------------------------------------------------------
    #  Set the ICLinked Varaibles
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '010':
        ICLinked = ruleMap["ICLinked"]["emeaSupplier"]
    elif hliDictObj.get("client") == '016':
        ICLinked = ruleMap["ICLinked"]["apjSupplier"]
    elif hliDictObj.get("client") == '007':
        ICLinked = ruleMap["ICLinked"]["amsSupplier"]
    # -------------------------------------------------------------------------
    #  Set the FusionReverse Varaibles
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '010':
        FusionReverse = ruleMap["FusionReverseShipPoint"]["emea"]
    # ApjFusionReverse = ruleMap["FusionReverseShipPoint"]["apj"]
    # AmsFusionReverse = ruleMap["FusionReverseShipPoint"]["ams"]
    # -------------------------------------------------------------------------
    #  Set the PO Varaibles
    # -------------------------------------------------------------------------
    # default values
    poTypeList = []
    poPlant = ''
    poPlantSerial = ''
    consolPlant = ''
    consolPlantSerial = ''
    # iterate through 'purchase_list'
    for POs in hliDictObj['purchase_list']:
        if POs.get("deletionindicator") != 'X':
            poTypeList.append(POs.get("supplierpotype", ''))
            if POs.get("supplierpotype", '') == 'NB':
                if poPlantSerial == '':
                    poPlant = POs.get("factoryplant", '')
                    poPlantSerial = POs.get("serial", '')
                elif POs.get("serial", '') > poPlantSerial:
                    poPlant = POs.get("factoryplant", '')
                    poPlantSerial = POs.get("serial", '')
            if POs.get("supplierpotype", '') == 'UB':
                if consolPlantSerial == '':
                    consolPlant = POs.get("factoryplant", '')
                    consolPlantSerial = POs.get("serial", '')
                elif POs.get("serial", '') > consolPlantSerial:
                    consolPlant = POs.get("factoryplant", '')
                    consolPlantSerial = POs.get("serial", '')
    # -------------------------------------------------------------------------
    #  S4 rules must run first in this order
    # -------------------------------------------------------------------------
    # NonPhysical
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        # Service lineitems, no physical flow
        if hliDictObj.get("itemcategory") in Services:
            hliDictObj[flowstr] = "Services"
        # ElectronicSoftware lineitems, no physical flow
        elif hliDictObj.get("itemcategory") in ESW:
            hliDictObj[flowstr] = "E-SW"
        # no physical hardware, software bill only: ZDDB, ZIWB, ZKWB, ZWSB
        # Marketing: ZIMS, ZKMS, ZSMS)
        elif hliDictObj.get("itemcategory") in OtherNP:
            hliDictObj[flowstr] = "OtherNonPhysical"
    # -------------------------------------------------------------------------
    # Physical
    # -------------------------------------------------------------------------
        elif 'NB' not in poTypeList:
            hliDictObj[flowstr] = "NotAssigned"
        else:
            # -----------------------------------------------------------------
            # S4ReverseBridgeHub
            # -----------------------------------------------------------------
            if poPlant == '2C1A' and hliDictObj.get("ordertype") == 'ZICS':
                hliDictObj[flowstr] = "BridgeFulfilledHub"
            # -----------------------------------------------------------------
            # S4ReverseBridgeDirect
            # -----------------------------------------------------------------
            elif hliDictObj.get("ordertype") == 'ZICS':
                hliDictObj[flowstr] = "BridgeFulfilledDirect"
            # -----------------------------------------------------------------
            # S4BridgeS4Hub
            # -----------------------------------------------------------------
            elif poPlant == '2C1A':
                hliDictObj[flowstr] = "BridgeICHub"
            # -----------------------------------------------------------------
            # S4BridgeHub
            # -----------------------------------------------------------------
            elif(hliDictObj.get("plant") == poPlant and
                 hliDictObj.get("compldelflag") == 'X'):
                hliDictObj[flowstr] = "BridgeHub"
            # -----------------------------------------------------------------
            # S4Hub
            # -----------------------------------------------------------------
            elif consolPlant == '2C1A':
                hliDictObj[flowstr] = "Hub"
            # -----------------------------------------------------------------
            # S4BridgeDirect
            # -----------------------------------------------------------------
            elif hliDictObj.get("plant") == poPlant:
                hliDictObj[flowstr] = "BridgeDirect"
            # -----------------------------------------------------------------
            # S4Direct
            # -----------------------------------------------------------------
            elif hliDictObj.get("plant") != poPlant:
                hliDictObj[flowstr] = "Direct"
    # -------------------------------------------------------------------------
    #  Fusion rules must run first in this order
    # -------------------------------------------------------------------------
    # NonPhysical
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") in ('010', '007', '016'):
        # Service lineitems, no physical flow
        if hliDictObj.get("itemcategory") in Services:
            hliDictObj[flowstr] = "Services"
        # ElectronicSoftware lineitems, no physical flow
        elif hliDictObj.get("itemcategory") in ESW:
            hliDictObj[flowstr] = "E-SW"
        # no physical hardware, software - bill only: ZDDB, ZIWB, ZKWB, ZWSB
        # Marketing: ZIMS, ZKMS, ZSMS)
        elif hliDictObj.get("itemcategory") in OtherNP:
            hliDictObj[flowstr] = "OtherNonPhysical"
    # -------------------------------------------------------------------------
    # Physical
    # -------------------------------------------------------------------------
        else:
            # -----------------------------------------------------------------
            # triple leg
            # -----------------------------------------------------------------
            # "ICLinked" all regions
            # -----------------------------------------------------------------
            if(hliDictObj.get("ordertype") == 'ZI1O' and
               hliDictObj.get("distributionchannel", '') == 'IT' and
               hliDictObj.get("soldtopartyid") in FusionBridge and
               hliDictObj.get("supplier") in ICLinked):
                hliDictObj[flowstr] = "ICLinked"
            # -----------------------------------------------------------------
            # BridgeFulfilledHub
            # -----------------------------------------------------------------
            elif(hliDictObj.get("ordertype") == 'ZI1O' and
                 hliDictObj.get("distributionchannel", "") == 'IT' and
                 hliDictObj.get("dccode") != '00' and
                 hliDictObj.get("soldtopartyid") in FusionBridge):
                hliDictObj[flowstr] = "BridgeFulfilledHub"
            # -----------------------------------------------------------------
            # BridgeFulfilledDirect
            # -----------------------------------------------------------------
            elif(hliDictObj.get("ordertype") == 'ZI1O' and
                 hliDictObj.get("distributionchannel") == 'IT' and
                 hliDictObj.get("dccode") == '00' and
                 hliDictObj.get("soldtopartyid") in FusionBridge):
                hliDictObj[flowstr] = "BridgeFulfilledDirect"
            # -----------------------------------------------------------------
            # BridgeICFulfilledHub
            # -----------------------------------------------------------------
            elif(hliDictObj.get("ordertype") == 'ZI1O' and
                 hliDictObj.get("distributionchannel") == 'IT' and
                 hliDictObj.get("soldtopartyid") in ('0590001116')):
                hliDictObj[flowstr] = "BridgeICFulfilledHub"
            # -----------------------------------------------------------------
            # ICFulfilledHub
            # -----------------------------------------------------------------
            elif(hliDictObj.get("ordertype") == 'ZI1O' and
                 hliDictObj.get("distributionchannel") == 'IT' and
                 hliDictObj.get("dccode") != '00'):
                hliDictObj[flowstr] = "ICFulfilledHub"
            # -----------------------------------------------------------------
            # ICFulfilledDirect
            # -----------------------------------------------------------------
            elif(hliDictObj.get("ordertype") == 'ZI1O' and
                 hliDictObj.get("distributionchannel") == 'IT' and
                 hliDictObj.get("dccode") == '00'):
                hliDictObj[flowstr] = "ICFulfilledDirect"
            # -----------------------------------------------------------------
            # ICHub (Order is fulfilled via another Fusion completly)
            # -----------------------------------------------------------------
            elif(hliDictObj.get("dccode") != '00' and
                 hliDictObj.get("supplier") in ICLinked):
                hliDictObj[flowstr] = "ICHub"
            # -----------------------------------------------------------------
            # ICDirect (Order is fulfilled via another Fusion completly)
            # -----------------------------------------------------------------
            elif(hliDictObj.get("dccode") == '00' and
                 hliDictObj.get("supplier") in ICLinked):
                hliDictObj[flowstr] = "ICDirect"
            # -----------------------------------------------------------------
            # BridgeHub (this one is getting fulfilled by S4)
            # -----------------------------------------------------------------
            elif(hliDictObj.get("shippingpoint") in FusionReverse and
                 hliDictObj.get("dccode") != '00'):
                hliDictObj[flowstr] = "BridgeHub"
            # -----------------------------------------------------------------
            # BridgeDirect (this one is getting fulfilled by S4)
            # -----------------------------------------------------------------
            elif(hliDictObj.get("shippingpoint") in FusionReverse and
                 hliDictObj.get("dccode") == '00'):
                hliDictObj[flowstr] = "BridgeDirect"
            # -----------------------------------------------------------------
            # Hub
            # -----------------------------------------------------------------
            elif hliDictObj.get("dccode") != '00':
                hliDictObj[flowstr] = "Hub"
            # -----------------------------------------------------------------
            # Direct
            # -----------------------------------------------------------------
            elif hliDictObj.get("dccode") == '00':
                hliDictObj[flowstr] = "Direct"
            # -----------------------------------------------------------------
    return hliDictObj


def ActiveHeaderHoldStatusRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'InfoStates' not in hliDictObj:
        hliDictObj['InfoStates'] = {}
    # -------------------------------------------------------------------------
    #  Set the InfoState Varaibles
    # -------------------------------------------------------------------------
    pipe = ' | '
    StateType = "headerholdstatus"
    InfoDict = hliDictObj['InfoStates']
    SL = hliDictObj['status_list']
    hliDictObj['InfoStates'][StateType] = []
    # -------------------------------------------------------------------------
    i = 0
    offset = len(hliDictObj['status_list'])
    while i < offset:
        if SL[i].get('objectinactive', '') != 'X' \
                and SL[i].get('statustype', '') == 'header':
            trace = (str('objectchangenum: ') +
                     str(hliDictObj['status_list'][i]['objectchangenum']) +
                     pipe +
                     str('statustype: ') +
                     str(hliDictObj['status_list'][i]['statustype']) +
                     pipe +
                     str('objectstatuscode: ') +
                     str(hliDictObj['status_list'][i]['objectstatuscode']) +
                     pipe +
                     str('Rule: ') +
                     str('ActiveHeaderHoldStatusRule')
                     )
            StateValue = str(hliDictObj['status_list'][i]['objectstatuscode'])
            StartTime = str(hliDictObj['status_list'][i]['objectdt']) + \
                str(hliDictObj['status_list'][i]['objecttime'])
            InfoDict[StateType].append(dict({
                                            'statevalue': StateValue,
                                            'starttime': StartTime,
                                            'endtime': '',
                                            'trace': trace}))
        i += 1
    return hliDictObj


def InactiveHeaderHoldStatusRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Set the InfoState Varaibles
    # -------------------------------------------------------------------------
    pipe = ' | '
    StateType = "headerholdstatus"
    InfoDict = hliDictObj['InfoStates']
    # -------------------------------------------------------------------------
    for SH in hliDictObj['status_list']:
        if SH.get('objectinactive', '') == 'X' and \
                SH.get('statustype', '') == 'header':
            trace = (str('objectchangenum: ') +
                     str(int(SH['objectchangenum']) - 1).zfill(3) +
                     pipe +
                     str('statustype: ') +
                     str(SH['statustype']) +
                     pipe +
                     str('objectstatuscode: ') +
                     str(SH['objectstatuscode']) +
                     pipe +
                     str('Rule: ') +
                     str('ActiveHeaderHoldStatusRule')
                     )
            endTime = str(SH['objectdt']) + \
                str(SH['objecttime'])
    # -------------------------------------------------------------------------
    # Close the previous Open InfoStates
    # -------------------------------------------------------------------------
            for holdHeader in InfoDict[StateType]:
                if holdHeader.get('trace') == trace:
                    holdHeader['endtime'] = endTime
    return hliDictObj


def ActiveItemHoldStatusRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'InfoStates' not in hliDictObj:
        hliDictObj['InfoStates'] = {}
    # -------------------------------------------------------------------------
    #  Set the InfoState Varaibles
    # -------------------------------------------------------------------------
    pipe = ' | '
    StateType = "itemholdstatus"
    hliDictObj['InfoStates'][StateType] = []
    InfoDict = hliDictObj['InfoStates']
    SL = hliDictObj['status_list']
    # -------------------------------------------------------------------------
    i = 0
    offset = len(SL)
    while i < offset:
        if SL[i].get('objectinactive', '') != 'X' and \
                SL[i].get('statustype', '') == 'item':
            trace = (str('objectchangenum: ') +
                     str(SL[i]['objectchangenum']) +
                     pipe +
                     str('statustype: ') +
                     str(SL[i]['statustype']) +
                     pipe +
                     str('objectstatuscode: ') +
                     str(SL[i]['objectstatuscode']) +
                     pipe +
                     str('Rule: ') +
                     str('ActiveItemHoldStatusRule')
                     )
            StateValue = str(SL[i]['objectstatuscode'])
            StartTime = str(SL[i]['objectdt']) + \
                str(SL[i]['objecttime'])
            InfoDict[StateType].append(dict({
                                            'statevalue': StateValue,
                                            'starttime': StartTime,
                                            'endtime': '',
                                            'trace': trace}))
        i += 1
    return hliDictObj


def InactiveItemHoldStatusRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Set the InfoState Varaibles
    # -------------------------------------------------------------------------
    pipe = ' | '
    StateType = "itemholdstatus"
    InfoDict = hliDictObj['InfoStates']
    # -------------------------------------------------------------------------
    for statusItem in hliDictObj['status_list']:
        if statusItem.get('objectinactive', '') == 'X' and \
                statusItem.get('statustype', '') == 'item':
            trace = (str('objectchangenum: ') +
                     str(int(statusItem['objectchangenum']) - 1).zfill(3) +
                     pipe +
                     str('statustype: ') +
                     str(statusItem['statustype']) +
                     pipe +
                     str('objectstatuscode: ') +
                     str(statusItem['objectstatuscode']) +
                     pipe +
                     str('Rule: ') +
                     str('ActiveItemHoldStatusRule')
                     )
            endTime = str(statusItem['objectdt']) + \
                str(statusItem['objecttime'])
    # -------------------------------------------------------------------------
    # Close the previous Open InfoStates
    # -------------------------------------------------------------------------
            for holdHeader in InfoDict[StateType]:
                if holdHeader.get('trace') == trace:
                    holdHeader['endtime'] = endTime
    return hliDictObj


def HliFEHoldsRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'InfoStates' not in hliDictObj:
        hliDictObj['InfoStates'] = {}
    # -------------------------------------------------------------------------
    #  Set the Rule Variables
    # -------------------------------------------------------------------------
    ruleMap = KanariDataHandler.Loader.loadDictionary("ruleMap.json")
    StateType = "feholdhli"
    InfoDict = hliDictObj['InfoStates']
    if StateType not in hliDictObj['InfoStates']:
        hliDictObj['InfoStates'][StateType] = []
    FEHeaderHoldlist = []
    FEHeaderHoldSequencing = {}
    ActiveStatusHeader = []
    FEItemHoldlist = []
    FEItemHoldSeq = {}
    ActiveStatusItem = []
    InfoStates = {}
    InfoStates['HeaderFEHoldsHli'] = []
    ISH = InfoStates['HeaderFEHoldsHli']
    InfoStates['ItemFEHoldsHli'] = []
    ISI = InfoStates['ItemFEHoldsHli']
    # -------------------------------------------------------------------------
    #  S4 Hold Mapping
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        HM = ruleMap['holdmapping']['S4HM']
    # -------------------------------------------------------------------------
    #  EMEA Fusion Hold Mapping
    # -------------------------------------------------------------------------
    elif hliDictObj.get("client") == '010':
        HM = ruleMap['holdmapping']['FusionEmeaHM']
    # -------------------------------------------------------------------------
    # header seq from HoldMapping
    # -------------------------------------------------------------------------
    for holdmapping in HM:
        if holdmapping.get('holdmapping', '') == 'FE' and \
                holdmapping.get('statutype', '') == 'header':
            FEHeaderHoldlist.append(holdmapping.get("objectstatuscode", '')
                                    )
            FEHeaderHoldSequencing.update({
                                          holdmapping.get("objectstatuscode"):
                                          int(holdmapping.get("sequencing", '')
                                              )
                                          })
    # -------------------------------------------------------------------------
    # Active header
    # -------------------------------------------------------------------------
    for statusHeader in hliDictObj['InfoStates']['headerholdstatus']:
        if statusHeader.get('endtime', '') == '' and \
                statusHeader.get('statevalue', '') in FEHeaderHoldlist:
            ActiveStatusHeader.append(statusHeader.get('statevalue', ''))

    if len(ActiveStatusHeader) > 0:
        FilteredSeq = {k: FEHeaderHoldSequencing[k]
                       for k in ActiveStatusHeader
                       if k in FEHeaderHoldSequencing}
        StateValue = min(FilteredSeq, key=lambda k: FEHeaderHoldSequencing[k])
        for statusHeader in hliDictObj['InfoStates']['headerholdstatus']:
            if statusHeader.get('endtime', '') == '' and \
                    statusHeader.get('statevalue', '') == StateValue:
                StartTime = statusHeader.get('starttime', '')
                Trace = 'statustype:header | sequence: ' + \
                    str(FilteredSeq.get(StateValue, ''))
        # ---------------------------------------------------------------------
        # Append InfoState
        # ---------------------------------------------------------------------
        ISH.append(dict({
                        'sequence': FilteredSeq.get(StateValue, ''),
                        'statevalue': StateValue,
                        'starttime': StartTime,
                        'endtime': '',
                        'trace': Trace
                        }))
    # -------------------------------------------------------------------------
    #  S4 Hold Mapping
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        HM = ruleMap['holdmapping']['S4HM']
        FE = ["FE"]
    # -------------------------------------------------------------------------
    #  EMEA Fusion Hold Mapping
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '010':
        HM = ruleMap['holdmapping']['FusionEmeaHM']
        FE = ["CS-FE", "FE"]
    # -------------------------------------------------------------------------
    # Item seq from HoldMapping
    # -------------------------------------------------------------------------
    for holdmapping in HM:
        if holdmapping.get('holdmapping', '') in FE and \
                holdmapping.get('statutype', '') == 'item':
            FEItemHoldlist.append(holdmapping.get("objectstatuscode", ''))
            FEItemHoldSeq.update({holdmapping.get("objectstatuscode", ''):
                                  int(float(holdmapping.get("sequencing")))})
    # -------------------------------------------------------------------------
    # Active Items
    # -------------------------------------------------------------------------
    for statusItem in hliDictObj['InfoStates']['itemholdstatus']:
        if statusItem.get('endtime', '') == '' and \
                statusItem.get('statevalue', '') in FEItemHoldlist:
            ActiveStatusItem.append(statusItem.get('statevalue', ''))

    if len(ActiveStatusItem) > 0:
        FilteredSeq = {k: FEItemHoldSeq[k] for k in ActiveStatusItem
                       if k in FEItemHoldSeq}
        StateValue = min(FilteredSeq, key=lambda k: FEItemHoldSeq[k])
        for statusItem in hliDictObj['InfoStates']['itemholdstatus']:
            if statusItem.get('endtime', '') == '' and \
                    statusItem.get('statevalue', '') == StateValue:
                StartTime = statusItem.get('starttime', '')
                Trace = 'statustype:item | sequence: ' + \
                    str(FilteredSeq.get(StateValue, ''))
        # ---------------------------------------------------------------------
        # Append InfoState
        # ---------------------------------------------------------------------
        ISI.append(dict({
                        'sequence': FilteredSeq.get(StateValue, ''),
                        'statevalue': StateValue,
                        'starttime': StartTime,
                        'endtime': '',
                        'trace': Trace
                        }))
    # -------------------------------------------------------------------------
    #  Set the Rule Variables
    # -------------------------------------------------------------------------
    FEHolds = {}
    result = ''
    # -------------------------------------------------------------------------
    #  Min seq from header & Item
    # -------------------------------------------------------------------------
    if len(InfoStates['ItemFEHoldsHli']) > 0:
        FEHolds["1"] = InfoStates['ItemFEHoldsHli'][0]
        if len(InfoStates['HeaderFEHoldsHli']) > 0:
            FEHolds["2"] = InfoStates['HeaderFEHoldsHli'][0]
        result = min(FEHolds.values(), key=lambda x: x['sequence'])
    elif len(InfoStates['HeaderFEHoldsHli']) > 0:
        FEHolds["1"] = InfoStates['HeaderFEHoldsHli'][0]
        result = min(FEHolds.values(), key=lambda x: x['sequence'])
    # check if status returned a value
    if result != '':
        # ---------------------------------------------------------------------
        # Close the previous Open FEHliHolds
        # ---------------------------------------------------------------------
        if len(hliDictObj['InfoStates'][StateType]) > 0:
            for holds in hliDictObj['InfoStates'][StateType]:
                if holds.get('endtime') == '':
                    if result.get('trace', '') != holds.get('trace', ''):
                        holds['endtime'] = result.get('starttime', '')
        InfoDict[StateType].append(dict({
                                        'statevalue': result.get('statevalue'),
                                        'starttime': result.get('starttime'),
                                        'endtime': '',
                                        'trace': result.get('trace', '')}))
    return hliDictObj


def HliSCHoldsRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'InfoStates' not in hliDictObj:
        hliDictObj['InfoStates'] = {}
    # -------------------------------------------------------------------------
    #  Set the Rule Variables
    # -------------------------------------------------------------------------
    ruleMap = KanariDataHandler.Loader.loadDictionary("ruleMap.json")
    StateType = "scholdhli"
    InfoDict = hliDictObj['InfoStates']
    if StateType not in hliDictObj['InfoStates']:
        hliDictObj['InfoStates'][StateType] = []
    SCHeaderHoldlist = []
    SCHeaderHoldSequencing = {}
    ActiveStatusHeader = []
    SCItemHoldlist = []
    SCItemHoldSeq = {}
    ActiveStatusItem = []
    InfoStates = {}
    InfoStates['HeaderSCHoldsHli'] = []
    ISH = InfoStates['HeaderSCHoldsHli']
    InfoStates['ItemSCHoldsHli'] = []
    ISI = InfoStates['ItemSCHoldsHli']
    # -------------------------------------------------------------------------
    #  S4 Hold Mapping
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        HM = ruleMap['holdmapping']['S4HM']
    # -------------------------------------------------------------------------
    #  EMEA Fusion Hold Mapping
    # -------------------------------------------------------------------------
    elif hliDictObj.get("client") == '010':
        HM = ruleMap['holdmapping']['FusionEmeaHM']
    # -------------------------------------------------------------------------
    # header seq from HoldMapping
    # -------------------------------------------------------------------------
    for holdmapping in HM:
        if holdmapping.get('holdmapping', '') == 'SC' and \
                holdmapping.get('statutype', '') == 'header':
            SCHeaderHoldlist.append(holdmapping.get("objectstatuscode", '')
                                    )
            SCHeaderHoldSequencing.update({
                                          holdmapping.get("objectstatuscode"):
                                          int(holdmapping.get("sequencing", '')
                                              )
                                          })
    # -------------------------------------------------------------------------
    # Active header
    # -------------------------------------------------------------------------
    for statusHeader in hliDictObj['InfoStates']['headerholdstatus']:
        if statusHeader.get('endtime', '') == '' and \
                statusHeader.get('statevalue', '') in SCHeaderHoldlist:
            ActiveStatusHeader.append(statusHeader.get('statevalue', ''))

    if len(ActiveStatusHeader) > 0:
        FilteredSeq = {k: SCHeaderHoldSequencing[k]
                       for k in ActiveStatusHeader
                       if k in SCHeaderHoldSequencing}
        StateValue = min(FilteredSeq, key=lambda k: SCHeaderHoldSequencing[k])
        for statusHeader in hliDictObj['InfoStates']['headerholdstatus']:
            if statusHeader.get('endtime', '') == '' and \
                    statusHeader.get('statevalue', '') == StateValue:
                StartTime = statusHeader.get('starttime', '')
                Trace = 'statustype:header | sequence: ' + \
                    str(FilteredSeq.get(StateValue, ''))
        # ---------------------------------------------------------------------
        # Append InfoState
        # ---------------------------------------------------------------------
        ISH.append(dict({
                        'sequence': FilteredSeq.get(StateValue, ''),
                        'statevalue': StateValue,
                        'starttime': StartTime,
                        'endtime': '',
                        'trace': Trace
                        }))
    # -------------------------------------------------------------------------
    #  S4 Hold Mapping
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        HM = ruleMap['holdmapping']['S4HM']
        SC = ["SC"]
    # -------------------------------------------------------------------------
    #  EMEA Fusion Hold Mapping
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '010':
        HM = ruleMap['holdmapping']['FusionEmeaHM']
        SC = ["SC"]
    # -------------------------------------------------------------------------
    # Item seq from HoldMapping
    # -------------------------------------------------------------------------
    for holdmapping in HM:
        if holdmapping.get('holdmapping', '') in SC and \
                holdmapping.get('statutype', '') == 'item':
            SCItemHoldlist.append(holdmapping.get("objectstatuscode", ''))
            SCItemHoldSeq.update({holdmapping.get("objectstatuscode", ''):
                                  int(float(holdmapping.get("sequencing")))})
    # -------------------------------------------------------------------------
    # Active Items
    # -------------------------------------------------------------------------
    for statusItem in hliDictObj['InfoStates']['itemholdstatus']:
        if statusItem.get('endtime', '') == '' and \
                statusItem.get('statevalue', '') in SCItemHoldlist:
            ActiveStatusItem.append(statusItem.get('statevalue', ''))

    if len(ActiveStatusItem) > 0:
        FilteredSeq = {k: SCItemHoldSeq[k] for k in ActiveStatusItem
                       if k in SCItemHoldSeq}
        StateValue = min(FilteredSeq, key=lambda k: SCItemHoldSeq[k])
        for statusItem in hliDictObj['InfoStates']['itemholdstatus']:
            if statusItem.get('endtime', '') == '' and \
                    statusItem.get('statevalue', '') == StateValue:
                StartTime = statusItem.get('starttime', '')
                Trace = 'statustype:item | sequence: ' + \
                    str(FilteredSeq.get(StateValue, ''))
        # ---------------------------------------------------------------------
        # Append InfoState
        # ---------------------------------------------------------------------
        ISI.append(dict({
                        'sequence': FilteredSeq.get(StateValue, ''),
                        'statevalue': StateValue,
                        'starttime': StartTime,
                        'endtime': '',
                        'trace': Trace
                        }))
    # -------------------------------------------------------------------------
    #  Set the Rule Variables
    # -------------------------------------------------------------------------
    SCHolds = {}
    result = ''
    # -------------------------------------------------------------------------
    #  Min seq from header & Item
    # -------------------------------------------------------------------------
    if len(InfoStates['ItemSCHoldsHli']) > 0:
        SCHolds["1"] = InfoStates['ItemSCHoldsHli'][0]
        if len(InfoStates['HeaderSCHoldsHli']) > 0:
            SCHolds["2"] = InfoStates['HeaderSCHoldsHli'][0]
        result = min(SCHolds.values(), key=lambda x: x['sequence'])
    elif len(InfoStates['HeaderSCHoldsHli']) > 0:
        SCHolds["1"] = InfoStates['HeaderSCHoldsHli'][0]
        result = min(SCHolds.values(), key=lambda x: x['sequence'])
    # check if status returned a value
    if result != '':
        # ---------------------------------------------------------------------
        # Close the previous Open FEHliHolds
        # ---------------------------------------------------------------------
        if len(hliDictObj['InfoStates'][StateType]) > 0:
            for holds in hliDictObj['InfoStates'][StateType]:
                if holds.get('endtime') == '':
                    if result.get('trace', '') != holds.get('trace', ''):
                        holds['endtime'] = result.get('starttime', '')
        InfoDict[StateType].append(dict({
                                        'statevalue': result.get('statevalue'),
                                        'starttime': result.get('starttime'),
                                        'endtime': '',
                                        'trace': result.get('trace', '')}))
    return hliDictObj


def BloodBuildRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'InfoStates' not in hliDictObj:
        hliDictObj['InfoStates'] = {}
    # -------------------------------------------------------------------------
    #  Set the InfoState Varaibles
    # -------------------------------------------------------------------------
    BBList = []
    StateType = 'bloodbuild'
    StateValue = ''
    InfoDict = hliDictObj['InfoStates']
    pipe = ' | '
    for BB in InfoDict['headerholdstatus']:
        # ---------------------------------------------------------------------
        # S4 Find BBLK status in HeaderHoldStatus
        # ---------------------------------------------------------------------
        if hliDictObj.get("client") == '300':
            if BB.get('statevalue', '') == 'BBLK':
                StateValue = BB['statevalue']
                StartTime = BB['starttime']
                EndTime = BB['endtime']
                Trace = (str('statustype: header') +
                         pipe +
                         str('objectstatuscode: ') +
                         BB['statevalue'] +
                         pipe +
                         str('Rule: ') +
                         str('BloodBuildRule')
                         )
                # -------------------------------------------------------------
                # Append InfoState
                # -------------------------------------------------------------
                if StateValue != '':
                    BBList.append(dict({
                                       'statevalue': StateValue,
                                       'starttime': StartTime,
                                       'endtime': EndTime,
                                       'trace': Trace
                                       }))
        # ---------------------------------------------------------------------
        # Fusion Find 1SLT and 2SLT status in HeaderHoldStatus
        # ---------------------------------------------------------------------
        elif hliDictObj.get("client") in ('010', '007', '016'):
            if BB.get('statevalue', '') in ('1SLT', '2SLT'):
                StateValue = BB['statevalue']
                StartTime = BB['starttime']
                EndTime = BB['endtime']
                Trace = (str('statustype: header') +
                         pipe +
                         str('objectstatuscode: ') +
                         BB['statevalue'] +
                         pipe +
                         str('Rule: ') +
                         str('BloodBuildRule')
                         )
                # -------------------------------------------------------------
                # Append InfoState
                # -------------------------------------------------------------
                if StateValue != '':
                    BBList.append(dict({
                                       'statevalue': StateValue,
                                       'starttime': StartTime,
                                       'endtime': EndTime,
                                       'trace': Trace
                                       }))
    InfoDict[StateType] = BBList
    return hliDictObj


def EmrRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'InfoStates' not in hliDictObj:
        hliDictObj['InfoStates'] = {}
    # -------------------------------------------------------------------------
    #  Set the InfoState Varaibles
    # -------------------------------------------------------------------------
    emrList = []
    StateType = 'emr'
    StateValue = ''
    InfoDict = hliDictObj['InfoStates']
    pipe = ' | '
    for emr in InfoDict['headerholdstatus']:
        # ---------------------------------------------------------------------
        # S4 Find ERMK status in HeaderHoldStatus
        # ---------------------------------------------------------------------
        if hliDictObj.get("client") == '300':
            if emr.get('statevalue', '') == 'ERMK':
                StateValue = emr['statevalue']
                StartTime = emr['starttime']
                EndTime = emr['endtime']
                Trace = (str('statustype: header') +
                         pipe +
                         str('objectstatuscode: ') +
                         emr['statevalue'] +
                         pipe +
                         str('Rule: ') +
                         str('EmrRule')
                         )
                # -------------------------------------------------------------
                # Append InfoState
                # -------------------------------------------------------------
                if StateValue != '':
                    emrList.append(dict({
                                        'statevalue': StateValue,
                                        'starttime': StartTime,
                                        'endtime': EndTime,
                                        'trace': Trace
                                        }))
    InfoDict[StateType] = emrList
    return hliDictObj


def MilestonesCustomerPOCreatedRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Key, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "customerpocreated"
    StartTime = str(hliDictObj.get("customerpocreateddt", '')) + str('000000')
    seq = 1
    MilestoneDict = hliDictObj['Milestones']
    # -------------------------------------------------------------------------
    MilestoneDict.update(dict({
                              milestone: dict({'starttime': StartTime,
                                               'endtime': '',
                                               'seq': seq})}))
    return hliDictObj


def MilestonesOrderReceivedRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Dict, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "orderreceived"
    StartTime = str(hliDictObj.get("receiveddt", ''))
    seq = 2
    MilestoneDict = hliDictObj['Milestones']
    # -------------------------------------------------------------------------
    # Close the previous Open milestone
    # -------------------------------------------------------------------------
    for k in MilestoneDict:
        if MilestoneDict[k].get('endtime') == '':
            MilestoneDict[k]['endtime'] = StartTime
    # -------------------------------------------------------------------------
    MilestoneDict.update(dict({
                              milestone: dict({'starttime': StartTime,
                                               'endtime': '',
                                               'seq': seq})}))
    return hliDictObj


def MilestonesOrderCreatedRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Dict, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "ordercreated"
    StartTime = str(hliDictObj.get("ordercreateddt", '')) + \
        str(hliDictObj.get("ordercreatetime", ''))
    seq = 3
    MilestoneDict = hliDictObj['Milestones']
    # -------------------------------------------------------------------------
    # Close the previous Open milestone
    # -------------------------------------------------------------------------
    for k in MilestoneDict:
        if MilestoneDict[k].get('endtime') == '':
            MilestoneDict[k]['endtime'] = StartTime
    # -------------------------------------------------------------------------
    MilestoneDict.update(dict({
                              milestone: dict({'starttime': StartTime,
                                               'endtime': '',
                                               'seq': seq})}))
    return hliDictObj


def MilestonesItemCreatedRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Dict, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "itemcreated"
    StartTime = str(hliDictObj.get("itemcreateddt", '')) + \
        str(hliDictObj.get("itemcreatetime", ''))
    seq = 4
    MilestoneDict = hliDictObj['Milestones']
    # -------------------------------------------------------------------------
    # Close the previous Open milestone
    # -------------------------------------------------------------------------
    for k in MilestoneDict:
        if MilestoneDict[k].get('endtime') == '':
            MilestoneDict[k]['endtime'] = StartTime
    # -------------------------------------------------------------------------
    MilestoneDict.update(dict({
                              milestone: dict({'starttime': StartTime,
                                               'endtime': '',
                                               'seq': seq})}))
    return hliDictObj


def MilestonesItemCleanedRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Dict, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    ruleMap = KanariDataHandler.Loader.loadDictionary("ruleMap.json")
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "itemcleaned"
    StartTime = ""
    seq = 5
    MilestoneDict = hliDictObj['Milestones']
    cleanDateList = []
    # -------------------------------------------------------------------------
    #  S4 rule
    # -------------------------------------------------------------------------
    # Find RTF status in ItemHoldStatus
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        for HoldStatus in hliDictObj['InfoStates']['itemholdstatus']:
            if HoldStatus.get('statevalue', '') == 'RTF':
                if StartTime == '':
                    StartTime = str(HoldStatus.get("starttime", ''))
                elif str(HoldStatus.get("starttime", '')) < StartTime:
                    StartTime = str(HoldStatus.get("starttime", ''))
    # -------------------------------------------------------------------------
    # Fusion EMEA rule
    # -------------------------------------------------------------------------
    elif hliDictObj.get("client") == '010':
        CR = ruleMap['fusioncleanrelevant']['emea']
        for HoldStatus in hliDictObj['InfoStates']['itemholdstatus']:
            if HoldStatus.get('statevalue', '') in CR:
                EndTime = str(HoldStatus.get("endtime", ''))
                cleanDateList.append(EndTime)
        # No "Clean" relevant holds in Item Status History
        if len(cleanDateList) == 0:
            for open in hliDictObj['status_list']:
                if(open.get('objectchangenum', '') == '001' and
                   open.get('objectstatuscode', '') == 'OPN' and
                   open.get('statustype', '') == 'item'):
                        StartTime = str(open.get('objectdt')) + \
                            str(open.get('objecttime'))
        # Max "EndTime" from "Clean" relevant holds in Item Status History
        elif '' not in cleanDateList:
            StartTime = max(cleanDateList)
    # check if status returned a value
    if StartTime != '':
        # ---------------------------------------------------------------------
        # Close the previous Open milestone
        # ---------------------------------------------------------------------
        for k in MilestoneDict:
            if MilestoneDict[k].get('endtime') == '':
                MilestoneDict[k]['endtime'] = StartTime
        # ---------------------------------------------------------------------
        MilestoneDict.update(dict({
                                  milestone: dict({'starttime': StartTime,
                                                   'endtime': '',
                                                   'seq': seq})}))
    return hliDictObj


def MilestonesReleasedToProductionRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Dict, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "releasedtoproduction"
    StartTime = ""
    seq = 6
    MilestoneDict = hliDictObj['Milestones']
    RTP = ['PROS', 'BKPL', 'XCOB']
    # -------------------------------------------------------------------------
    # Find RTP status in ItemHoldStatus
    # -------------------------------------------------------------------------
    for HoldStatus in hliDictObj['InfoStates']['itemholdstatus']:
        if HoldStatus.get('statevalue', '') in RTP:
            StartTime = str(HoldStatus.get("starttime", ''))
    # check if status returned a value
    if StartTime != '':
        # ---------------------------------------------------------------------
        # Close the previous Open milestone
        # ---------------------------------------------------------------------
        for k in MilestoneDict:
            if MilestoneDict[k].get('endtime') == '':
                MilestoneDict[k]['endtime'] = StartTime
        # ---------------------------------------------------------------------
        MilestoneDict.update(dict({
                                  milestone: dict({'starttime': StartTime,
                                                   'endtime': '',
                                                   'seq': seq})}))
    return hliDictObj


def MilestonesProductionDoneRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Create Milestone Dict, if not exist
    # -------------------------------------------------------------------------
    if 'Milestones' not in hliDictObj:
        hliDictObj['Milestones'] = {}
    # -------------------------------------------------------------------------
    #  Set the Milestone Varaibles
    # -------------------------------------------------------------------------
    milestone = "productiondone"
    StartTime = ""
    seq = 7
    MilestoneDict = hliDictObj['Milestones']
    PD = ['PROC', 'FGI', 'XFGI']
    # -------------------------------------------------------------------------
    # Find PD status in ItemHoldStatus
    # -------------------------------------------------------------------------
    for HoldStatus in hliDictObj['InfoStates']['itemholdstatus']:
        if HoldStatus.get('statevalue', '') in PD:
            StartTime = str(HoldStatus.get("starttime", ''))
    # check if status returned a value
    if StartTime != '':
        # ---------------------------------------------------------------------
        # Close the previous Open milestone
        # ---------------------------------------------------------------------
        for k in MilestoneDict:
            if MilestoneDict[k].get('endtime') == '':
                MilestoneDict[k]['endtime'] = StartTime
        # ---------------------------------------------------------------------
        MilestoneDict.update(dict({
                                  milestone: dict({'starttime': StartTime,
                                                   'endtime': '',
                                                   'seq': seq})}))
    return hliDictObj


def SystemESDHliScheduleDateRule(hliDictObj, stateObj):
    # -------------------------------------------------------------------------
    #  Workday Function
    # -------------------------------------------------------------------------
    def workday(datevalue, offset):
        # use regex to to seperate datevalue into "Year, Month, and Day"
        datelist = re.findall(r"[\w']+", datevalue)
        if len(datelist) > 1:
            y = int(datelist[0])
            m = int(datelist[1])
            d = int(datelist[2])
        elif len(datelist) == 1:
            y = int(datevalue[0:4])
            m = int(datevalue[4:6])
            d = int(datevalue[6:8])
        # pass the datelist values to the "date" fuction
        startdate = date(y, m, d)
        i = 0
        workdays = startdate
        # iterate through offset to return working days
        while i < offset:
            # weekday
            # print(newdate.isoweekday())
            if workdays.isoweekday() < 5 or workdays.isoweekday() == 7:
                workdays += timedelta(days=1)
                # weekend
            else:
                # print("weekends don't count for offset, so add 1")
                workdays = workdays + timedelta(days=1)
                # weekends don't count for offset, so add 1
                offset += 1
            i += 1
        return workdays.strftime("%Y%m%d")
    # -------------------------------------------------------------------------
    #  Create InfoStates Dict, if not exist
    # -------------------------------------------------------------------------
    if 'ScheduleDates' not in hliDictObj:
        hliDictObj['ScheduleDates'] = {}
    # -------------------------------------------------------------------------
    #  Set the ESD Varaibles
    # -------------------------------------------------------------------------
    HubFlowList = ["BridgeFulfilledHub",
                   "BridgeICFulfilledHub",
                   "BridgeICHub",
                   "ICFulfilledHub",
                   "BridgeHub",
                   "ICHub",
                   "Hub"]
    CommitList = []
    confirmmedList = []
    UTCNow = stateObj.getUTCDatetime().strftime("%Y-%m-%d")
    DateType = 'systemesd'
    DateValue = ''
    InfoDict = hliDictObj['ScheduleDates']
    # -------------------------------------------------------------------------
    #  Set the ATPFlag Varaibles
    # -------------------------------------------------------------------------
    ATPCHECKFLAG_X = ''
    # -------------------------------------------------------------------------
    #  get the atpcheckflag serial' in "atpcheck_list" where 'X'
    # -------------------------------------------------------------------------
    if hliDictObj.get("client") == '300':
        for atp in hliDictObj.get("atpcheck_list", ''):
            if atp.get('atpcheckflag', '') == 'X':
                ATPCHECKFLAG_X = atp.get('serial', '')
    # -------------------------------------------------------------------------
    # Schedule list
    # -------------------------------------------------------------------------
    # last serial
    MaxSchedSerial = max(hliDictObj.get("schedule_list", ''),
                         key=lambda x: x['serial'])
    # -------------------------------------------------------------------------
    for confirmedLine in hliDictObj["schedule_list"]:
        confirmmedList.append(int
                              (float(confirmedLine.get
                                     ("schedulelineconfirmedqty",
                                      ''))) > 0)
        # ---------------------------------------------------------------------
        # S4 Rule Loop
        # ---------------------------------------------------------------------
        if hliDictObj.get("client") == '300':
            # -----------------------------------------------------------------
            # Confirmmed Qty > 0
            # -----------------------------------------------------------------
            if int(float
                   (confirmedLine.get("schedulelineconfirmedqty", ''))) > 0:
                # schedule line was confirmmed before the ATPCheck X "serial"
                if confirmedLine.get("serial", '') <= ATPCHECKFLAG_X:
                    # HubIndicator
                    if hliDictObj.get("FlowType", '') in(HubFlowList):
                        # current schedule line, so we use "Now"
                        if(confirmedLine.get("serial", '') ==
                           MaxSchedSerial.get('serial', '')):
                                DateValue = str(workday(UTCNow, 9))
                            # past schedule line, so we use serial
                        else:
                            DateValue = str(workday
                                            (confirmedLine.get("serial",
                                                               ''), 9))
                # schedule line was confirmmed after ATPCheck X "serial"
                else:
                    # HubIndicator
                    if hliDictObj.get("FlowType", '') in(HubFlowList):
                        DateValue = str(workday
                                        (confirmedLine.get
                                         ("plannedfactoryshipdate", ''), 2))
                    else:
                        DateValue = str(confirmedLine.get
                                        ("plannedfactoryshipdate", ''))
            else:
                # -------------------------------------------------------------
                # Confirmmed Qty = 0
                # -------------------------------------------------------------
                # current schedule line, so we use "Now
                if(confirmedLine.get("serial", '') !=
                   MaxSchedSerial.get('serial', '')):
                    # HubIndicator
                    if hliDictObj.get("FlowType", '') in(HubFlowList):
                        DateValue = str(workday
                                        (confirmedLine.get("serial", ''), 9))
                    else:
                        DateValue = str(workday
                                        (confirmedLine.get("serial", ''), 7))
                # past schedule line, so we use serial
                else:
                    # HubIndicator
                    if hliDictObj.get("FlowType", '') in(HubFlowList):
                        DateValue = str(workday(UTCNow, 9))
                    else:
                        DateValue = str(workday(UTCNow, 7))
            StartTime = confirmedLine.get("serial", '')
            EndTime = ""
            Trace = 'schedulelinenum: ' + \
                confirmedLine.get("schedulelinenum", '') + \
                ' | schedulelineconfirmedqty: ' + \
                str(confirmedLine.get("schedulelineconfirmedqty",
                                      '')).strip() + \
                ' | change: ' + \
                confirmedLine.get("serial", '')
            # -----------------------------------------------------------------
            # Close the previous Open ScheduleDate
            # -----------------------------------------------------------------
            for prevScheduleDate in CommitList:
                if(len(CommitList) >= 1 and
                   prevScheduleDate.get('endtime') == ''):
                    prevScheduleDate['endtime'] = StartTime
            # -----------------------------------------------------------------
            # Append schedule dates
            # -----------------------------------------------------------------
            if DateValue != '':
                CommitList.append(dict({
                                       'datevalue': DateValue,
                                       'starttime': StartTime,
                                       'endtime': EndTime,
                                       'trace': Trace
                                       }))
        # ---------------------------------------------------------------------
        # Fusion schedule Rule
        # ---------------------------------------------------------------------
        elif hliDictObj.get("client") in ('010', '007', '016'):
            # -----------------------------------------------------------------
            # Confirmmed Qty > 0
            # -----------------------------------------------------------------
            if int(float
                   (confirmedLine.get("schedulelineconfirmedqty", ''))) > 0:
                # HubIndicator
                if hliDictObj.get("FlowType", '') in(HubFlowList):
                    DateValue = str(workday
                                    (confirmedLine.get
                                     ("plannedfactoryshipdate", ''), 2))
                else:
                    DateValue = str(confirmedLine.get
                                    ("plannedfactoryshipdate", ''))
            else:
                # -------------------------------------------------------------
                # Confirmmed Qty = 0
                # -------------------------------------------------------------
                # current schedule line, so we use "Now
                if(confirmedLine.get("serial", '') !=
                   MaxSchedSerial.get('serial', '')):
                    # HubIndicator
                    if hliDictObj.get("FlowType", '') in(HubFlowList):
                        DateValue = str(workday
                                        (confirmedLine.get("serial", ''), 9))
                    else:
                        DateValue = str(workday
                                        (confirmedLine.get("serial", ''), 7))
                # past schedule line, so we use serial
                else:
                    # HubIndicator
                    if hliDictObj.get("FlowType", '') in(HubFlowList):
                        DateValue = str(workday(UTCNow, 9))
                    else:
                        DateValue = str(workday(UTCNow, 7))
            StartTime = confirmedLine.get("serial", '')
            EndTime = ""
            Trace = 'schedulelinenum: ' + \
                confirmedLine.get("schedulelinenum", '') + \
                ' | schedulelineconfirmedqty: ' + \
                str(confirmedLine.get("schedulelineconfirmedqty",
                                      '')).strip() + \
                ' | change: ' + \
                confirmedLine.get("serial", '')
            # -----------------------------------------------------------------
            # Close the previous Open ScheduleDate
            # -----------------------------------------------------------------
            for prevScheduleDate in CommitList:
                if(len(CommitList) >= 1 and
                   prevScheduleDate.get('endtime') == ''):
                    prevScheduleDate['endtime'] = StartTime
            # -----------------------------------------------------------------
            # Append schedule dates
            # -----------------------------------------------------------------
            if DateValue != '':
                CommitList.append(dict({
                                       'datevalue': DateValue,
                                       'starttime': StartTime,
                                       'endtime': EndTime,
                                       'trace': Trace
                                       }))
    InfoDict[DateType] = CommitList
    return hliDictObj

from KanariDigital import KanariDataHandler, TestRunner, KanariRule
KanariDataHandler.init(spark, "OrderObject")


KanariDataHandler.registerDefaultRules("OrderObject", [
    KanariRule("Flowtype Rule", FlowTypeRule),
    KanariRule("SourceRegion Rule", SourceRegionRule),
    KanariRule("Active Header Hold Status Rules", ActiveHeaderHoldStatusRule),
    KanariRule("Inactive Header Hold Status Rule", InactiveHeaderHoldStatusRule),
    KanariRule("Active Item Hold Status Rules", ActiveItemHoldStatusRule),
    KanariRule("Inactive Item Hold Status Rule", InactiveItemHoldStatusRule),
    KanariRule("Milestones CustomerPOCreated Rule", MilestonesCustomerPOCreatedRule),
    KanariRule("Milestones OrderReceived Rule", MilestonesOrderReceivedRule),
    KanariRule("Milestones OrderCreated Rule", MilestonesOrderCreatedRule),
    KanariRule("Milestones ItemCreated Rule", MilestonesItemCreatedRule),
    KanariRule("Milestones ItemCleaned Rule", MilestonesItemCleanedRule),
    KanariRule("Milestones ReleasedToProduction Rule", MilestonesReleasedToProductionRule),
    KanariRule("Milestones ProductionDone Rule", MilestonesProductionDoneRule),
    KanariRule("FEHoldHli Rule", HliFEHoldsRule),
    KanariRule("SCHoldHli Rule", HliSCHoldsRule),
    KanariRule("BloodBuildRule", BloodBuildRule),
    KanariRule("EmrRule", EmrRule),
    KanariRule("SystemESDHli ScheduleDate Rule", SystemESDHliScheduleDateRule)
])

df = TestRunner.runDefaultRules("OrderObject")


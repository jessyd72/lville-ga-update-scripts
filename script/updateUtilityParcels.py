'''
 ____________________________________________________________________
 Lawrenceville, GA
_____________________________________________________________________

   Program:    updateUtilityParcels.py
   Purpose:    Updates the UtilityParcels feature class and update map 
               service. Runs M, T, W, F @ 11 (task scheduler)
_____________________________________________________________________
   History:     GTG     11/2019     Created

                ATV     04/02/2020  line 331 - Moved this down after the
                                    below input variables
                                    # environments
                                    arcpy.env.overwriteOutput = True
                                    arcpy.env.workspace = gdb
                JB      04/08/2020  lines 207-234 - Changed method to
                                    remove dictionaries, complete join and
                                    update cursor instead
                JB      04/13/2020  Using a version to perform edits on
                                    version = updateParcels
                                    will be deleted after each reconcile/post        
                JB      04/28/2020  lines 206-209 changed spatial join method
                                    from INTERSECT to HAVE_THEIR_CENTER_IN      
                JB      05/01/2020  updated publish_utilityparcels functions 
                                    to perform update cursor on FGDB. Moved 
                                    append after sani and limb pickup update
                                    Removed need for editing session- functions
                                    removed and sde connection file removed 
                                    as arguments for publish_utilityparcels 
                                    function
                JB      05/06/2020  Adding a clip process to the create_parcelsall_fc
                                    function to limit the service area
                JB      11/25/2020  Rewrote script to not recreate ParcelsAll 
                                    and only update UtilityParcels
                JB      11/30/2020  Changed "MyGovernmentServices.MapServer" to 
                                    "MyCityServices.MapServer" as Ethan Credle removed 
                                    the previous service.
                JB      02/04/2021  Updated BD instance after migration. Removed
                                    the delete version param from reconcile/post,
                                    adding new line to remove version.
                JB      02/11/2021  Deleting updatParcel version using SDE, not GISADMIN.
                JB      11/23/2021  Adding recycle routes populateServiceFields function.
_____________________________________________________________________
'''

import arcpy
from arcpy import env
import os
import json, urllib, urllib2
import contextlib
from datetime import datetime
import logging

def prepUtilityParcels(gdb, parcelsall, servicearea):

    # create utilityparcels fc in fgdb for working 
    logging.info('Creating copy of ParcelsAll to fgdb for working...')
    utilityparcels_f = arcpy.Clip_analysis(parcelsall, servicearea, gdb + r'\UtilityParcels_f')

    # adding service fields
    fields = {'Electric':10, 'Garbage':10, 'Gas':10, 'Security_Lights':10, 'Sewer':10, 'Stormwater':10, 'Water':10, 
                'Account':50, 'Customer_Classification':50}
    logging.info('Adding service fields...')
    for k, v in sorted(fields.items()):
        arcpy.AddField_management(utilityparcels_f, k, 'TEXT', field_length=v)

    arcpy.AddField_management(utilityparcels_f, 'Limb_Pickup_Day', 'TEXT', field_length=50)
    arcpy.AddField_management(utilityparcels_f, 'Sanitation_Pickup_Day', 'TEXT', field_length=50)
    arcpy.AddField_management(utilityparcels_f, 'Recycle_Pickup_Day', 'TEXT',  field_length=50)
    arcpy.AddField_management(utilityparcels_f, 'Recycle_Pickup_Week', 'TEXT', field_length=10)

    return(utilityparcels_f)

def populateServiceFields(utilityparcels, serviceinfo, limb, sanitation, recycle):

    # creating feature layers for selection
    logging.info('Making feature layer of utility parcels for selection...')
    utilityparcels_lyr = arcpy.MakeFeatureLayer_management(utilityparcels, 'utilityparcels_lyr')

    # dictionary with vaues in ServiceInfo as keys, and field names in utility parcels as values
    svc_dict = {'GAS':'Gas', 'ELECTRIC':'Electric', 'GARBAGE':'Garbage', 'STORMWATER FEE':'Stormwater', 'SHD SWR':'Stormwater', 
                'SEWER':'Sewer', 'WATER':'Water', 'SHD WTR':'Water', 'SECURITY LIGHTS':'Security_Lights'}

    logging.info("Running selections to find service by parcel...")
    # loop through items to populate service fields 
    for k, v in svc_dict.items():
        logging.info("Checking service {}".format(k))
        # select service type from ServiceInfo
        select_lyr = arcpy.MakeFeatureLayer_management(serviceinfo, "serviceinfo_lyr", "SvcName = '{}'".format(k))
        # select parcel that intersects selected service point
        select_loc = arcpy.SelectLayerByLocation_management(utilityparcels_lyr, 'INTERSECT', select_lyr, '', 'NEW_SELECTION')
        logging.info(str(int(arcpy.GetCount_management(select_loc).getOutput(0))) + " records with {} service".format(v))
        # update corresponding field as 'Available' in utility parcels
        with arcpy.da.UpdateCursor(select_loc, [v]) as ucur:
            for urow in ucur:
                urow[0] = "Available"
                ucur.updateRow(urow)

    # spatial join between parcels and services to get account number and customer classification
    logging.info("Spatial join between utility parcels and ServiceInfo...")
    parcel_service_sj = arcpy.SpatialJoin_analysis(utilityparcels, serviceinfo, "par_serv_sj", "JOIN_ONE_TO_MANY")
    logging.info('Adding AccountNum_final field...')
    arcpy.AddField_management(parcel_service_sj, "AccountNum_final", "TEXT")

    # use cursor to calculate account number for gas accounts
    logging.info("Finding accounts with Gas service...")
    gas_list = {}
    with arcpy.da.SearchCursor(parcel_service_sj, ["AcctNum", "TARGET_FID"], "SvcName = 'GAS'") as scur: 
        for row in scur:
            if row[1] not in gas_list:
                gas_list[str(row[1])] = str(row[0])
    logging.info('number of gas records: ' + str(len(gas_list)))

    # update account number row to prioritize Gas accounts
    logging.info("Starting to update account numbers...")
    with arcpy.da.UpdateCursor(parcel_service_sj, ["AccountNum_final", "AcctNum", "TARGET_FID"]) as ucur:
        for urow in ucur:
            if urow[2] in gas_list:
                urow[0] = gas_list[urow[2]]
            else:
                urow[0] = urow[1]
            ucur.updateRow(urow)

    # spatial join between parcels and services to get service
    logging.info('Spatial join between utility parcels and limb service...')
    util_limb_sj = arcpy.SpatialJoin_analysis(utilityparcels, limb, 'util_limb_sj', 'JOIN_ONE_TO_ONE',
                                              'KEEP_ALL', '', 'HAVE_THEIR_CENTER_IN')
    logging.info('Spatial join between utility parcels and sanitation service...')
    util_sani_sj = arcpy.SpatialJoin_analysis(utilityparcels, sanitation, 'util_sani_sj', 'JOIN_ONE_TO_ONE',
                                              'KEEP_ALL', '', 'HAVE_THEIR_CENTER_IN')
    logging.info('Spatial join between utility parcels and recycle service...')
    util_recycle_sj = arcpy.SpatialJoin_analysis(utilityparcels, recycle, 'util_recycle_sj', 'JOIN_ONE_TO_ONE',
                                              'KEEP_ALL', '', 'HAVE_THEIR_CENTER_IN')

    # to avoid join table limitations, creating dictionaries to use in update cursor
    sj_id = 'TARGET_FID'
    oid = 'OBJECTID'
    whereclause = ['{} < 135653', '{} BETWEEN 135653 AND 271304', '{} > 271304']
    service_fields = ["OBJECTID", "Account", "Customer_Classification", "Limb_Pickup_Day", 
                    "Sanitation_Pickup_Day", "Recycle_Pickup_Day", "Recycle_Pickup_Week"]

    for w in whereclause:
        # service account number and customer classification dictionary
        logging.info("Creating account number and cust. class. dictionary for updating...")
        lutDict_ser = dict([(row[0], (row[1], row[2])) for row in arcpy.da.SearchCursor(parcel_service_sj,["TARGET_FID", "AccountNum_final", "CustClass"], w.format(sj_id))])
        logging.info("Creating DOW limb dictionary for updating...")
        lutDict_limb = dict([(row[0], (row[1])) for row in arcpy.da.SearchCursor(util_limb_sj, ["TARGET_FID", "DOW"], w.format(sj_id))])
        logging.info("Creating DOW sanitation dictionary for updating...")
        lutDict_sani = dict([(row[0], (row[1])) for row in arcpy.da.SearchCursor(util_sani_sj, ["TARGET_FID", "DOW"], w.format(sj_id))])
        logging.info("Creating DOW and Week recycle dictionary for updating...")
        lutDict_recycle = dict([(row[0], (row[1], row[2])) for row in arcpy.da.SearchCursor(util_recycle_sj, ["TARGET_FID", "Weekday", "Week"], w.format(sj_id))])
        
        
        # update cursor for utility parcels output
        logging.info("Updating Account, Customer Classification, limb, and sanitation...")
        with arcpy.da.UpdateCursor(utilityparcels, service_fields, w.format(oid)) as ucur:
            for urow in ucur:
                joinFld = urow[0]
                if joinFld in lutDict_ser.keys():
                    urow[1] = lutDict_ser[joinFld][0]
                    urow[2] = lutDict_ser[joinFld][1]
                if joinFld in lutDict_limb.keys():
                    urow[3] = lutDict_limb[joinFld]
                if joinFld in lutDict_sani.keys():
                    urow[4] = lutDict_sani[joinFld]
                if joinFld in lutDict_recycle.keys():
                    urow[5] = lutDict_recycle[joinFld][0]
                    urow[6] = lutDict_recycle[joinFld][1]
                                                 
                ucur.updateRow(urow)

    logging.info("Ready to populate UtilityParcels in SDE!")

    return utilityparcels

def publishUtilityParcels(utilityparcels_f, utilityparcels_sde):

    logging.info("Updating UtilityParcels and MyGovernmentServices map service")
    try:
        # getting token
        token = get_token(admin_user, admin_pass, server_name, port, expiration)

        # stopping service
        action = 'stop'
        json_output = serviceStartStop(server_name, port, service_name, action, token)
        # verify success
        if json_output['status'] == 'success':
            logging.info('{} was stopped successfully'.format(service_name))
        else:
            logging.info('Failed to stop {}'.format(service_name))
            raise Exception(json_output)

    except Exception, e:
        logging.info(e)

    # appending new data to utility parcel
    logging.info("appending new data to UtilityParcels...")
    arcpy.DeleteRows_management(utilityparcels_sde)
    arcpy.Append_management(utilityparcels_f, utilityparcels_sde, "NO_TEST")

    try:
        # starting service
        action = 'start'
        json_output = serviceStartStop(server_name, port, service_name, action, token)
        # verify success
        if json_output['status'] == 'success':
            logging.info('{} was started successfully'.format(service_name))
        else:
            logging.info('Failed to start {}'.format(service_name))
            raise Exception(json_output)

    except Exception, e:
        logging.info(e)

def get_token(adminuser, adminpass, server, port, exp):
    '''Generates token'''
    logging.info("getting token")
    
    # build url
    url = r"https://{}:{}/arcgis/admin/generateToken?f=json".format(server, port)

    # dict for query string, used to request token
    query_dict = {"username":adminuser, "password":adminpass, "expiration":str(exp), "client":"requestip"}
    query_string = urllib.urlencode(query_dict)

    try:
        # request token, will close url after completed
        with contextlib.closing(urllib2.urlopen(url, query_string)) as json_response:
            token_result = json.loads(json_response.read())
            if 'token' not in token_result or token_result == None:
                raise Exception('Failed to get token: {}'.format(token_result['messages']))
            else:
                return token_result['token']

    except urllib2.URLError, e:
        raise Exception('Could not connect to {} on port {}. {}'.format(server, port, e))

def serviceStartStop(server, port, service, action, token):
    '''Starts or stops service'''
    logging.info("{} service".format(action))

    # build url
    url = r"https://{}:{}/arcgis/admin".format(server, port)
    request_url = url + r"/services/MyCityServices/{}/{}".format(service, action)
    logging.info(request_url)

    # dict for query string, used to send request to start/stop
    query_dict = {"token": token, "f": "json"}
    query_string = urllib.urlencode(query_dict)

    # send request, close after complete
    with contextlib.closing(urllib.urlopen(request_url, query_string)) as json_response:
        logging.info(json_response)
        return json.loads(json_response.read())
    
if __name__ == "__main__":

    try:

        # env
        arcpy.env.overwriteOutput = True

        # inputs
        sde_cxn= r'D:\sdeConn\GISProd_Alias\Alias@GISProd@sde@sdeCity.sde'
        gisadmin_cxn = r"D:\sdeConn\GISProd_Alias\Alias@GISProd@GISAdmin@sdeCity.sde"
        
        up_fldr = r"D:\prod-scripts\utility-billing"

        # maintain log file
        current = datetime.today()
        logfile = up_fldr + r"\logs\updateUtilityParcel_log_{0}_{1}.txt".format(current.month, current.year)
        logging.basicConfig(filename=logfile,
                            level=logging.INFO,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Starting run... \n")

        # input workspaces
        gdb = up_fldr + r"\working.gdb"
        arcpy.env.workspace = gdb

        # service inputs 
        # credentials
        admin_user = "siteadmin"
        admin_pass = "colg1sadmin"
        # server info
        server_name = "gis-app.lawrencevillega.org"
        port = "6443"
        # service name
        service_name = "MyCityServices.MapServer"
        # token expires in 12 hours
        expiration = 720

        # removing version
        versions = [ver.name for ver in arcpy.da.ListVersions(gisadmin_cxn)]
        if 'GISADMIN.updateParcels' in versions:
            arcpy.DeleteVersion_management(gisadmin_cxn, 'GISADMIN.updateParcels')
        # set up version
        logging.info("creating updateParcels version")
        arcpy.CreateVersion_management(gisadmin_cxn, 'sde.DEFAULT', 'updateParcels', 'PRIVATE')
        logging.info("creating connection file")
        arcpy.CreateDatabaseConnection_management(up_fldr, "updateParcels@sdeCity", "SQL_SERVER", r"ch-server-sql\sql2019GISProd", "DATABASE_AUTH",
                                                "GISAdmin", "G1SAdm1n!", "SAVE_USERNAME", "sdeCity", "", "TRANSACTIONAL", "GISADMIN.updateParcels")
        parcel_gisadmin_cxn = r"D:\prod-scripts\utility-billing\updateParcels@sdeCity.sde"

        # input FDS
        datamining_fds = parcel_gisadmin_cxn + r"\sdeCity.GISADMIN.DataMining"
        facilstreets_fds = parcel_gisadmin_cxn + r"\sdeCity.GISADMIN.FacilitiesStreets"

        # input FC
        parcelsall_fc = datamining_fds + r"\sdeCity.GISADMIN.ParcelsAll"
        # new version was empty - using parent service info feature class
        serviceinfo_fc = gisadmin_cxn + r"\sdeCity.GISADMIN.DataMining\sdeCity.GISADMIN.ServiceInfo"
        servicearea_fc = datamining_fds + r"\sdeCity.GISADMIN.CityUtilityServiceArea"
        limb_fc = facilstreets_fds + r"\sdeCity.GISADMIN.LimbRoutes"
        sanitation_fc = facilstreets_fds + r"\sdeCity.GISADMIN.SanitationRoutes"
        recycle_fc = facilstreets_fds + r'\sdeCity.GISADMIN.RecycleRoutes'
        
        # output FC
        utilityparcels_fc = datamining_fds + r"\sdeCity.GISADMIN.UtilityParcels"

        # run modules
        logging.info('Running prepUtilityParcels')
        utilityparcels_out = prepUtilityParcels(gdb, parcelsall_fc, servicearea_fc)
        logging.info('Running populateServiceFields')
        utilityparcels_final = populateServiceFields(utilityparcels_out, serviceinfo_fc, limb_fc, sanitation_fc, recycle_fc)
        logging.info('Running publishUtilityParcels')
        publishUtilityParcels(utilityparcels_final, utilityparcels_fc)
        
        # clean up, aisle 5
        logging.info("reconcile and posting edits to sde.DEFAULT")
        arcpy.ReconcileVersions_management(parcel_gisadmin_cxn, "ALL_VERSIONS", "sde.DEFAULT", "GISADMIN.updateParcels", "LOCK_ACQUIRED", "", "", "", 
                                           "POST", "KEEP_VERSION", up_fldr + r"\logs\updateParcelsReconcile.txt")
        logging.info('deleting sde connection to updateParcels version')
        if os.path.exists(parcel_gisadmin_cxn):
            os.remove(parcel_gisadmin_cxn)

        logging.info("Deleting version updateParcels...") 
        # using sde owner, not gisadmin
        user_name = 'GISADMIN'
        users = arcpy.ListUsers(sde_cxn)
        for u in users:
            logging.info('{0} - {1}'.format(u.Name, u.ConnectionTime))
            if u.Name == user_name:
                arcpy.DisconnectUser(sde_cxn, u.ID)
        arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateParcels')

        logging.info("Success! \n ------------------------------------ \n\n")

    except Exception as e:
        logging.error("EXCEPTION OCCURRED", exc_info=True)

        # removing version
        versions = [ver.name for ver in arcpy.da.ListVersions(gisadmin_cxn)]
        if 'GISADMIN.updateParcels' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateParcels')
        if os.path.exists(parcel_gisadmin_cxn):
            os.remove(parcel_gisadmin_cxn)

        # starting service 
        token = get_token(admin_user, admin_pass, server_name, port, expiration)
        action = 'start'
        json_output = serviceStartStop(server_name, port, service_name, action, token)
        # verify success
        if json_output['status'] == 'success':
            logging.info('{} was started successfully'.format(service_name))
        else:
            logging.info('Failed to start {}'.format(service_name))
            raise Exception(json_output)  

        logging.info("Quitting! \n ------------------------------------ \n\n")


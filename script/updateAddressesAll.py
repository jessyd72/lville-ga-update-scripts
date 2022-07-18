'''
' ____________________________________________________________________
' Lawrenceville, GA
'_____________________________________________________________________
'
'   Program:    updateAddressesAll.py
'   Purpose:    Updates the AddressesAll feature class
'_____________________________________________________________________
'   History:    GTG     11/2019     Created
                JB      12/2020     Updated to mimic other scripts'
                                    processes and adjust for versioned
                                    environment (no truncate!!)
                JB      05/2021     Updated Municipality field for 
                                    Gwinnett field mapping after new
                                    data delivery
                JB      04/2022     Maintenance
'_____________________________________________________________________
'''

import arcpy
from arcpy import env
import os
from datetime import datetime
import logging

arcpy.env.overwriteOutput = True

def prepAddressesAll(fgdb, addressall, gwinnett, rockdale, walton):

    # create addressesall fc in fgdb for working
    logging.info('Creating temporary fc...')
    addall_f = arcpy.CreateFeatureclass_management(fgdb, 'AddressesAll_f', 'POLYGON', addressall, spatial_reference=addressall)
    logging.info('Copying Gwinnett addresses...')
    gwinnett_copy = arcpy.Copy_management(address_gwinnett, fgdb + r'\address_gwinnett')
    logging.info('Copying Rockdale addresses...')
    rockdale_copy = arcpy.Copy_management(address_rockdale, fgdb + r'\address_rockdale')
    logging.info('Copying Walton addresses...')
    walton_copy = arcpy.Copy_management(address_walton, fgdb + r'\address_walton')
    
    logging.info('Adding fields...')
    add_fields = ['geo_Number', 'geo_Address', 'geo_City', 'geo_State', 'geo_Zip']
    for f in add_fields:
        arcpy.AddField_management(addall_f, f, 'TEXT')
        
    flds = {gwinnett_copy: [["FULLADDR","geo_Address"], ["MUNICIPALITY","geo_City"], ["ZIP5", "geo_Zip"]],
    rockdale_copy: [["ADDR","geo_Number"], ["Street_Nam","geo_Address"], ["City_Name", "geo_City"]],
    walton_copy: [["ADDR", "geo_Address"], ["Mail_City", "geo_City"], ["Zip_Code", "geo_Zip"]]}

    for k, v in flds.items():

        fc = k
        for f in v:
            f_orig = f[0]
            f_new = f[1]
            arcpy.AddField_management(fc, f_new, 'TEXT')
            arcpy.CalculateField_management(fc, f_new, '!{}!'.format(f_orig), 'PYTHON_9.3')

    keep_flds = ["CREATED_USER", "CREATED_DATE", "LAST_EDITED_USER", "LAST_EDITED_DATE"]
    for add_fc in [gwinnett_copy, rockdale_copy, walton_copy]:
        all_flds = [fld.name for fld in arcpy.ListFields(add_fc) if (not fld.required and fld.name not in keep_flds)]
        del_flds = [n for n in all_flds if n not in add_fields]
        logging.info('Deleting fields from {}...'.format(add_fc))
        arcpy.DeleteField_management(add_fc, del_flds)

    # append addresses to addressesAll
    logging.info('Appending addresses to AddressesAll_f...')
    arcpy.Append_management([gwinnett, rockdale, walton], addall_f, 'NO_TEST')

    return(addall_f)

def populateAddressesAll(addressall):

    logging.info('Updating Full_Address field...')
    with arcpy.da.UpdateCursor(addressall, ['Full_Address', 'geo_Number', 'geo_Address', 'geo_City', 'geo_State', 'geo_Zip']) as ucur:
        for row in ucur:

            for row in ucur:

                if row[4] == None:
                    state = 'GA'
                else:
                    state = row[4]

                l = [row[1], row[2], row[3], state, row[5]]
                new_l = [x for x in l if x != None]

                full_add = ' '.join(new_l)

                row[0] = full_add
                ucur.updateRow(row)

    logging.info('Deleting parsed address fields...')
    del_fields = ['geo_Number', 'geo_Address', 'geo_City', 'geo_State', 'geo_Zip']
    arcpy.DeleteField_management(addressall, del_fields)

    return(addressall)

def updateAddressesAllSDE(addall_f, addall_sde):

    # delete rows from addressesAll in SDE
    logging.info('Deleting rows...')
    arcpy.DeleteRows_management(addall_sde)

    # append addressesAll to addressesAll in SDE
    logging.info('Appending rows...')
    arcpy.Append_management(addall_f, addall_sde, 'TEST')

    logging.info('AddressesAll is updated!')


if __name__ == '__main__':

    try:

        # env
        arcpy.env.overwriteOutput = True

        # inputs
        sde_cxn = r"D:\sdeConn\GISAdmin@sdeCity.sde"
        working_fldr = r'D:\prod-scripts\addressesall'

        # maintain log file
        current = datetime.today()
        logfile = working_fldr + r"\logs\addressessAll_log_{0}_{1}.txt".format(current.month, current.year)
        logging.basicConfig(filename=logfile,
                            level=logging.INFO,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Starting run... \n")
        
        # removing version check
        versions = [ver.name for ver in arcpy.da.ListVersions(sde_cxn)]
        if 'GISADMIN.updateAddressesAll' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateAddressesAll')
        # set up version
        logging.info("creating updateAddressesAll version")
        arcpy.CreateVersion_management(sde_cxn, 'sde.DEFAULT', 'updateAddressesAll', 'PRIVATE')
        logging.info("creating connection file")
        arcpy.CreateDatabaseConnection_management(working_fldr, "updateAddressesAll@sdeCity", "SQL_SERVER", r"Blade-3\SQL2014", "DATABASE_AUTH",
                                                "GISAdmin", "G1SAdm1n!", "SAVE_USERNAME", "sdeCity", "", "TRANSACTIONAL", "GISADMIN.updateAddressesAll")

        addressesAll_sde_cxn = working_fldr + r"\updateAddressesAll@sdeCity.sde"
        datamining_fds = addressesAll_sde_cxn + r'\sdeCity.GISADMIN.DataMining'
        external_fds = addressesAll_sde_cxn + r"\sdeCity.GISADMIN.ExternalData"

        # workspace
        fgdb = working_fldr + r'\addressesAll.gdb'
        arcpy.env.workspace = fgdb
        
        # input feature classes
        address_gwinnett = external_fds + r"\sdeCity.GISADMIN.GwinnettAddresses"
        address_rockdale = external_fds + r"\sdeCity.GISADMIN.RockdaleAddresses"
        address_walton = external_fds + r"\sdeCity.GISADMIN.WaltonAddresses"

        # output feature
        addressesall_fc = datamining_fds + r'\sdeCity.GISADMIN.AddressesAll'

        # execute functs
        logging.info('Running prepAddressesAll')
        addAll_out = prepAddressesAll(fgdb, addressesall_fc, address_gwinnett, address_rockdale, address_walton)
        logging.info('Running populateAddressesAll')
        addAll_final = populateAddressesAll(addAll_out)
        logging.info('Running updateAddressesAllSDE')
        updateAddressesAllSDE(addAll_final, addressesall_fc)

        # clean up, aisle 5
        logging.info("reconcile and posting edits to sde.DEFAULT")
        logging.info("version updateAddressesAll will be deleted...")
        arcpy.ReconcileVersions_management(addressesAll_sde_cxn, "ALL_VERSIONS", "sde.DEFAULT", "GISADMIN.updateAddressesAll", "LOCK_ACQUIRED", "", "", "", 
                                           "POST", "DELETE_VERSION", working_fldr + r"\logs\updateAddressesAllReconcile.txt")
        logging.info('deleting sde connection to removed version')
        if os.path.exists(addressesAll_sde_cxn):
            os.remove(addressesAll_sde_cxn)

        logging.info("Success! \n ------------------------------------ \n\n")

    except Exception as e:
        logging.error("EXCEPTION OCCURRED", exc_info=True)

        # removing version
        versions = [ver.name for ver in arcpy.da.ListVersions(sde_cxn)]
        if 'GISADMIN.updateAddressesAll' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateAddressesAll')
        if os.path.exists(addressesAll_sde_cxn):
            os.remove(addressesAll_sde_cxn)

        logging.info("Quitting! \n ------------------------------------ \n\n")

        

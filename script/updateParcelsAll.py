'''
 ____________________________________________________________________
 Lawrenceville, GA
_____________________________________________________________________

   Program:    updateParcelsAll.py
   Purpose:    Updates the ParcelsAll feature class. Run ad-hoc.
_____________________________________________________________________
   History:     GTG     11/2020     Created
_____________________________________________________________________
'''

import arcpy
from arcpy import env
import os
from datetime import datetime
import logging

def prepParcelsAll(gdb, parcelsall, gwinnett, rockdale, walton):

    # create parcelsall fc in fgdb for working
    parcelsall_f = arcpy.CreateFeatureclass_management(gdb, 'ParcelsAll_f', 'POLYGON', parcelsall, spatial_reference=parcelsall)

    # field mappings for append
    logging.info('Building fieldmappings...')
    fm_gwinnett = arcpy.FieldMap()
    fm_rockdale = arcpy.FieldMap()
    fm_walton = arcpy.FieldMap()
    fms = arcpy.FieldMappings()

    fm_gwinnett.addInputField(gwinnett, 'PIN')
    fm_rockdale.addInputField(rockdale, 'PARCEL_NO')
    fm_walton.addInputField(walton, 'Parcel_No')

    gwinnet_fld = fm_gwinnett.outputField
    gwinnet_fld.name = 'Parcel_No'
    fm_gwinnett.outputField = gwinnet_fld

    rockdale_fld = fm_rockdale.outputField
    rockdale_fld.name = 'Parcel_No'
    fm_rockdale.outputField = rockdale_fld

    walton_fld = fm_walton.outputField
    walton_fld.name = 'Parcel_No'
    fm_walton.outputField = walton_fld

    fms.addFieldMap(fm_gwinnett)
    fms.addFieldMap(fm_rockdale)
    fms.addFieldMap(fm_walton)

    # append parcels to parcelsAll
    arcpy.Append_management([gwinnett, rockdale, walton], parcelsall_f, 'NO_TEST', fms)

    return(parcelsall_f)

def populateParcelsAll(parcelsall, addressall):

    # spatial join between parcels and addresses to get full address field
    logging.info("Spatial join between ParcelsAll and AddressesAll...")
    parcel_address_sj = arcpy.SpatialJoin_analysis(parcelsall, addressall, "par_add_sj", "JOIN_ONE_TO_ONE", "KEEP_ALL", "", "INTERSECT")

    # to avoid join table limitations, creating dictionaries to use in update cursor
    logging.info("Creating full address and account/cust. calss. dictionary for updating...")
    sj_id = 'TARGET_FID'
    oid = 'OBJECTID'
    whereclause = ['{} < 135653', '{} BETWEEN 135653 AND 271304', '{} > 271304']

    for w in whereclause:
        # full address dictionary
        lutDict_add = dict([(row[0], (row[1])) for row in arcpy.da.SearchCursor(parcel_address_sj, ["TARGET_FID", "Full_Address_1"], w.format(sj_id))])
    
        # update cursor for final ParcelsAll output
        logging.info("Updating Full Address")
        with arcpy.da.UpdateCursor(parcelsall, ["OBJECTID", "Full_Address"], w.format(oid)) as ucur:
            for urow in ucur:
                joinFld = urow[0]
                if joinFld in lutDict_add.keys():
                    urow[1] = lutDict_add[joinFld]
                ucur.updateRow(urow)
        lutDict_add.clear()
        del(lutDict_add)

    return(parcelsall)

def updateParcelsAllSDE(parcelsall_f, parcelsall_sde):

    # delete rows from parcelsAll in SDE
    logging.info('Deleting rows...')
    arcpy.DeleteRows_management(parcelsall_sde)

    # append parcelsAll to parcelsAll in SDE
    logging.info('Appending rows...')
    arcpy.Append_management(parcelsall_f, parcelsall_sde, 'TEST')

    logging.info('ParcelsAll is updated!')


if __name__ == '__main__':

    try:

        # env
        arcpy.env.overwriteOutput = True

        # inputs
        sde_cxn = r"D:\sdeConn\GISAdmin@sdeCity.sde"
        working_fldr = r'D:\prod-scripts\parcelsall'

        # maintain log file
        current = datetime.today()
        logfile = working_fldr + r"\logs\parcelsAll_log_{0}_{1}.txt".format(current.month, current.year)
        logging.basicConfig(filename=logfile,
                            level=logging.INFO,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Starting run... \n")
        
        # removing version check
        versions = [ver.name for ver in arcpy.da.ListVersions(sde_cxn)]
        if 'GISADMIN.updateParcelsAll' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateParcelsAll')
        # set up version
        logging.info("creating updateparcelsAll version")
        arcpy.CreateVersion_management(sde_cxn, 'sde.DEFAULT', 'updateParcelsAll', 'PRIVATE')
        logging.info("creating connection file")
        arcpy.CreateDatabaseConnection_management(working_fldr, "updateParcelsAll@sdeCity", "SQL_SERVER", r"Blade-3\SQL2014", "DATABASE_AUTH",
                                                "GISAdmin", "G1SAdm1n!", "SAVE_USERNAME", "sdeCity", "", "TRANSACTIONAL", "GISADMIN.updateParcelsAll")

        parcelsAll_sde_cxn = working_fldr + r"\updateParcelsAll@sdeCity.sde"
        datamining_fds = parcelsAll_sde_cxn + r'\sdeCity.GISADMIN.DataMining'
        external_fds = parcelsAll_sde_cxn + r"\sdeCity.GISADMIN.ExternalData"

        # workspace
        fgdb = working_fldr + r'\parcelsAll.gdb'
        arcpy.env.workspace = fgdb
        
        # feature classes
        addressesall_fc = datamining_fds + r'\sdeCity.GISADMIN.AddressesAll'
        parcelsall_fc = datamining_fds + r'\sdeCity.GISADMIN.ParcelsAll'
        parcel_gwinnett = external_fds + r"\sdeCity.GISADMIN.GwinnettParcels"
        parcel_rockdale = external_fds + r"\sdeCity.GISADMIN.RockdaleParcels"
        parcel_walton = external_fds + r"\sdeCity.GISADMIN.WaltonParcels"

        # execute functs
        logging.info('Running prepParcelsAll')
        parcelsAll_out = prepParcelsAll(fgdb, parcelsall_fc, parcel_gwinnett, parcel_rockdale, parcel_walton)
        logging.info('Running populateParcelsAll')
        parcelsAll_final = populateParcelsAll(parcelsAll_out, addressesall_fc)
        logging.info('Running updateParcelsAllSDE')
        updateParcelsAllSDE(parcelsAll_final, parcelsall_fc)

        # clean up, aisle 5
        logging.info("reconcile and posting edits to sde.DEFAULT")
        logging.info("version updateparcelsAll will be deleted...")
        arcpy.ReconcileVersions_management(parcelsAll_sde_cxn, "ALL_VERSIONS", "sde.DEFAULT", "GISADMIN.updateParcelsAll", "LOCK_ACQUIRED", "", "", "", 
                                           "POST", "DELETE_VERSION", working_fldr + r"\logs\updateparcelsAllReconcile.txt")
        logging.info('deleting sde connection to removed version')
        if os.path.exists(parcelsAll_sde_cxn):
            os.remove(parcelsAll_sde_cxn)

        logging.info("Success! \n ------------------------------------ \n\n")

    except Exception as e:
        logging.error("EXCEPTION OCCURRED", exc_info=True)

        # removing version
        versions = [ver.name for ver in arcpy.da.ListVersions(sde_cxn)]
        if 'GISADMIN.updateParcelsAll' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateParcelsAll')
        if os.path.exists(parcelsAll_sde_cxn):
            os.remove(parcelsAll_sde_cxn)

        logging.info("Quitting! \n ------------------------------------ \n\n")

        

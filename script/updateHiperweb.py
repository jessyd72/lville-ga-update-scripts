'''
 ____________________________________________________________________
 Lawrenceville, GA
_____________________________________________________________________

   Program:    updateHiperweb.py
   Purpose:    Updates the ParcelsHiperweb feature class. Runs ad-hoc.
_____________________________________________________________________
   History:     GTG     11/2020     Created
_____________________________________________________________________
'''

import arcpy
from arcpy import env
import os
import json
from datetime import datetime
import logging

def prepHiperweb(gdb, hiperweb, parcelsall, parcelno, fulladd):

    # create hiperweb fc in fdb for working
    logging.info('Creating empty fc with hiperweb template...')
    hiperweb_f = arcpy.CreateFeatureclass_management(gdb, 'ParcelsHiperweb_f', 'POLYGON', hiperweb, '', '', hiperweb)

    # field mappings for append
    logging.info('Building fieldmappings...')
    fm_parcelno = arcpy.FieldMap()
    fm_fulladd = arcpy.FieldMap()
    fms = arcpy.FieldMappings()

    fm_parcelno.addInputField(parcelsall, parcelno)
    fm_fulladd.addInputField(parcelsall, fulladd)

    parcelno_fld = fm_parcelno.outputField
    parcelno_fld.name = parcelno
    fm_parcelno.outputField = parcelno_fld

    fulladd_fld = fm_fulladd.outputField
    fulladd_fld.name = fulladd
    fm_fulladd.outputField = fulladd_fld

    fms.addFieldMap(fm_parcelno)
    fms.addFieldMap(fm_fulladd)

    # append parcelsAll to Hiperweb
    logging.info('Appending rows...')
    arcpy.Append_management(parcelsall, hiperweb_f, 'NO_TEST', fms)

    return(hiperweb_f)

def populateHiperweb(hiperweb, fulladd, hiperweb_fld, stnum, stname, sttype, predir, postdir):

    logging.info('Entering cursor...')
    
    with arcpy.da.UpdateCursor(hiperweb, [fulladd, hiperweb_fld, stnum, stname, sttype, predir, postdir]) as ucur:
        for row in ucur:
            if row[0] != None:
                address_split = row[0].split(' GA ')
                if len(address_split) > 2:
                    address = ' '.join((row[0].split(' '))[:-2]).replace('<Null>', '').replace('<Nul.l>', '').replace('  ',' ').replace('  ',' ').strip()
                else:
                    address = ''.join(row[0].split(' GA ')[0]).replace('<Null>', '').replace('<Nul.l>', '').replace('  ',' ').replace('  ',' ').strip()
                # hiperweb_fld
                row[1] = address

                # clean up for 2430 Tucker Dr... GYST
                if address.split(' ')[1] == '-':
                    templist = address.split(' ')
                    del(templist[1:3])
                    address = ' '.join(templist)

                # removing text after comma, or text after subadd val, or removing city
                commacheck = address.split(',')
                dashcheck = address.split('-')
                if len(commacheck) > 1:
                    del(commacheck[1:])
                    address_list = (''.join(commacheck).strip()).split(' ')
                elif len(dashcheck) > 1:
                    del(dashcheck[1:])
                    address_list = (''.join(dashcheck).strip()).split(' ')
                else:
                    address_list = address.split(' ')
                    subadd = [address_list.index(s) for s in address_list if s in subadd_list]
                    if subadd:
                        del(address_list[subadd[0]:])
                    else: 
                        if address_list[-1].upper() in city_list:
                            del(address_list[-1])
                        elif ' '.join(address_list[-2:]) in city_list:
                            del(address_list[-2:])

                # all invalid nulls, sub addresses, city, state, and zip info is removed!!

                if address_list and address_list[0].isdigit():
                    addnum = address_list[0]
                    # stnum
                    row[2] = addnum
                    address_list.remove(addnum)

                if address_list and address_list[0] in dir_list:
                    predir_val = address_list[0]
                    # predir
                    row[5] = predir_val
                    address_list.remove(predir_val)

                if address_list and address_list[-1] in dir_list:
                    postdir_val = address_list[-1]
                    # postdir
                    row[6] = postdir_val
                    address_list.remove(postdir_val)

                if address_list and address_list[0] == 'HWY':
                    stname_val = ' '.join(address_list[:2])
                    # stname
                    row[3] = stname_val
                elif address_list and address_list[-1] in sttype_list:
                    sttype_val = address_list[-1]
                    # sttype
                    row[4] = sttype_val
                    address_list.remove(sttype_val)
                    stname_val = ' '.join(address_list)
                    # stname
                    row[3] = stname_val
                else:
                    stname_val = ' '.join(address_list)
                    # stname
                    row[3] = stname_val

            ucur.updateRow(row)

    logging.info('Finished!')

    return(hiperweb)

def updateHiperwebSDE(hiperweb_f, hiperweb_sde):

    # delete rows from hiperweb
    logging.info('Deleting rows...')
    arcpy.DeleteRows_management(hiperweb_sde)

    # append parcelsAll to Hiperweb
    logging.info('Appending rows...')
    arcpy.Append_management(hiperweb_f, hiperweb_sde, 'TEST')

    logging.info('ParcelsHiperweb is updated!')


if __name__ == '__main__':

    try:

        # env
        arcpy.env.overwriteOutput = True

        # inputs
        sde_cxn = r"D:\sdeConn\GISAdmin@sdeCity.sde"
        working_fldr = r'D:\prod-scripts\hiperweb'

        # maintain log file
        current = datetime.today()
        logfile = working_fldr + r"\logs\Hiperweb_log_{0}_{1}.txt".format(current.month, current.year)
        logging.basicConfig(filename=logfile,
                            level=logging.INFO,
                            format='%(levelname)s: %(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')
        logging.info("Starting run... \n")
        
        # load json array for global lists
        text = open(working_fldr + r"\supp_data\parsing_lists.json").read()
        json_array = json.loads(text)

        dir_list = json_array['dir_list']
        subadd_list = json_array['subadd_list']
        city_list = json_array['city_list']
        sttype_list = json_array['sttype_list']

        # removing version check
        versions = [ver.name for ver in arcpy.da.ListVersions(sde_cxn)]
        if 'GISADMIN.updateHiperweb' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateHiperweb')
        # set up version
        logging.info("creating updateHiperweb version")
        arcpy.CreateVersion_management(sde_cxn, 'sde.DEFAULT', 'updateHiperweb', 'PRIVATE')
        logging.info("creating connection file")
        arcpy.CreateDatabaseConnection_management(working_fldr, "updateHiperweb@sdeCity", "SQL_SERVER", r"Blade-3\SQL2014", "DATABASE_AUTH",
                                                "GISAdmin", "G1SAdm1n!", "SAVE_USERNAME", "sdeCity", "", "TRANSACTIONAL", "GISADMIN.updateHiperweb")
        hiperweb_sde_cxn = working_fldr + r"\updateHiperweb@sdeCity.sde"
        datamining_fds = hiperweb_sde_cxn + r'\sdeCity.GISADMIN.DataMining'

        # workspace
        fgdb = working_fldr + r'\Hiperweb.gdb'
        arcpy.env.workspace = fgdb
        
        # feature classes
        hiperweb_fc = datamining_fds + r'\sdeCity.GISADMIN.ParcelsHiperweb'
        parcelsall_fc = datamining_fds + r'\sdeCity.GISADMIN.ParcelsAll'

        # fields
        parcelno_fld = 'Parcel_No'
        fulladd_fld = 'Full_Address'
        hiperweb_fld = 'Hiperweb_Address'
        addnum_fld = 'StreetNumber'
        stname_fld = 'StreetName'
        sttype_fld = 'StreetType'
        predir_fld = 'PreDirection'
        postdir_fld = 'PostDirection'

        # execute functs
        logging.info('Running prepHiperweb')
        hiperweb_out = prepHiperweb(fgdb, hiperweb_fc, parcelsall_fc, parcelno_fld, fulladd_fld)
        logging.info('Running populateHiperweb')
        hiperweb_final = populateHiperweb(hiperweb_out, fulladd_fld, hiperweb_fld, addnum_fld, stname_fld, sttype_fld, predir_fld, postdir_fld)
        logging.info('Running updateHiperwebSDE')
        updateHiperwebSDE(hiperweb_final, hiperweb_fc)

        # clean up, aisle 5
        logging.info("reconcile and posting edits to sde.DEFAULT")
        logging.info("version updateHiperweb will be deleted...")
        arcpy.ReconcileVersions_management(hiperweb_sde_cxn, "ALL_VERSIONS", "sde.DEFAULT", "GISADMIN.updateHiperweb", "LOCK_ACQUIRED", "", "", "", 
                                           "POST", "DELETE_VERSION", working_fldr + r"\logs\updateHiperwebReconcile.txt")
        logging.info('deleting sde connection to removed version')
        if os.path.exists(hiperweb_sde_cxn):
            os.remove(hiperweb_sde_cxn)

        logging.info("Success! \n ------------------------------------ \n\n")

    except Exception as e:
        logging.error("EXCEPTION OCCURRED", exc_info=True)

        # removing version
        versions = [ver.name for ver in arcpy.da.ListVersions(sde_cxn)]
        if 'GISADMIN.updateHiperweb' in versions:
            arcpy.DeleteVersion_management(sde_cxn, 'GISADMIN.updateHiperweb')
        if os.path.exists(hiperweb_sde_cxn):
            os.remove(hiperweb_sde_cxn)

        logging.info("Quitting! \n ------------------------------------ \n\n")

        

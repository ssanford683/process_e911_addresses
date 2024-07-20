## author; Steve S.
## date: 6/2018
## updated for python 3.6: 11/2019
## script processes newly submitted address points, punching NGUID, copying new and edited points to history table, and alerting various entities of updates via email

# Import libraries
import time
import arcpy
import smtplib
import os
import sys
import re
import logging
import xml.etree.cElementTree as ET
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def main():

    # global variables
    global now, this_path, this_file, this_name, logfile, this_logger, westfield_recipients, noblesville_recipients, fishers_recipients, carmel_recipients, hamco_recipients, parks_recipients
    now = datetime.now()
    full_path = os.path.realpath(__file__)
    this_path = os.path.dirname(full_path)
    this_file = os.path.basename(full_path)
    this_name = os.path.splitext(full_path)
    this_parent = os.path.dirname(this_path)

    # email recipients
    westfield_recipients = ["imaginary_recipient@localgov.gov"]
    noblesville_recipients = ["imaginary_recipient@localgov.gov"]
    fishers_recipients = ["imaginary_recipient@localgov.gov"]
    carmel_recipients = ["imaginary_recipient@localgov.gov"]
    hamco_recipients = ["imaginary_recipient@localgov.gov"]
    parks_recipients = ["imaginary_recipient@localgov.gov"]

    # email recipients test
    # westfield_recipients = ["imaginary_recipient@localgov.gov"]
    # noblesville_recipients = ["imaginary_recipient@localgov.gov"]
    # fishers_recipients = ["imaginary_recipient@localgov.gov"]
    # carmel_recipients = ["imaginary_recipient@localgov.gov"]
    # hamco_recipients = ["imaginary_recipient@localgov.gov"]
    # parks_recipients = ["imaginary_recipient@localgov.gov"]

    # local variables
    xml_file = this_path + '\\xml\\last_time_checked.xml'
    sde_connection = this_path + "\\db_connections\\sa@prod.sde"

    # sde variables
    e911_addresses_fc =  sde_connection + "\\E911_Addresses"
    trail_mile_markers_fc = sde_connection + "\\PARKS_MileMarkers"
    e911_addresses_history =  sde_connection + "\\E911_Addresses_History"
    e911_addresses_alias = sde_connection + "\\E911_Addresses_Alias"
    e911_addresses_alias_history = sde_connection + "\\E911_Addresses_Alias_History"
    nguid_field = "NGUID"

    # local copy variables
    processing_gdb = this_path + "\\processing\\processing.gdb"
    items_to_copy_from_sde = ["terrapin.DBO.E911_Addresses", "terrapin.DBO.SUR_corp_limits", "terrapin.DBO.PARKS_MileMarkers"]
    e911_addresses_processing_fc = processing_gdb + "\\E911_Addresses"
    corp_limits_processing_fc = processing_gdb + "\\SUR_corp_limits"
    e911_addresses_with_corp_limits = processing_gdb + "\\E911_Addresses_Corp_Limits"
    cursor_fields = ["NGUID", "EDIT_PRIV", "EDIT_STATUS", "created_user", "created_date", "SUBTYPE", "LOC_NO", "LOC_NO_SUF",
              "LOC_PR_DIR", "LOC_ST", "LOC_ST_SUF", "LOC_DIR", "LOC_CITY", "LOC_STATE", "LOC_ZIP", "COMMENTS", "last_edited_user", "last_edited_date", "MUNI"]
    trail_mile_markers_offline_processing_fc = processing_gdb + "\\PARKS_MileMarkers"
    trail_mile_markers_with_corp_limits = processing_gdb + "\\PARKS_MileMarkers_Corp_Limits"

    # set logfile
    logfile = this_path + '\\logs\\{}_{}_{}.log'.format(str(now.year), str(now.month), str(now.day))
    # Create logging formatter
    fh_formatter = logging.Formatter('%(asctime)-12s %(funcName)-36s %(message)-96s', '%I:%M:%S %p')
    # Create logging file handler
    fh_debug = logging.FileHandler(logfile, 'w')
    fh_debug.setLevel(logging.DEBUG)
    fh_debug.setFormatter(fh_formatter)
    # Create logger object
    this_logger = logging.getLogger('this_logger')
    this_logger.setLevel(logging.DEBUG)
    this_logger.addHandler(fh_debug)

    # start log
    this_logger.debug("script started")

    # get last time xml was written, store as returned variable
    last_run = read_xml(xml_file)
    # write new time
    write_xml(xml_file)
    # process new points into history table
    transfer_to_history(e911_addresses_fc, e911_addresses_history, last_run)

    # nguid process
    next_value = get_next_value(e911_addresses_fc, nguid_field)
    add_global_id(sde_connection, e911_addresses_fc, next_value, last_run)

    # parks offline process -------------------------------------------------
    # nguid process
    next_value = get_next_value(e911_addresses_fc, nguid_field)
    add_global_id(sde_connection, trail_mile_markers_fc, next_value+200000, last_run)          # multi_user_editing_mode=False removed 2/18/2021
    # +200000 to ensure that next day same nguids don't get used in address pts if melissa hasn't copied over into address pts yet
    # -----------------------------------------------------------------------

    # get last time xml was written, store as returned variable
    this_run = read_xml(xml_file)
    # write new time
    write_xml(xml_file)
    # process new points into history table
    transfer_to_history(e911_addresses_fc, e911_addresses_history, this_run)

    # make processing copy to calculate CORP_LIMIT field without affecting editor tracking in sde dataset
    update_copy(sde_connection, items_to_copy_from_sde, processing_gdb)
    spatial_join(e911_addresses_processing_fc, corp_limits_processing_fc, e911_addresses_with_corp_limits)

    # process new submissions
    iterate_through_dataset(e911_addresses_with_corp_limits, cursor_fields, last_run, "points")

    # parks offline process ----------------------------
    # make processing copy to calculate CORP_LIMIT field without affecting editor tracking in sde dataset
    spatial_join(trail_mile_markers_offline_processing_fc, corp_limits_processing_fc, trail_mile_markers_with_corp_limits)
    # process new submissions
    iterate_through_dataset_parks_offline(trail_mile_markers_with_corp_limits, cursor_fields, last_run, "points collected offline")
    # --------------------------------------------------
    # end log
    this_logger.debug("script complete")

    # gather information to pass to and send email
    subject = str(this_file.split('.')[0])
    open_log = open(logfile, 'r')
    msg_html = open_log.read()
    open_log.close()
    recipients = ["steve.sanford@hamiltoncounty.in.gov", "joan.keene@hamiltoncounty.in.gov"]  # , "joan.keene@hamiltoncounty.in.gov"
    send_email(subject, msg_html, recipients, internal=True)

def read_xml(xml_file):

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        last_time_checked = root.find('time').text
        last_time_checked_date = datetime.strptime(last_time_checked[:19], '%Y-%m-%d %H:%M:%S')
        this_logger.debug("last time checked was: {}".format(str(last_time_checked_date)))

        return last_time_checked_date

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def write_xml(xml_file):

    try:
        root = ET.Element("root")
        ET.SubElement(root, "time").text = str(datetime.utcnow())
        tree = ET.ElementTree(root)
        tree.write(xml_file)
        this_logger.debug("new xml written")

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))


def transfer_to_history(in_table, out_table, last_time_checked_date):

    try:
        # get all fields, excluding shapes
        field_names = [f.name for f in arcpy.ListFields(in_table )if (f.name != 'Shape') and (f.name != 'SHAPE')]

        # copy into history table
        with arcpy.da.SearchCursor(in_table, field_names) as scursor:
            for row in scursor:
                punch_time = row[32]     # index number of last_edited_date field
                # print("punch_time is: " + str(punch_time))
                # print("last_time_checked_date is: " + str(last_time_checked_date))
                if punch_time > last_time_checked_date:
                    with arcpy.da.InsertCursor(out_table, field_names) as icursor:
                        icursor.insertRow(row)
                        this_logger.debug("inserted update of OBJECTID {} into {}".format(str(row[0]), str(out_table.split("\\")[-1])))
                    del icursor
        del scursor

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))


def get_next_value(in_fc, field):

    try:
        nguid_list = []
        nulls = ['None', '', ' ']
        with arcpy.da.SearchCursor(in_fc, field) as scursor:
            for row in scursor:
                #if (str(row[0]) <> 'None') or (str(row[0]) <> ' '):
                if str(row[0]) not in nulls:
                    nguid = str(row[0])
                    nguid_digits = int(''.join(i for i in nguid if i.isdigit()))
                    nguid_list.append(nguid_digits)

        max_value = max(nguid_list)
        this_logger.debug("current max NGUID value is: {}".format(str(max_value)))
        next_value = max_value + 1
        this_logger.debug("next NGUID value is: {}".format(str(next_value)))

        return next_value

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def add_global_id(sde_connection, in_fc, next_value, last_time_checked_date, multi_user_editing_mode=True):

    try:

        # update new blank address fc records with nguids, build dictionary of those entered as aliases
        alias_dict = {}
        edit = arcpy.da.Editor(sde_connection)
        edit.startEditing(False, multi_user_editing_mode)     # previously False, True
        edit.startOperation()
        fields = ["NGUID", "STATUS", "EDIT_STATUS", "created_user", "created_date"]
        with arcpy.da.UpdateCursor(in_fc, fields) as ucursor:
            for row in ucursor:
                nguid_value = str(row[0])
                #if (nguidValue == 'None') or (nguidValue == ' ') or (nguidValue == ''):                #and str(row[1]) == 'Approved':
                punch_time = row[4]
                if (punch_time > last_time_checked_date) and (str(row[3]) != "SA"):     #  IF ADMIN ADDS BACK IN ANY DELETED POINTS, ADD THIS TO THIS CONDITIONAL: and (str(row[3]) != 'SA')
                    row[0] = "ADD" + str(next_value) + "@HAMILTONCOUNTY.IN.GOV"
                    if row[3] != "MBO":   # if points submitted by anyone other than Rich, ensure status and edit status are set correctly. Rich doesn't have to review his own submitted points.
                        #row[1] = "Proposed"    # commented out because cities would like to submit as "Active" most often, but not always
                        #row[2] = "Submitted"    # change to "Tentatively Approved" when Rich leaves, for vacant seat period
                        row[2] = "Submitted"    # change to "Tentatively Approved" when Rich leaves, for vacant seat period
                    else:
                        #row[1] = "Active"  # commented out because cities would like to submit as "Active" most often, but not always
                        row[2] = "Approved"
                    ucursor.updateRow(row)
                    this_logger.debug("populated new {} feature NGUID with {}".format(str(in_fc.split("\\")[-1]), str(row[0])))
                    next_value = next_value + 1
                    this_logger.debug("new next_value is: {}".format(str(next_value)))
                elif len(nguid_value) == 4:                     # Find NGUID numbers that users have submitted with an alias in the the Addresses FC table (numbers should be 1, 2, 3, 4, up to a maximum of 1,000)
                    alias_dict.setdefault(nguid_value, []).append(str(next_value))          # Add value to dictionary for processing in alias table below
                    row[0] = "ADD" + str(next_value) + "@HAMILTONCOUNTY.IN.GOV"
                    if row[3] != "MBO":   # if points submitted by anyone other than Rich, ensure status and edit status are set correctly .  Rich doesn't have to review his own submitted points.
                        #row[1] = "Proposed"  # commented out because cities would like to submit as "Active" most often, but not always
                        #row[2] = "Submitted"    # change to "Tentatively Approved" when Rich leaves, for vacant seat period
                        row[2] = "Submitted"    # change to "Tentatively Approved" when Rich leaves, for vacant seat period
                    else:
                        #row[1] = "Active"  # commented out because cities would like to submit as "Active" most often, but not always
                        row[2] = "Approved"
                    ucursor.updateRow(row)
                    this_logger.debug("populated new point fc alias NGUID with {}".format(str(row[0])))
                    next_value = next_value + 1
                    this_logger.debug("new next_value is: {}".format(str(next_value)))

        edit.stopOperation()
        edit.stopEditing(True)

##        # alias table processing
##        print ""
##        print "Alias dictionary is: " + str(alias_dict)
##        edit = arcpy.da.Editor(sde_connection)
##        edit.startEditing(False, True)
##        edit.startOperation()
##        with arcpy.da.UpdateCursor(e911AddressesAlias, ["NGUID", "STATUS", "EDIT_STATUS", "created_user"]) as cursor:
##            for row in cursor:
##                nguid_value = str(row[0])
##                for key, value in alias_dict.iteritems():
##                    if key == nguidValue:
##                        row[0] = "ADD" + str(value[0]) + "@HAMILTONCOUNTY.IN.GOV"
##                        if row[3] != "RANDERSON":   # if aliases submitted by anyone other than Rich, ensure status and edit status are set correctly.  Rich doesn't have to review his own submitted aliases.
##                            row[1] = "Proposed"
##                            row[2] = "Submitted"
##                        else:
##                            row[1] = "Active"
##                            row[2] = "Approved"
##                        cursor.updateRow(row)
##                        print "Populated alias table NGUID with " + str(row[0])
##                    else:
##                        pass
##
##        edit.stopOperation()
##        edit.stopEditing(True)
##        print ""

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))


def update_copy(in_directory, in_list, out_directory):

    try:
        for item in in_list:
            arcpy.TruncateTable_management(out_directory + "\\" + item.split(".")[-1])
            this_logger.debug("truncated existing processing copy of {}".format(item.split(".")[-1]))
            arcpy.Append_management(in_directory + "\\" + item, out_directory + "\\" + item.split(".")[-1], "TEST", "#", "#")
            this_logger.debug("appended new processing copy of {}".format(item.split(".")[-1]))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def spatial_join(in_fc_1, in_fc_2, out_fc):

    try:
        arcpy.env.overwriteOutput = True
        arcpy.SpatialJoin_analysis(in_fc_1, in_fc_2, out_fc, "JOIN_ONE_TO_ONE", "KEEP_ALL", "#", "INTERSECT")
        this_logger.debug("completed spatial join of {} with {}".format(str(in_fc_1).split('\\')[-1], str(in_fc_2).split('\\')[-1]))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def city_build_init_dict(created_punch_time, last_time_checked_date, creator_list, creator_value, creator_dict, subtype, record_info, surrogate=False, edit_priv="", edit_priv_value=""):

    try:
        if not surrogate:
            if (created_punch_time > last_time_checked_date) and (creator_value in creator_list):
                creator_dict.setdefault(subtype, []).append(str(record_info))
        elif surrogate:
            if (created_punch_time > last_time_checked_date) and (creator_value in creator_list) and (edit_priv_value == edit_priv):
                creator_dict.setdefault(subtype, []).append(str(record_info))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def parks_build_init_dict(created_punch_time, last_time_checked_date, creator_list, creator_value, creator_dict, subtype, record_info, muni, muni_value, surrogate=False, edit_priv="", edit_priv_value=""):

    try:
        if not surrogate:
            if (created_punch_time > last_time_checked_date) and (creator_value in creator_list) and (muni_value == muni):
                creator_dict.setdefault(subtype, []).append(str(record_info))
        elif surrogate:
            if (created_punch_time > last_time_checked_date) and (creator_value in creator_list) and (edit_priv_value == edit_priv):
                creator_dict.setdefault(subtype, []).append(str(record_info))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def city_build_review_list(edited_punch_time, last_time_checked_date, editor_list, editor_value, edit_priv, edit_priv_value, edit_status_value, review_list, record_info, comment_value):
    try:
        if (edited_punch_time > last_time_checked_date) and (editor_value in editor_list) and (edit_priv in edit_priv_value):
            if edit_status_value == "Approved":
                review_list.append(str(record_info) + ', status: ' + edit_status_value)
            elif edit_status_value == "Denied":
                review_list.append(str(record_info) + ', status: ' + edit_status_value + ', comment: ' + comment_value)
    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def parks_build_review_list(edited_punch_time, last_time_checked_date, editor_list, editor_value, edit_priv, edit_priv_value, muni, muni_value, edit_status_value, review_list, record_info, comment_value):
    try:
        if (edited_punch_time > last_time_checked_date) and (editor_value in editor_list) and (edit_priv in edit_priv_value) and (muni_value == muni):
            if edit_status_value == "Approved":
                review_list.append(str(record_info) + ' was ' + edit_status_value)
            elif edit_status_value == "Denied":
                review_list.append(str(record_info) + ' was ' + edit_status_value + ', with comment: ' + comment_value)
    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def city_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, editor_list, editor_value, editor_dict, subtype, record_info):

    try:
        if (edited_punch_time > last_time_checked_date) and (created_punch_time != edited_punch_time) and (editor_value in editor_list):
            editor_dict.setdefault(subtype, []).append(str(record_info))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, editor_list, editor_value, muni, muni_value, editor_dict, subtype, record_info):

    try:
        if (edited_punch_time > last_time_checked_date) and (created_punch_time != edited_punch_time) and (editor_value in editor_list) and (muni_value == muni):
            editor_dict.setdefault(subtype, []).append(str(record_info))

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def email_submissions(dict, org_name, category, recipients, phase="New", surrogate=False):

    try:
        # total_points = sum(len(v) for v in dict.items())
        total_points = 0
        for key, value in dict.items():
            total_points += len(value)
        if not surrogate:
            subject = "{} {} address {} for your review".format(str(phase.capitalize()), org_name, str(category))
            email_text = "{} has submitted {} {} address {}.\n".format(org_name, str(total_points), str(phase), str(category))
        if surrogate:
            subject = "Public Safety submitted {} address {}".format(org_name, str(category))
            email_text = "Public Safety has submitted {} new address {} on your behalf.\n".format(str(total_points), str(category))
        email_text = ""
        for key, value in dict.items():
            num_entries = len(value)
            email_text += "\n" + str(num_entries) + " " + str(key) + "-subtype address " + category + ":\n\n" + "\n".join(item for item in value) + "\n"
            #print email
        send_email(subject, email_text, recipients)

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def email_reviews(org_list, category, recipients):

    try:
        total_points = len(org_list)
        subject = "Address {} reviewed".format(str(category))
        if total_points > 1:
            email_text = "Public Safety has reviewed or edited the following " + str(total_points) + " address " + str(category) + ":\n\n" + "\n".join(item for item in org_list)
        elif total_points == 1:
            email_text = "Public Safety has reviewed or edited the following " + str(total_points) + " address " + str(category[:-1]) + ":\n\n" + "\n".join(item for item in org_list)
        send_email(subject, email_text, recipients)

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

# function iterates through address points, building dicts and lists and calling email functions accordingly
def iterate_through_dataset_parks_offline(in_table, fields, last_time_checked_date, category):

    global westfield_recipients, noblesville_recipients, fishers_recipients, carmel_recipients, hamco_recipients, parks_recipients

    try:
        # fields by index: [0"NGUID", 1"EDIT_PRIV", 2"EDIT_STATUS", 3"created_user", 4"created_date", 5"SUBTYPE", 6"LOC_NO", 7"LOC_NO_SUF",
        # 8"LOC_PR_DIR", 9"LOC_ST", 10"LOC_ST_SUF", 11"LOC_DIR", 12"LOC_CITY", 13"LOC_STATE", 14"LOC_ZIP", 15"COMMENTS",
        # 16"last_edited_user", 17"last_edited_date", 18"MUNI"]

        # set all dicts and lists to empty

        parks_init_dict_hamco = {}
        parks_init_dict_westfield = {}
        parks_init_dict_carmel = {}
        parks_init_dict_fishers = {}
        parks_init_dict_noblesville = {}

        parks_review_list_hamco = {}
        parks_review_list_westfield = {}
        parks_review_list_fishers = {}
        parks_review_list_carmel = {}
        parks_review_list_noblesville = {}

        parks_edit_dict_hamco = {}
        parks_edit_dict_westfield = {}
        parks_edit_dict_fishers = {}
        parks_edit_dict_carmel = {}
        parks_edit_dict_noblesville = {}

        with arcpy.da.SearchCursor(in_table, fields) as scursor:
            for row in scursor:
                if str(row[4]) != 'None':

                    # set time variables
                    created_punch_time = row[4]
                    edited_punch_time = row[17]
                    # print("created_punch_time is: " + str(created_punch_time))
                    # print("edited_punch_time is: " + str(edited_punch_time))
                    # print("last_time_checked_date is: " + str(last_time_checked_date))

                    # replace nones in various fields
                    suite = str(row[7])
                    if suite == 'None':
                        suite = ''
                    prefix_direction = str(row[8])
                    if prefix_direction == 'None':
                        prefix_direction = ''
                    street_suffix = str(row[10])
                    if street_suffix == 'None':
                        street_suffix = ''
                    loc_direction = str(row[11])
                    if loc_direction == 'None':
                        loc_direction = ''

                    # build record info into single variable
                    record_info_with_extra_spaces = str(row[0]) + " / " + str(row[6]) + " " + str(
                        prefix_direction) + " " + str(row[9]) + " " + str(street_suffix) + " " + str(
                        loc_direction) + " " + str(suite) + " " + str(row[12]) + " " + str(row[13]) + " " + str(
                        row[14]).replace(" ", "")
                    record_info = re.sub(' +', ' ', record_info_with_extra_spaces)

                    # call functions to build dicts and lists for initial submissions, reviews, and edited resubmissions
                    # parks_build_init_dict(created_punch_time, last_time_checked_date, creator_list, creator_value, creator_dict, subtype, record_info, muni_, muni_value, surrogate=False, edit_priv="", edit_priv_value="")
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_hamco,
                                    str(row[5]), record_info, "None", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_westfield,
                                    str(row[5]), record_info, "WESTFIELD", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_fishers,
                                    str(row[5]), record_info, "FISHERS", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_carmel,
                                    str(row[5]), record_info, "CARMEL", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_noblesville,
                                    str(row[5]), record_info, "NOBLESVILLE", str(row[18]))

                    # parks_build_review_list(edited_punch_time, last_time_checked_date, editor_list, editor_value, edit_priv, edit_priv_value, muni, muni_value, edit_status_value, review_list, record_info, comment_value)
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "None", str(row[18]), str(row[2]), parks_review_list_hamco, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "WESTFIELD", str(row[18]), str(row[2]), parks_review_list_westfield, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "FISHERS", str(row[18]), str(row[2]), parks_review_list_fishers, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "CARMEL", str(row[18]), str(row[2]), parks_review_list_carmel, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "NOBLESVILLE", str(row[18]), str(row[2]), parks_review_list_noblesville, record_info, str(row[15]))

                    # parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, editor_list, editor_value, muni, muni_value, editor_dict, subtype, record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "None", str(row[18]), parks_edit_dict_hamco, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "WESTFIELD", str(row[18]), parks_edit_dict_westfield, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "FISHERS", str(row[18]), parks_edit_dict_fishers, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "CARMEL", str(row[18]), parks_edit_dict_carmel, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "NOBLESVILLE", str(row[18]), parks_edit_dict_noblesville, str(row[5]), record_info)


        # write dicts and lists to log, then send emails if dicts and lists not empty
        this_logger.debug("new parks {} in unincorporated area to ps: {}".format(str(category), str(parks_init_dict_hamco)))
        this_logger.debug(
            "new parks {} in Westfield to ps: {}".format(str(category), str(parks_init_dict_westfield)))
        this_logger.debug(
            "new parks {} in Fishers area to ps: {}".format(str(category), str(parks_init_dict_fishers)))
        this_logger.debug(
            "new parks {} in Carmel area to ps: {}".format(str(category), str(parks_init_dict_carmel)))
        this_logger.debug(
            "new parks {} in Noblesville area to ps: {}".format(str(category), str(parks_init_dict_noblesville)))

        if len(parks_init_dict_hamco) > 0:
            email_submissions(parks_init_dict_hamco, "County Parks", category, parks_recipients + hamco_recipients)
        if len(parks_init_dict_westfield) > 0:
            email_submissions(parks_init_dict_westfield, "County Parks", category, parks_recipients + westfield_recipients + hamco_recipients)
        if len(parks_init_dict_fishers) > 0:
            email_submissions(parks_init_dict_fishers, "County Parks", category, parks_recipients + fishers_recipients + hamco_recipients)
        if len(parks_init_dict_carmel) > 0:
            email_submissions(parks_init_dict_carmel, "County Parks", category, parks_recipients + carmel_recipients + hamco_recipients)
        if len(parks_init_dict_noblesville) > 0:
            email_submissions(parks_init_dict_noblesville, "County Parks", category, parks_recipients + noblesville_recipients + hamco_recipients)

        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "unincorporated areas", str(parks_review_list_hamco)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "westfield", str(parks_review_list_westfield)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "fishers", str(parks_review_list_fishers)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "carmel", str(parks_review_list_carmel)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "noblesville", str(parks_review_list_noblesville)))

        if len(parks_review_list_hamco) > 0:
            email_reviews(parks_review_list_hamco, category, parks_recipients + hamco_recipients)
        if len(parks_review_list_westfield) > 0:
            email_reviews(parks_review_list_westfield, category, parks_recipients + westfield_recipients + hamco_recipients)
        if len(parks_review_list_fishers) > 0:
            email_reviews(parks_review_list_fishers, category, parks_recipients + fishers_recipients + hamco_recipients)
        if len(parks_review_list_carmel) > 0:
            email_reviews(parks_review_list_carmel, category, parks_recipients + carmel_recipients + hamco_recipients)
        if len(parks_review_list_noblesville) > 0:
            email_reviews(parks_review_list_noblesville, category, parks_recipients + noblesville_recipients + hamco_recipients)

        this_logger.debug("edited parks {} in {}: {}".format(str(category), "unincorporated areas", str(parks_edit_dict_hamco)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "fishers", str(parks_edit_dict_fishers)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "westfield", str(parks_edit_dict_westfield)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "carmel", str(parks_edit_dict_carmel)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "noblesville", str(parks_edit_dict_noblesville)))

        if len(parks_edit_dict_hamco) > 0:
            email_submissions(parks_edit_dict_hamco, "County Parks", category, parks_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_westfield) > 0:
            email_submissions(parks_edit_dict_westfield, "County Parks", category, parks_recipients + westfield_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_fishers) > 0:
            email_submissions(parks_edit_dict_fishers, "County Parks", category, parks_recipients + fishers_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_carmel) > 0:
            email_submissions(parks_edit_dict_carmel, "County Parks", category, parks_recipients + carmel_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_hamco) > 0:
            email_submissions(parks_edit_dict_noblesville, "County Parks", category, parks_recipients + noblesville_recipients + hamco_recipients, phase="edited")

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))


# function iterates through address points, building dicts and lists and calling email functions accordingly
def iterate_through_dataset(in_table, fields, last_time_checked_date, category):

    global westfield_recipients, noblesville_recipients, fishers_recipients, carmel_recipients, hamco_recipients, parks_recipients

    try:
        # fields by index: [0"NGUID", 1"EDIT_PRIV", 2"EDIT_STATUS", 3"created_user", 4"created_date", 5"SUBTYPE", 6"LOC_NO", 7"LOC_NO_SUF",
        # 8"LOC_PR_DIR", 9"LOC_ST", 10"LOC_ST_SUF", 11"LOC_DIR", 12"LOC_CITY", 13"LOC_STATE", 14"LOC_ZIP", 15"COMMENTS",
        # 16"last_edited_user", 17"last_edited_date", 18"MUNI"]

        # set all dicts and lists to empty
        westfield_init_dict = {}
        carmel_init_dict = {}
        fishers_init_dict = {}
        noblesville_init_dict = {}
        hamco_init_dict = {}

        parks_init_dict_hamco = {}
        parks_init_dict_westfield = {}
        parks_init_dict_carmel = {}
        parks_init_dict_fishers = {}
        parks_init_dict_noblesville = {}

        ps_for_westfield_init_dict = {}
        ps_for_carmel_init_dict = {}
        ps_for_fishers_init_dict = {}
        ps_for_noblesville_init_dict = {}

        ps_for_parks_init_dict_hamco = {}
        ps_for_parks_init_dict_westfield = {}
        ps_for_parks_init_dict_fishers = {}
        ps_for_parks_init_dict_carmel = {}
        ps_for_parks_init_dict_noblesville = {}

        westfield_review_list = []
        fishers_review_list = []
        carmel_review_list = []
        noblesville_review_list = []

        parks_review_list_hamco = {}
        parks_review_list_westfield = {}
        parks_review_list_fishers = {}
        parks_review_list_carmel = {}
        parks_review_list_noblesville = {}

        westfield_edit_dict = {}
        carmel_edit_dict = {}
        fishers_edit_dict = {}
        noblesville_edit_dict = {}
        hamco_edit_dict = {}

        parks_edit_dict_hamco = {}
        parks_edit_dict_westfield = {}
        parks_edit_dict_fishers = {}
        parks_edit_dict_carmel = {}
        parks_edit_dict_noblesville = {}

        with arcpy.da.SearchCursor(in_table, fields) as scursor:
            for row in scursor:
                if str(row[4]) != 'None':

                    # set time variables
                    created_punch_time = row[4]
                    edited_punch_time = row[17]
                    # print("created_punch_time is: " + str(created_punch_time))
                    # print("edited_punch_time is: " + str(edited_punch_time))
                    # print("last_time_checked_date is: " + str(last_time_checked_date))

                    # replace nones in various fields
                    suite = str(row[7])
                    if suite == 'None':
                        suite = ''
                    prefix_direction = str(row[8])
                    if prefix_direction == 'None':
                        prefix_direction = ''
                    street_suffix = str(row[10])
                    if street_suffix == 'None':
                        street_suffix = ''
                    loc_direction = str(row[11])
                    if loc_direction == 'None':
                        loc_direction = ''

                    # build record info into single variable
                    record_info_with_extra_spaces = str(row[0]) + " / " + str(row[6]) + " " + str(
                        prefix_direction) + " " + str(row[9]) + " " + str(street_suffix) + " " + str(
                        loc_direction) + " " + str(suite) + " " + str(row[12]) + " " + str(row[13]) + " " + str(
                        row[14]).replace(" ", "")
                    record_info = re.sub(' +', ' ', record_info_with_extra_spaces)

                    # call functions to build dicts and lists for initial submissions, reviews, and edited resubmissions
                    # build_init_dict(created_punch_time, last_time_checked_date, creator_list, creator_value, creator_dict, subtype, record_info, surrogate=False, edit_priv="", edit_priv_value="")
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["westfield_e911", "WESTFIELD_WRITER"], str(row[3]), westfield_init_dict, str(row[5]), record_info)
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["noblesville_e911", "NOBLESVILLE_WRITER"], str(row[3]), noblesville_init_dict,
                               str(row[5]), record_info)
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["carmel_e911", "CARMEL_WRITER"], str(row[3]), carmel_init_dict,
                               str(row[5]), record_info)
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["fishers_e911", "FISHERS3", "FISHERS_WRITER"], str(row[3]), fishers_init_dict,
                               str(row[5]), record_info)

                    # parks_build_init_dict(created_punch_time, last_time_checked_date, creator_list, creator_value, creator_dict, subtype, record_info, muni_, muni_value, surrogate=False, edit_priv="", edit_priv_value="")
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_hamco,
                                    str(row[5]), record_info, "None", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_westfield,
                                    str(row[5]), record_info, "WESTFIELD", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_fishers,
                                    str(row[5]), record_info, "FISHERS", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_carmel,
                                    str(row[5]), record_info, "CARMEL", str(row[18]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date,
                                    ["hamparks"], str(row[3]), parks_init_dict_noblesville,
                                    str(row[5]), record_info, "NOBLESVILLE", str(row[18]))

                    city_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), hamco_init_dict,
                               str(row[5]), record_info, surrogate=True, edit_priv="HAM_911", edit_priv_value=str(row[1]))                    # change from SA to new 911 gis person when they arrive
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_noblesville_init_dict,
                               str(row[5]), record_info, surrogate=True, edit_priv="NOB_911", edit_priv_value=str(row[1]))                    # change from SA to new 911 gis person when they arrive
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_carmel_init_dict,
                               str(row[5]), record_info, surrogate=True, edit_priv="CAR_911", edit_priv_value=str(row[1]))                    # change from SA to new 911 gis person when they arrive
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_fishers_init_dict,
                               str(row[5]), record_info, surrogate=True, edit_priv="FIS_911", edit_priv_value=str(row[1]))                    # change from SA to new 911 gis person when they arrive
                    city_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_westfield_init_dict,
                               str(row[5]), record_info, surrogate=True, edit_priv="WES_911", edit_priv_value=str(row[1]))                    # change from SA to new 911 gis person when they arrive

                    # parks_build_init_dict(created_punch_time, last_time_checked_date, creator_list, creator_value, creator_dict, subtype, record_info, muni_, muni_value, surrogate=False, edit_priv="", edit_priv_value="")
                    parks_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_parks_init_dict_hamco,
                               str(row[5]), record_info, "None", str(row[18]), surrogate=True, edit_priv="PAR_911", edit_priv_value=str(row[1]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_parks_init_dict_westfield,
                               str(row[5]), record_info, "WESTFIELD", str(row[18]), surrogate=True, edit_priv="PAR_911", edit_priv_value=str(row[1]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_parks_init_dict_carmel,
                               str(row[5]), record_info, "CARMEL", str(row[18]), surrogate=True, edit_priv="PAR_911", edit_priv_value=str(row[1]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_parks_init_dict_fishers,
                               str(row[5]), record_info, "FISHERS", str(row[18]), surrogate=True, edit_priv="PAR_911", edit_priv_value=str(row[1]))
                    parks_build_init_dict(created_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[3]), ps_for_parks_init_dict_noblesville,
                               str(row[5]), record_info, "NOBLESVILLE", str(row[18]), surrogate=True, edit_priv="PAR_911", edit_priv_value=str(row[1]))

                    # build_review_list(edited_punch_time, last_time_checked_date, editor_list, editor_value, edit_priv, edit_priv_value, edit_status_value, review_list, record_info, comment_value)
                    city_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "WES_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), str(row[2]), westfield_review_list, record_info, str(row[15]))
                    city_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]),                          # change from SA to new 911 gis person when they arrive
                                      "NOB_911", str(row[1]), str(row[2]), noblesville_review_list, record_info, str(row[15]))
                    city_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]),                      # change from SA to new 911 gis person when they arrive
                                      "CAR_911", str(row[1]), str(row[2]), carmel_review_list,
                                      record_info, str(row[15]))
                    city_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]),              # change from SA to new 911 gis person when they arrive
                                      "FIS_911", str(row[1]), str(row[2]), fishers_review_list,
                                      record_info, str(row[15]))

                    # parks_build_review_list(edited_punch_time, last_time_checked_date, editor_list, editor_value, edit_priv, edit_priv_value, muni, muni_value, edit_status_value, review_list, record_info, comment_value)
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "None", str(row[18]), str(row[2]), parks_review_list_hamco, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "WESTFIELD", str(row[18]), str(row[2]), parks_review_list_westfield, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "FISHERS", str(row[18]), str(row[2]), parks_review_list_fishers, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "CARMEL", str(row[18]), str(row[2]), parks_review_list_carmel, record_info, str(row[15]))
                    parks_build_review_list(edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), "PAR_911",             # change from SA to new 911 gis person when they arrive
                                      str(row[1]), "NOBLESVILLE", str(row[18]), str(row[2]), parks_review_list_noblesville, record_info, str(row[15]))

                    # build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, editor_list, editor_value, editor_dict, subtype, record_info)
                    city_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, ["westfield_e911", "WESTFIELD_WRITER"],
                                    str(row[16]), westfield_edit_dict, str(row[5]), record_info)
                    city_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, ["noblesville_e911", "NOBLESVILLE_WRITER"],
                                    str(row[16]), noblesville_edit_dict, str(row[5]), record_info)
                    city_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, ["carmel_e911", "CARMEL_WRITER"],
                                    str(row[16]), carmel_edit_dict, str(row[5]), record_info)
                    city_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, ["fishers_e911", "FISHERS3", "FISHERS_WRITER"],
                                    str(row[16]), fishers_edit_dict, str(row[5]), record_info)
                    city_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, ["MBO", "mboggs"], str(row[16]), hamco_edit_dict, str(row[5]), record_info)

                    # parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date, editor_list, editor_value, muni, muni_value, editor_dict, subtype, record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "None", str(row[18]), parks_edit_dict_hamco, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "WESTFIELD", str(row[18]), parks_edit_dict_westfield, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "FISHERS", str(row[18]), parks_edit_dict_fishers, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "CARMEL", str(row[18]), parks_edit_dict_carmel, str(row[5]), record_info)
                    parks_build_edit_dict(created_punch_time, edited_punch_time, last_time_checked_date,
                                         ["hamparks"], str(row[16]), "NOBLESVILLE", str(row[18]), parks_edit_dict_noblesville, str(row[5]), record_info)


        # write dicts and lists to log, then send emails if dicts and lists not empty
        this_logger.debug("new westfield {} to ps: {}".format(str(category), str(westfield_init_dict)))
        this_logger.debug("new carmel {} to ps: {}".format(str(category), str(carmel_init_dict)))
        this_logger.debug("new fishers {} to ps: {}".format(str(category), str(fishers_init_dict)))
        this_logger.debug("new noblesville {} to ps: {}".format(str(category), str(noblesville_init_dict)))
        this_logger.debug("new hamco {}: {}".format(str(category), str(hamco_init_dict)))
        this_logger.debug("new parks {} in unincorporated area to ps: {}".format(str(category), str(parks_init_dict_hamco)))
        this_logger.debug(
            "new parks {} in Westfield to ps: {}".format(str(category), str(parks_init_dict_westfield)))
        this_logger.debug(
            "new parks {} in Fishers area to ps: {}".format(str(category), str(parks_init_dict_fishers)))
        this_logger.debug(
            "new parks {} in Carmel area to ps: {}".format(str(category), str(parks_init_dict_carmel)))
        this_logger.debug(
            "new parks {} in Noblesville area to ps: {}".format(str(category), str(parks_init_dict_noblesville)))


        this_logger.debug("new westfield {} submitted by ps: {}".format(str(category), str(ps_for_westfield_init_dict)))
        this_logger.debug("new carmel {} submitted by ps: {}".format(str(category), str(ps_for_carmel_init_dict)))
        this_logger.debug("new fishers {} submitted by ps: {}".format(str(category), str(ps_for_fishers_init_dict)))
        this_logger.debug("new noblesville {} submitted by ps: {}".format(str(category), str(ps_for_noblesville_init_dict)))

        this_logger.debug(
            "new parks {} submitted by ps in unincorporated area: {}".format(str(category), str(ps_for_parks_init_dict_hamco)))
        this_logger.debug(
            "new parks {} submitted by ps in Westfield: {}".format(str(category), str(ps_for_parks_init_dict_westfield)))
        this_logger.debug(
            "new parks {} submitted by ps in Fishers: {}".format(str(category), str(ps_for_parks_init_dict_fishers)))
        this_logger.debug(
            "new parks {} submitted by ps in Carmel: {}".format(str(category), str(ps_for_parks_init_dict_carmel)))
        this_logger.debug(
            "new parks {} submitted by ps in Noblesville: {}".format(str(category), str(ps_for_parks_init_dict_noblesville)))

        if len(westfield_init_dict) > 0:
            email_submissions(westfield_init_dict, "Westfield", category, westfield_recipients + hamco_recipients)
        if len(carmel_init_dict) > 0:
            email_submissions(carmel_init_dict, "Carmel", category, carmel_recipients + hamco_recipients)
        if len(fishers_init_dict) > 0:
            email_submissions(fishers_init_dict, "Fishers", category, fishers_recipients + hamco_recipients)
        if len(noblesville_init_dict) > 0:
            email_submissions(noblesville_init_dict, "Noblesville", category, noblesville_recipients + hamco_recipients)
        if len(hamco_init_dict) > 0:
            email_submissions(hamco_init_dict, "HamCo", category, hamco_recipients)

        if len(parks_init_dict_hamco) > 0:
            email_submissions(parks_init_dict_hamco, "County Parks", category, parks_recipients + hamco_recipients)
        if len(parks_init_dict_westfield) > 0:
            email_submissions(parks_init_dict_westfield, "County Parks", category, parks_recipients + westfield_recipients + hamco_recipients)
        if len(parks_init_dict_fishers) > 0:
            email_submissions(parks_init_dict_fishers, "County Parks", category, parks_recipients + fishers_recipients + hamco_recipients)
        if len(parks_init_dict_carmel) > 0:
            email_submissions(parks_init_dict_carmel, "County Parks", category, parks_recipients + carmel_recipients + hamco_recipients)
        if len(parks_init_dict_noblesville) > 0:
            email_submissions(parks_init_dict_noblesville, "County Parks", category, parks_recipients + noblesville_recipients + hamco_recipients)


        if len(ps_for_noblesville_init_dict) > 0:
            email_submissions(ps_for_noblesville_init_dict, "Noblesville", category, noblesville_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_fishers_init_dict) > 0:
            email_submissions(ps_for_fishers_init_dict, "Fishers", category, fishers_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_westfield_init_dict) > 0:
            email_submissions(ps_for_westfield_init_dict, "Westfield", category, westfield_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_carmel_init_dict) > 0:
            email_submissions(ps_for_carmel_init_dict, "Carmel", category, carmel_recipients + hamco_recipients, phase="new", surrogate=True)

        if len(ps_for_parks_init_dict_hamco) > 0:
            email_submissions(ps_for_parks_init_dict_hamco, "Public Safety", category, parks_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_parks_init_dict_westfield) > 0:
            email_submissions(ps_for_parks_init_dict_westfield, "Public Safety", category, parks_recipients + westfield_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_parks_init_dict_fishers) > 0:
            email_submissions(ps_for_parks_init_dict_fishers, "Public Safety", category, parks_recipients + fishers_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_parks_init_dict_carmel) > 0:
            email_submissions(ps_for_parks_init_dict_carmel, "Public Safety", category, parks_recipients + carmel_recipients + hamco_recipients, phase="new", surrogate=True)
        if len(ps_for_parks_init_dict_noblesville) > 0:
            email_submissions(ps_for_parks_init_dict_noblesville, "Public Safety", category, parks_recipients + noblesville_recipients + hamco_recipients, phase="new", surrogate=True)

        this_logger.debug("{} {} reviewed or edited by ps: {}".format("westfield", str(category), str(westfield_review_list)))
        this_logger.debug("{} {} reviewed or edited by ps: {}".format("carmel", str(category), str(carmel_review_list)))
        this_logger.debug("{} {} reviewed or edited by ps: {}".format("fishers", str(category), str(fishers_review_list)))
        this_logger.debug("{} {} reviewed or edited by ps: {}".format("noblesville", str(category), str(noblesville_review_list)))

        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "unincorporated areas", str(parks_review_list_hamco)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "westfield", str(parks_review_list_westfield)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "fishers", str(parks_review_list_fishers)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "carmel", str(parks_review_list_carmel)))
        this_logger.debug(
            "{} {} in {} reviewed or edited by ps: {}".format("parks", str(category), "noblesville", str(parks_review_list_noblesville)))

        if len(westfield_review_list) > 0:
            email_reviews(westfield_review_list, category, westfield_recipients + hamco_recipients)
        if len(fishers_review_list) > 0:
            email_reviews(fishers_review_list, category, fishers_recipients + hamco_recipients)
        if len(carmel_review_list) > 0:
            email_reviews(carmel_review_list, category, carmel_recipients + hamco_recipients)
        if len(noblesville_review_list) > 0:
            email_reviews(noblesville_review_list, category, noblesville_recipients + hamco_recipients)

        if len(parks_review_list_hamco) > 0:
            email_reviews(parks_review_list_hamco, category, parks_recipients + hamco_recipients)
        if len(parks_review_list_westfield) > 0:
            email_reviews(parks_review_list_westfield, category, parks_recipients + westfield_recipients + hamco_recipients)
        if len(parks_review_list_fishers) > 0:
            email_reviews(parks_review_list_fishers, category, parks_recipients + fishers_recipients + hamco_recipients)
        if len(parks_review_list_carmel) > 0:
            email_reviews(parks_review_list_carmel, category, parks_recipients + carmel_recipients + hamco_recipients)
        if len(parks_review_list_noblesville) > 0:
            email_reviews(parks_review_list_noblesville, category, parks_recipients + noblesville_recipients + hamco_recipients)

        this_logger.debug("edited westfield {} to ps: {}".format(str(category), str(westfield_edit_dict)))
        this_logger.debug("edited carmel {} to ps: {}".format(str(category), str(carmel_edit_dict)))
        this_logger.debug("edited fishers {} to ps: {}".format(str(category), str(fishers_edit_dict)))
        this_logger.debug("edited noblesville {} to ps: {}".format(str(category), str(noblesville_edit_dict)))
        this_logger.debug("edited hamco {}: {}".format(str(category), str(hamco_edit_dict)))

        this_logger.debug("edited parks {} in {}: {}".format(str(category), "unincorporated areas", str(parks_edit_dict_hamco)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "fishers", str(parks_edit_dict_fishers)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "westfield", str(parks_edit_dict_westfield)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "carmel", str(parks_edit_dict_carmel)))
        this_logger.debug(
            "edited parks {} in {}: {}".format(str(category), "noblesville", str(parks_edit_dict_noblesville)))

        if len(westfield_edit_dict) > 0:
            email_submissions(westfield_edit_dict, "Westfield", category, westfield_recipients + hamco_recipients, phase="edited")
        if len(carmel_edit_dict) > 0:
            email_submissions(carmel_edit_dict, "Carmel", category, carmel_recipients + hamco_recipients, phase="edited")
        if len(fishers_edit_dict) > 0:
            email_submissions(fishers_edit_dict, "Fishers", category, fishers_recipients + hamco_recipients, phase="edited")
        if len(noblesville_edit_dict) > 0:
            email_submissions(noblesville_edit_dict, "Noblesville", category, noblesville_recipients + hamco_recipients, phase="edited")
        # if len(hamco_edit_dict) > 0:
        #     email_submissions(hamco_edit_dict, "HamCo", category, hamco_recipients, phase="edited")

        if len(parks_edit_dict_hamco) > 0:
            email_submissions(parks_edit_dict_hamco, "County Parks", category, parks_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_westfield) > 0:
            email_submissions(parks_edit_dict_westfield, "County Parks", category, parks_recipients + westfield_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_fishers) > 0:
            email_submissions(parks_edit_dict_fishers, "County Parks", category, parks_recipients + fishers_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_carmel) > 0:
            email_submissions(parks_edit_dict_carmel, "County Parks", category, parks_recipients + carmel_recipients + hamco_recipients, phase="edited")
        if len(parks_edit_dict_hamco) > 0:
            email_submissions(parks_edit_dict_noblesville, "County Parks", category, parks_recipients + noblesville_recipients + hamco_recipients, phase="edited")

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

def send_email(subject, msg_html, recipients, internal=False):

    try:
        if internal:
            if "ERROR" in str(msg_html):
                subject += " - ERROR"
            else:
                subject += " - SUCCESS"
        sender = "No-Reply@hamiltoncounty.in.gov"
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['To'] = ';'.join(recipients)
        msg['From'] = sender
        # create the tags for the HTML version of the message so it has a fixed width font
        prefix_html = '''\
        <html>
        <head></head>
        <body>
          <p style="font-family:'Lucida Console', Monaco, monospace;font-size:12px">
        '''
        suffix_html = '''\
          </p>
        </body>
        </html>
        '''
        # replace spaces with non-breaking spaces (otherwise, multiple spaces are truncated)
        msg_html = msg_html.replace(' ', '&nbsp;')
        # replace new lines with <br> tags and add the HTML tags before and after the message
        msg_html = prefix_html + msg_html.replace('\n', '<br>') + suffix_html

        # # Record the MIME types of both parts - text/plain and text/html.
        #part1 = MIMEText(msgPlain, 'plain')
        part2 = MIMEText(msg_html, 'html')

        # Add both forms of the message
        #msg.attach(part1)
        msg.attach(part2)

        # Connect to exchange and send email
        conn = smtplib.SMTP('10.200.10.105')
        conn.ehlo()
        conn.starttls()
        conn.ehlo()
        conn.sendmail(sender, recipients, msg.as_string())
        conn.close()

    except Exception as e:
        tb = sys.exc_info()[2]
        this_logger.debug("ERROR @ Line %i" % tb.tb_lineno + ". {}".format(str(e.args[0])))

if __name__ == '__main__':
    main()

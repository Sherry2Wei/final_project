import os
from dfply import *
import re
from multiprocessing import Pool
import time
import subprocess as sub
from functools import partial
import pandas as pd


def convert_pdf2text(pdffile, pdf2text_path, txt_output_path):
    """
    This function use the sub processing which call a exe which is not a python package run in the python document
    and through the pdf2text.exe, this function convert the pdf into text and save it to the required location
    :param pdffile:
    :param pdf2text_path:
    :param txt_output_path:
    :return:
    """
    sub.call([pdf2text_path + "\\pdftotext", pdffile, txt_output_path + "\\" + pdffile[:-4] + ".txt"])


def extract(document):
    """
    This function will extract the Company info such as company name, sec code, business addression
    direct and indirect executive as well as thier civil, regulatory, arbitration detail
    :param document:
    :return: to a dictory which key is the name of the filed and value to be the corrsponding answer of that key
    """
    raw = open(document, "r")
    raw = raw.read()
    clean_doc = re.split("\n+", raw)
    company_name = clean_doc[1]
    # go to the summary page
    try:
        summary_page = clean_doc[clean_doc.index("Report Summary for this Firm"):
                                 clean_doc.index("Firm Names and Locations") + 1]
    except ValueError:
        summary_page = clean_doc[clean_doc.index("Main Office Location"):
                                 clean_doc.index("Firm Names and Locations") + 1]
    # go to the disclosures section
    civil = np.nan
    arbitration = np.nan
    regulatory_event = np.nan
    no_regulatory = False
    try:
        for item in (summary_page[summary_page.index("Count"):
                     summary_page.index("Is this brokerage firm currently suspended with any")]):
            if item == "Civil Event":
                civil = summary_page[summary_page.index(item) + 1]
            elif item == "Regulatory Event":
                regulatory_event = summary_page[summary_page.index(item) + 1]
            elif item == "Arbitration":
                arbitration = summary_page[summary_page.index(item) + 1]
    except ValueError:
        no_regulatory = True
    suspend = re.split("[?]",
                       summary_page[summary_page.index("Is this brokerage firm currently suspended with any") + 1])[-1]
    firm_profile = " ".join(summary_page[summary_page.index("User Guidance"):
                                         summary_page.index("Firm Names and Locations")][2:])
    firm_operation = " ".join(summary_page[summary_page.index("Is this brokerage firm currently suspended with any") + 2
                                           :[index for index, item in enumerate(summary_page)
                                           if "All rights reserved." in item][0]])
    try:
        register_info = ", ".join([item for item in summary_page[
                                                  summary_page.index("Investment Adviser Public Disclosure website at:") + 2
                                                  : summary_page.index("Disclosure Events")] if item != "â€¢"])
    except ValueError:
        try:
            register_info = ", ".join([item for item in summary_page[summary_page.index("Regulatory Event") + 2:
                                                                     summary_page.index("Is this brokerage firm "
                                                                                        "currently suspended with any")]
                                       ])
        except ValueError:
            register_info = ", ".join([item for item in summary_page[
                                                        summary_page.index("This firm is registered with:") + 1
                                                        : summary_page.index("Is this brokerage firm currently "
                                                                             "suspended with any")] if item != "â€¢"])
    # go the firm location page:
    location_page = clean_doc[clean_doc.index("Firm Names and Locations"):]
    try:
        location_page = location_page[: location_page.index("2") - 1]
    except ValueError:
        location_page = location_page[: location_page.index("3") - 1]
    main_address = np.nan
    main_city = np.nan
    main_state = np.nan
    main_zip = np.nan
    mailing_address = np.nan
    mailing_city = np.nan
    mailing_state = np.nan
    mailing_zip = np.nan
    sec_code = np.nan
    crd_code = np.nan
    phone_number = np.nan
    for index, item in enumerate(location_page):
        if "CRD" in item:
            crd_code = re.split("#", item)[-1]
        elif "SEC" in item:
            sec_code = location_page[index + 1]
        elif "Main Office Location" in item:
            main_address = location_page[index + 1]
            main_city = re.split("[,]", location_page[index + 2])[0]
            main_state = re.split(r"[,|\s]", location_page[index + 2])[-2]
            main_zip = re.split(r"[,|\s]", location_page[index + 2])[-1]
        elif "Mailing Address" in item:
            mailing_address = ", ".join(location_page[index + 1: index + 3])
            mailing_city = re.split(",", location_page[index + 3])[0]
            mailing_state = re.split(r"[,|\s]", location_page[index + 3])[-2]
            mailing_zip = re.split(r"[,|\s]", location_page[index + 3])[-1]
        elif "Business Telephone Number" in item:
            phone_number = location_page[index + 1]
    # go to the direct owner page
    owner_page = clean_doc[clean_doc.index("Direct Owners and Executive Officers"): clean_doc.index("Indirect Owners")]
    irrelvent_content = ["Report about", "All rights", "reserved.", "www.finra.org"]
    # fiter out content that have word in the irrelvent_content list
    owner_page = [content for content in owner_page if all([word not in content for word in irrelvent_content])]
    # get the index of begining of each owner or executive so we can cut up the list content down to indivdual
    owner_index = [index for index, item in enumerate(owner_page) if item == "Legal Name & CRD# (if any):"]
    owner_info = []
    dname = np.nan
    crd = np.nan
    status = np.nan
    position = np.nan
    position_date = np.nan
    ownership = np.nan
    direct = np.nan
    for index, item in enumerate(owner_index):
        if item != owner_index[-1]:
            try:
                section = owner_page[item: owner_index[index + 1]]
                dname = section[section.index("Legal Name & CRD# (if any):") + 1]
                position = section[section.index("Position") + 1]
                position_date = section[section.index("Position Start Date") + 1]
                ownership = section[section.index("Percentage of Ownership") + 1]
                if section[section.index("entity or an individual?") - 1] == "Is this a domestic or foreign":
                    status = section[section.index("entity or an individual?") + 1]
                else:
                    new_section = section[section.index("entity or an individual?") + 1:]
                    status = new_section[new_section.index("entity or an individual?") + 1]
                if section[section.index("the firm?") - 1] == "management or policies of":
                    direct = section[section.index("the firm?") + 1]
                else:
                    new_section = section[section.index("the firm?") + 1:]
                    direct = new_section[new_section.index("management or policies of") + 1]
                if index == 0:
                    crd = np.nan
                else:
                    crd = section[section.index("Legal Name & CRD# (if any):") + 2]
                if section[section.index("company?") - 1] == "Is this a public reporting":
                    public = section[section.index("company?") + 1]
                else:
                    new_section = section[section.index("company?") + 1:]
                    public = new_section[new_section.index("company?") + 1]
            except IndexError:
                print(document[:-4] + " has a empty spot on the public report question " + "on the " + str(
                    index + 1) + " executive")
                public = np.nan
        else:
            try:
                section = owner_page[item: -1]
                dname = section[section.index("Legal Name & CRD# (if any):") + 1]
                position = section[section.index("Position") + 1]
                position_date = section[section.index("Position Start Date") + 1]
                ownership = section[section.index("Percentage of Ownership") + 1]
                if section[section.index("entity or an individual?") - 1] == "Is this a domestic or foreign":
                    status = section[section.index("entity or an individual?") + 1]
                else:
                    new_section = section[section.index("entity or an individual?") + 1:]
                    status = new_section[new_section.index("entity or an individual?") + 1]
                if section[section.index("the firm?") - 1] == "management or policies of":
                    direct = section[section.index("the firm?") + 1]
                else:
                    new_section = section[section.index("the firm?") + 1:]
                    direct = new_section[new_section.index("management or policies of") + 1]
                crd = section[section.index("Legal Name & CRD# (if any):") + 2]
                if section[section.index("company?") - 1] == "Is this a public reporting":
                    public = section[section.index("company?") + 1]
                else:
                    new_section = section[section.index("company?") + 1:]
                    public = new_section[new_section.index("company?") + 1]
            except IndexError:
                print(document[:-4] + " has a empty spot on the public report question " + "on the " + str(
                    index + 1) + " executive")
                public = np.nan
        owner_info.append([dname, crd, status, position, position_date, ownership, direct, public])

    # go to the indirect owner page:
    indirect_owner_page = clean_doc[clean_doc.index("Indirect Owners"):
                                    [index for index, item in enumerate(clean_doc) if item == "Firm History"][-1]]
    # fiter out irrelvent content of the indirect owner page
    indirect_owner_page = [content for content in indirect_owner_page
                           if all([word not in content for word in irrelvent_content])]
    indirect_owner_index = [index for index, item in enumerate(indirect_owner_page)
                            if item == "Legal Name & CRD# (if any):"]
    indirect_owner_info = []
    for index, item in enumerate(indirect_owner_index):
        if item != indirect_owner_index[-1]:
            section = indirect_owner_page[item: indirect_owner_index[index + 1]]
            indirect_name = section[section.index("Legal Name & CRD# (if any):") + 1]
            status = section[section.index("entity or an individual?") + 1]
            establish_by = section[section.index("established") + 1]
            relationship = section[section.index("Relationship to Direct Owner") + 1]
            relationship_date = section[section.index("Relationship Established") + 1]
            ownership = section[section.index("Percentage of Ownership") + 1]
            direct = section[section.index("the firm?") + 1]
            public = section[section.index("company?") + 1]
        else:
            section = indirect_owner_page[item: -1]
            indirect_name = section[section.index("Legal Name & CRD# (if any):") + 1]
            status = section[section.index("entity or an individual?") + 1]
            establish_by = section[section.index("established") + 1]
            relationship = section[section.index("Relationship to Direct Owner") + 1]
            relationship_date = section[section.index("Relationship Established") + 1]
            ownership = section[section.index("Percentage of Ownership") + 1]
            direct = section[section.index("the firm?") + 1]
            public = section[section.index("company?") + 1]
        indirect_owner_info.append([indirect_name, status, establish_by, relationship,
                                    relationship_date, ownership, direct, public])
    # Regulatory page
    no_civil = False
    no_arbitration = False
    if no_regulatory is False:
        try:
            regulatory_page = clean_doc[clean_doc.index("Regulatory - Final"): clean_doc.index("Civil - Final")]
        except ValueError:
            try:
                no_civil = True
                regulatory_page = clean_doc[clean_doc.index("Regulatory - Final"):
                                            clean_doc.index("Arbitration Award - Award / Judgment")]
            except ValueError:
                no_arbitration = True
                regulatory_page = clean_doc[clean_doc.index("Regulatory - Final"):]
        regulatory_page = [content for content in regulatory_page
                           if all([word not in content for word in irrelvent_content])]
        regulatory_index = [index for index, item in enumerate(regulatory_page) if "Disclosure" in item]
        # remove repeat content in the regulatory_page
        clean_regulatory_page = []
        for index, item in enumerate(regulatory_index):
            if item != regulatory_index[-1]:
                section = regulatory_page[item: regulatory_index[index + 1]]
                if len(section) > 10:
                    clean_regulatory_page.append(section)
            else:
                section = regulatory_page[regulatory_index[-1]:]
                clean_regulatory_page.append(section)
        regulatory_info = []
        for section in clean_regulatory_page:
                source = section[section.index("Reporting Source:") + 1]
                status = section[section.index("Current Status:") + 1]
                if section[section.index("Initiated By:") + 1] == "Date Initiated:":
                    inital_by = section[section.index("User Guidance") + 1]
                    inital_date = section[section.index("User Guidance") + 2]
                else:
                    inital_by = section[section.index("Initiated By:") + 1]
                    inital_date = section[section.index("Date Initiated:") + 1]
                allegation = " ".join(section[section.index("Allegations:") + 1: section.index("Initiated By:")])
                docket = section[section.index("Docket/Case Number:") + 1]
                product_type = section[section.index("Principal Product Type:") + 1]
                resolution = section[section.index("Resolution:") + 1]
                resolution_date = section[section.index("Resolution Date:") + 1]
                try:
                    if section[section.index("Sanctions Ordered:") + 1] == "No" or \
                            section[section.index("Sanctions Ordered:") + 1] == "Yes":
                        sanction_order = " ".join(section[section.index("Sanctions Ordered:") + 2:
                                                          section.index("Other Sanctions Ordered:")])
                    else:
                        sanction_order = " ".join(section[section.index("Sanctions Ordered:") + 1:
                                                          section.index("Other Sanctions Ordered:")])
                except ValueError:
                    sanction_order = np.nan
                regulatory_info.append([source, status, inital_by, inital_date, allegation, docket, product_type,
                                        resolution, resolution_date, sanction_order])
        # comfirm whether there is a civil report or not:
        if no_civil is False:
            try:
                civil_page = clean_doc[clean_doc.index("Civil - Final"):
                                       clean_doc.index("Arbitration Award - Award / Judgment")]
            except ValueError:
                no_arbitration = True
                civil_page = clean_doc[clean_doc.index("Civil - Final"):]
            civil_page = [content for content in civil_page
                          if all([word not in content for word in irrelvent_content])]
            civil_index = [index for index, item in enumerate(civil_page) if "Disclosure" in item]
            clean_civil_page = []
            for index, item in enumerate(civil_index):
                if item != civil_index[-1]:
                    section = civil_page[item: civil_index[index + 1]]
                    if len(section) > 10:
                        clean_civil_page.append(section)
                else:
                    section = civil_page[civil_index[-1]:]
                    clean_civil_page.append(section)
            civil_info = []
            for section in clean_civil_page:
                source = section[section.index("Reporting Source:") + 1]
                status = section[section.index("Current Status:") + 1]
                allegation = " ".join(section[section.index("Allegations:") + 1: section.index("Initiated By:")])
                inital_by = section[section.index("Initiated By:") + 1]
                court_detail = " ".join(section[section.index("Court Details:") + 1:
                                                section.index("Date Court Action Filed:")])
                file_date = section[section.index("Date Court Action Filed:") + 1]
                product_type = section[section.index("Principal Product Type:") + 1]
                if status == "Final":
                    resolution = section[section.index("Resolution:") + 1]
                    resolution_date = section[section.index("Resolution Date:") + 1]
                    sanction_order = ", ".join(section[section.index("Granted:") + 1:
                                                       section.index("Other Sanctions:")])
                else:
                    resolution = np.nan
                    resolution_date = np.nan
                    sanction_order = np.nan
                civil_info.append([source, status, allegation, inital_by, court_detail, file_date, product_type,
                                   resolution, resolution_date, sanction_order])
        else:
            civil_info = np.nan
        # go the Arbitration Award - Award / Judgment section if it is available
        if no_arbitration is False:
            arbitration_page = clean_doc[clean_doc.index("Arbitration Award - Award / Judgment"):]
            arbitration_page = [content for content in arbitration_page
                                if all([word not in content for word in irrelvent_content])]
            arbitration_index = [index for index, item in enumerate(arbitration_page) if "Disclosure" in item]
            # remove repeat content in the regulatory_page
            clean_arbitration_page = []
            for index, item in enumerate(arbitration_index):
                if item != arbitration_index[-1]:
                    section = arbitration_page[item: arbitration_index[index + 1]]
                    if len(section) > 10:
                        clean_arbitration_page.append(section)
                else:
                    section = arbitration_page[item:]
                    clean_arbitration_page.append(section)
            arbitration_info = []
            for section in clean_arbitration_page:
                source = section[section.index("Reporting Source:") + 1]
                event_type = section[section.index("Type of Event:") + 1]
                allegation = " ".join(section[section.index("Allegations:") + 1: section.index("Arbitration Forum:")])
                forum = section[section.index("Arbitration Forum:") + 1]
                inital_date = section[section.index("Case Initiated:") + 1]
                case_number = section[section.index("Case Number:") + 1]
                product_type = section[section.index("Disputed Product Type:") + 1:
                                       section.index("Sum of All Relief Requested:")]
                request = section[section.index("Sum of All Relief Requested:") + 1]
                disposition = section[section.index("Disposition:") + 1]
                disposition_date = section[section.index("Disposition Date:") + 1]
                award = section[section.index("Sum of All Relief Awarded:") + 1]
                arbitration_info.append([source, event_type, allegation, forum, inital_date, case_number, product_type,
                                         request, disposition, disposition_date, award])
        else:
            arbitration_info = np.nan
    else:
        regulatory_info = np.nan
        civil_info = np.nan
        arbitration_info = np.nan
    return {"civil": civil, "arbitration": arbitration, "regulatory_event": regulatory_event, "suspend": suspend,
            "firm_profile": firm_profile, "firm_operation": firm_operation, "register_info": register_info,
            "company_name": company_name, "crd_code": crd_code, "sec_code": sec_code, "main_address": main_address,
            "main_city": main_city, "main_state": main_state, "main_zip": main_zip, "mailing_address": mailing_address,
            "mailing_city": mailing_city, "mailing_state": mailing_state, "mailing_zip": mailing_zip,
            "phone_number": phone_number, "owner": owner_info, "indirect": indirect_owner_info,
            "regulatory_info": regulatory_info, "civil_info": civil_info, "arbitration_info": arbitration_info}


if __name__ == '__main__':
    start_time = time.time()
    # Convert the Pdf file into text file
    os.chdir(r"c:\Users\ChonWai\Desktop\big_data\final\data\pdf")
    output_path = r"c:\Users\ChonWai\Desktop\big_data\final\data\txt"
    # path of the pdftotext exe located
    pdftotext_path = r"c:\Users\ChonWai\Desktop\big_data\final\poppler-0.68.0\bin"
    input_list = os.listdir()
    p = Pool(processes=7)
    p.map(partial(convert_pdf2text, pdf2text_path=pdftotext_path, txt_output_path=output_path), input_list)
    p.close()
    # extracting the text out of the txt file from the output path
    os.chdir(output_path)
    p = Pool(processes=7)
    file_list = os.listdir()
    content_list = list(p.map(extract, file_list))
    p.close()
    company_df = pd.DataFrame({"company_name": [],
                               "crd_code": [],
                               "sec_code": [],
                               "main_address": [],
                               "main_city": [],
                               "main_state": [],
                               "main_zip": [],
                               "mailing_address": [],
                               "mailing_city": [],
                               "mailing_state": [],
                               "mailing_zip": [],
                               "phone_number": [],
                               "firm_profile": [],
                               "firm_operation": [],
                               "register_info": [],
                               "civil": [],
                               "arbitration": [],
                               "regulatory_event": [],
                               "suspend": []})
    owner_df = pd.DataFrame([], columns=["name", "crd", "status", "position", "position_date", "ownership",
                                         "direct", "public"])
    indirect_owner_df = pd.DataFrame([], columns=["name", "status", "establish_by", "relationship",
                                                  "relationship_date", "ownership", "direct", "public"])
    regulatory_df = pd.DataFrame([], columns=["source", "status", "inital_by", "inital_date", "allegation", "docket",
                                              "product_type", "resolution", "resolution_date", "sanction_order"])
    civil_df = pd.DataFrame([], columns=["source", "status", "allegation", "inital_by", "court_detail", "file_date",
                                         "product_type", "resolution", "resolution_date", "sanction_order"])
    arbitration_df = pd.DataFrame([], columns=["source", "event_type", "allegation", "forum", "inital_date",
                                               "case_number", "product_type", "request", "disposition",
                                               "disposition_date", "award"])
    for dictionary in content_list:
        company_df = pd.concat([company_df,
                                pd.DataFrame({"company_name": [dictionary["company_name"]],
                                              "crd_code": [dictionary["crd_code"]],
                                              "sec_code": [dictionary["sec_code"]],
                                              "main_address": [dictionary["main_address"]],
                                              "main_city": [dictionary["main_city"]],
                                              "main_state": [dictionary["main_state"]],
                                              "main_zip": [dictionary["main_zip"]],
                                              "mailing_address": [dictionary["mailing_address"]],
                                              "mailing_city": [dictionary["mailing_city"]],
                                              "mailing_state": [dictionary["mailing_state"]],
                                              "mailing_zip": [dictionary["mailing_zip"]],
                                              "phone_number": [dictionary["phone_number"]],
                                              "firm_profile": [dictionary["firm_profile"]],
                                              "firm_operation": [dictionary["firm_operation"]],
                                              "register_info": [dictionary["register_info"]],
                                              "civil": [dictionary["civil"]],
                                              "arbitration": [dictionary["arbitration"]],
                                              "regulatory_event": [dictionary["regulatory_event"]],
                                              "suspend": [dictionary["suspend"]]})], axis=0)
        owner_df = pd.concat([owner_df, pd.DataFrame(dictionary["owner"],
                                                     columns=["name", "crd", "status", "position", "position_date",
                                                              "ownership", "direct", "public"])], axis=0)
        indirect_owner_df = pd.concat([indirect_owner_df, pd.DataFrame(dictionary["indirect"],
                                                                       columns=["name", "status", "establish_by",
                                                                                "relationship", "relationship_date",
                                                                                "ownership", "direct", "public"])],
                                      axis=0)
        if dictionary["regulatory_info"] != dictionary["regulatory_info"]:
            pass
        else:
            regulatory_df = pd.concat([regulatory_df, pd.DataFrame(dictionary["regulatory_info"],
                                                                   columns=["source", "status", "inital_by", "inital_date",
                                                                            "allegation", "docket", "product_type",
                                                                            "resolution", "resolution_date",
                                                                            "sanction_order"])], axis=0)
        if dictionary["civil_info"] != dictionary["civil_info"]:
            pass
        else:
            civil_df = pd.concat([civil_df, pd.DataFrame(dictionary["civil_info"],
                                                         columns=["source", "status", "allegation", "inital_by",
                                                                  "court_detail", "file_date", "product_type",
                                                                  "resolution", "resolution_date", "sanction_order"])],
                                 axis=0)
        if dictionary["arbitration_info"] != dictionary["arbitration_info"]:
            pass
        else:
            arbitration_df = pd.concat([arbitration_df, pd.DataFrame(dictionary["arbitration_info"],
                                                                     columns=["source", "event_type", "allegation",
                                                                              "forum", "inital_date",
                                                                              "case_number", "product_type", "request",
                                                                              "disposition", "disposition_date",
                                                                              "award"])])
    csv_path = r"c:\Users\ChonWai\Desktop\big_data\final\data"
    company_df.to_csv(csv_path + "\\company.csv")
    owner_df.to_csv(csv_path + "\\owner.csv")
    indirect_owner_df.to_csv(csv_path + "\\indirect.csv")
    regulatory_df.to_csv(csv_path + "\\regulatory.csv")
    civil_df.to_csv(csv_path + "\\civil.csv")
    arbitration_df.to_csv(csv_path + "arbitration.csv")
    print("--- %s seconds ---" % (time.time() - start_time))











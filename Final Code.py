import os
from tika import parser
import re
import numpy as np
from operator import itemgetter
os.chdir(r"c:\Users\ChonWai\Desktop\big_data\final\data\pdf")


def extract(document):
    company_name = ""
    for letter in document[50:]:
        if letter != "\n":
            company_name = company_name + letter
        else:
            break
    crd_code_index = 0
    # go to the firm summary page and extract the basic info of the firm
    for i in range(4):
        crd_code_index = document.find("CRD", crd_code_index)+1
    clean_doc = re.split("[\n|#]+", document[crd_code_index:])
    crd_code = clean_doc[1]
    sec_code = clean_doc[3]
    main_office_address = clean_doc[5]
    mailing_address = clean_doc[9] + " " + clean_doc[10]
    main_address_city = re.split("[,]", clean_doc[6])[0]
    main_address_state = re.split(r"[,|\s]+", clean_doc[6])[-2]
    main_address_zip = re.split(r"[,|\s]+", clean_doc[6])[-1]
    mailing_city = re.split("[,]", clean_doc[11])[0]
    mailing_state = re.split(r"[,|\s]+", clean_doc[11])[-2]
    mailing_zip = re.split(r"[,|\s]+", clean_doc[11])[-1]
    phone_number = clean_doc[17]
    civil = np.nan
    arbitration= np.nan
    regulatory_event = np.nan
    suspend = re.split('[?]',
                       clean_doc[clean_doc.index(
                           "Is this brokerage firm currently suspended with any") + 1])[-1].strip()
    firm_profile = " ".join(clean_doc[clean_doc.index("Firm Profile") + 1: clean_doc.index("Firm History")])
    firm_operation = " ".join(clean_doc[clean_doc.index("Firm Operations") + 3:
                                        clean_doc.index("This firm is registered with:")])
    register_info = " ".join(clean_doc[clean_doc.index("This firm is registered with:") + 1:
                             clean_doc.index("www.finra.org/brokercheck User Guidance")])
    for item in clean_doc[clean_doc.index("Type Count") + 1: clean_doc.index("Type Count") + 4]:
        if "Civil" in item:
            civil = int(re.split(r"\s+", item)[-1])
        elif "Regulatory" in item:
            regulatory_event = int(re.split(r"\s+", item)[-1])
        elif "Arbitration" in item:
            arbitration = int(re.split(r"\s+", item)[-1])

    # extract the office Manager info and direct owner
    owner_page = clean_doc[clean_doc.index("Direct Owners and Executive Officers"):
                           clean_doc.index("Indirect Owners")]
    irrevlent_list = ["Position", "Percentage of Ownership", "Is this a public reporting", "company?",
                      "Position Start Date", "Does this owner direct the", "management or policies of", "the firm?",
                      "Is this a domestic or foreign", "entity or an individual?", "Legal Name & CRD", " (if any):",
                      "Report about",
                      "http://www.finra.org/brokercheck",
                      "http://www.finra.org/brokercheck_reports",
                      "http://www.finra.org",
                      "www.finra.org/brokercheck User Guidance",
                      "Direct Owners and Executive Officers (continued)",
                      "Firm Profile"]



    # extract indirect owner info
    indirect_owner_page = clean_doc[clean_doc.index("Indirect Owners"):
                                    [index for index, value in enumerate(clean_doc) if value == "Firm History"][-1]]
    indirect_owner_info = {}
    indirect_owner_index = [0]
    indirect_owner_index.extend([index for index, value in enumerate(indirect_owner_page)
                                 if value == "Is this a public reporting"])

    for index, value in enumerate(indirect_owner_index):
        if value != indirect_owner_index[-1]:
            owner = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][2]
            domestic = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][7]
            indirect_relationship = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][4]
            relationship = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][3]
            establish = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][8]
            percentage = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][5]
            management = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][9]
            report = indirect_owner_page[value: indirect_owner_index[index + 1] + 1][6]
            indirect_owner_info[owner] = [domestic, indirect_relationship, relationship, establish,
                                          percentage, management, report]
    return firm_operation
    """

    return {"company_name": company_name, "crd": crd_code, "sec": sec_code,
            "main_office_address": main_office_address, "main_address_city": main_address_city,
            "main_address_state": main_address_state, "main_address_zip": main_address_zip,
            "mailing_address": mailing_address, "mailing_city": mailing_city,
            "mailing_state": mailing_state, "mailing_zip": mailing_zip, "phone_number": phone_number,
            "civil": civil, "arbitration": arbitration,
            "regulatory_event": regulatory_event, "suspend": suspend,
            "firm_profile": firm_profile, "firm_operation": firm_operation,
            "register_info": register_info}
    """


raw = parser.from_file('firm_79.pdf')
content = raw['content']
print(extract(content))
#print(itemgetter("firm_operation", "firm_profile", "register_info")(extract(content)))



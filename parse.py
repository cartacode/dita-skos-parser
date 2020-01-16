import os
import sys
import re
import logging
import time
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as et
from lxml import etree, html

logging.basicConfig(filename="logs.log")

#Creating an object
logger=logging.getLogger()
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
now = datetime.now()

tags = {
    'topic': {
        'abbr': 't',
        'count': 0,
        'level': 0,
    },
    'section': {
        'abbr': 's',
        'count': 0,
        'level': 1,
    },
    'p': {
        'abbr': 'p',
        'count': 0,
        'level': 2,
    },
    'ph': {
        'abbr': 'ph',
        'count': 0,
        'level': 3,
    },
    'simpletable': {
        'abbr': 'st',
        'count': 0
    },
    'li': {
        'abbr': 'li',
        'count': 0,
    }
}

TAG_LEVEL = ['topic', 'section', 'p', 'ph']

def log(msg):
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    logging.info("{} at {}.\n".format(msg, current_time))

def validate_cell_value(val):
    if str(val) == 'nan':
        return None
    return val

if __name__ == "__main__":
    master_xlsx_path = "/mnt/g/projects/ResearchMeta"
    master_xlsx_filename = "DBA_ResearchMetaData_Python.xlsx"

    # Read data from sheets
    os.chdir(master_xlsx_path)
    fn = pd.ExcelFile(master_xlsx_filename)
    pv = pd.read_excel(fn, sheet_name="PathsAndValues",index_col=None)
    # tb = pd.read_excel(fn, sheet_name='ThesaurusBase', index_col=1)
    # cl = pd.read_excel(fn, sheet_name= 'ConceptList', index_col=2)
    # rm = pd.read_excel(fn, sheet_name='RelationsAndMappings', index_col=None)
    ma = pd.read_excel(fn, sheet_name= "Master_Article", index_col=0)
    # mu = pd.read_excel(fn, sheet_name='Master_Author', index_col=1)
    # ar = pd.read_excel(fn, sheet_name= 'Master_AuthorRole', index_col=1)
    # ca = pd.read_excel(fn, sheet_name= 'Master_CollectionArticle', index_col=None)


    # define variables
    dita_map_path = None
    input_path = None
    output_path = None
    cAuthor = None # author name
    tag_count = 0 # number of tags [topic, section, paragraph]
    figure = 0
    table = 0
    form = 0
    fig_abbr = None
    table_abbr = None
    form_abbr = None
    references = []
    referenceIds = []

    """ Notice: topic.dtd should be changed for each files? i.e 
        ReferencesList.dtd, FigureList.dtd, etc """
    base_str = '<?xml version="1.0" encoding="UTF-8"?>\n\
                <!DOCTYPE topic PUBLIC "-//OASIS//DTD DITA Topic//EN" "topic.dtd">'
    
    # Get values from PathAndValues sheet
    # need to be updated later for dynamic
    try:
        dita_map_path = validate_cell_value(pv["Value"][6])
        input_path = validate_cell_value(pv["Value"][10])
        output_path = validate_cell_value(pv["Value"][11])
        fig_abbr = validate_cell_value(pv["Value"][12])
        table_abbr = validate_cell_value(pv["Value"][13])
        form_abbr = validate_cell_value(pv["Value"][14])
        cAuthor = validate_cell_value(pv["Value"][1])
    except Exception as e:
        print("Error when reading values from PathAndValues")
        log(str(e))
        sys.exit(1)

    if not os.path.exists(input_path):
        print("Input folder doesn't exist!")
        log("{} is not valid path".format(input_path))
        sys.exit(1)

    if not os.path.exists(output_path):
        print("Output folder doesn't exist!")
        log("{} is not valid path".format(output_path))
        sys.exit(1)

    """ parse ditamap """
    dita_files = []
    try:
        tree = et.parse("{}/Thesis.ditamap".format(dita_map_path))
        root = tree.getroot()
        topicrefs = tree.findall(".//topicref")
        for topicref in topicrefs:
            dita_files.append(topicref.attrib['href'])

    except Exception as e:
        print("Error when reading ditamap file: {}".format(str(e)))
        log(str(e))
        sys.exit(1)

    """ read dita files """
    for d_file in dita_files:
        os.chdir(dita_map_path)
        d_file_path = os.path.join(dita_map_path, d_file)

        tree = None
        xmljson = None
        with open(d_file_path, 'r') as content_file:
            txt = content_file.read()
            tree = etree.XML('<topic' + txt.split('<topic')[1])

        if tree == None:
            log("dita files don't exist or format is invalid")
            sys.exit(1)

        # check if author tag exists

        # parse body
        body = tree.xpath("//body")

        for el in tree.iter("*"):
            if el.tag == "author":
                el.set("text", cAuthor)
                # xml_str = xml_str+"<author>{}</author>\n\t".format(cAuthor)
            elif el.tag in tags.keys():
                """ Check if ###, ###Fig, ###Tab, ###Form exits """
                current_level = None
                if 'level' in tags[el.tag]:
                    current_level = tags[el.tag]['level']

                if el.tag == "topic":
                    el.set("id", "xx") # question? t[topic]
                    
                    if current_level and current_level < 3:
                        for tag_name in TAG_LEVEL[current_level+1:]:
                            tags[tag_name]["count"] = 0

                if el.tag == "section":
                    el.set("id", "s{}".format(tags[el.tag]["count"]))
                    if current_level and current_level < 3:
                        for tag_name in TAG_LEVEL[current_level+1:]:
                            tags[tag_name]['count'] = 0

                if el.tag == "p":
                    el.set("id", "s{}_p{}".format(
                        tags["section"]["count"],
                        tags["p"]["count"]))
                    if current_level and current_level < 3:
                        for tag_name in TAG_LEVEL[current_level+1:]:
                            tags[tag_name]['count'] = 0

                if el.tag == "ph":
                    el.set("id", "s{}_p{}_ph{}".format(
                        tags["section"]["count"],
                        tags["p"]["count"],
                        tags["ph"]["count"]))
                    if current_level and current_level < 3:
                        for tag_name in TAG_LEVEL[current_level+1:]:
                            tags[tag_name]['count'] = 0

                if el.tag == "simpletable":
                    el.set("id", "s{}_p{}_ph{}_st{}".format(
                        tags["section"]["count"],
                        tags["p"]["count"],
                        tags["ph"]["count"],
                        tags["simpletable"]["count"]))

                if el.tag == "li":
                    el.set("id", "s{}_p{}_ph{}_st{}_li{}".format(
                        tags["section"]["count"],
                        tags["p"]["count"],
                        tags["ph"]["count"],
                        tags["simpletable"]["count"],
                        tags["li"]["count"],))

                tags[el.tag]['count'] = tags[el.tag]['count'] + 1

                """ dynamic solution (later) """
                # for tag in tags:
                #     if 'level' in tags[tag]:
                #         if current_level < tags[tag]['level']:
                #             tags[tag]['level'] = 0
                #         else:

            else:
                pass

        # Write each dita file
        xml_str = base_str + str(etree.tostring(tree).decode())
        filename = 'NoName'
        try:
            filename = tree.xpath("//topic/title/text()")[0]
            filename = filename.replace(" ", "")
        except:
            pass

        with open(output_path+filename+".dita", "w") as f:
            f.write(xml_str)            

        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        if "###Fig" in txt:
            # add figId and title to FigureList.dita
            print("processing ###Fig")
            pass
        elif "###Table" in txt:
            print("processing ###Table")
            # add figId and title to FigureList.dita
        elif "###Form" in txt:
            print("processing ###Form")
            # add figId and title to FigureList.dita
        elif "###" in txt:
            """ subsitute & add referenceId, documentLink
            to reference arrary"""
            print("processing ###!")
            start_points = [m.start() for m in re.finditer('###', txt)]
            for s_point in start_points:
                input_code = txt[s_point:].split("###")[1].split('@@')[0]
                codes = input_code.split("##")
                if len(codes) > 3:
                    if codes[0][:3] == "Fig" or codes[0][:3] == "Tab" or codes[0][:3] == "Form":
                        pass
                    else:
                        print("~~~~~~~~~~~ ### here ~~~~~~~~~~~~")
                        reference_item = dict()
                        referenceIds = []
                        referenceAPAs = []
                        documentLinks = []

                        reference_item['x'] = 'this'
                        reference_item['cito'] = codes[1].replace('c=', '')
                        # Notice: let's assume x is "this" for now
                        for code in codes[2:]:
                            if 't=' in code:
                                reference_item['t'] = code.replace('t=', '')
                            elif 'p=' in code:
                                reference_item['p'] = code.replace('p=', '')
                            else:
                                ma_attrs = ma[ma.index==int(code)]
                                if len(ma_attrs) > 0:
                                    referenceIds.append(code)

                        if len(referenceIds) == 0:
                            reference_item['ids'] = None
                        else:
                            print(reference_item)
                            for ref_id in referenceIds:
                                apa = ma['Reference Entry (APA)'][int(ref_id)]
                                if str(apa) != "nan":
                                    referenceAPAs.append(apa)
                                else:
                                    referenceAPAs.append('')

                                # create output xref
                                if str(ma['Attachment'][int(ref_id)]) != "nan":
                                    xref_href = ma['Attachment'][int(ref_id)]
                                    if 'p' in reference_item:
                                        # Notice: Is it necessary? "view=fitH,100"
                                        xref_href = '{}?page={}&view=fitH,100'.format(
                                            xref_href, reference_item['p'])
                                    documentLinks.append({'url': xref_href,
                                                        'type': 'Attachment' })
                                elif str(ma['DOI'][int(ref_id)]) != "nan":
                                    xref_href = ma['DOI'][int(ref_id)]
                                    documentLinks.append({'url': xref_href,
                                                        'type': 'DOI' })
                                elif str(ma['URL'][int(ref_id)]) != "nan":
                                    xref_href = ma['URL'][int(ref_id)]
                                    documentLinks.append({'url': xref_href,
                                                        'type': 'URL' })
                                else:
                                    documentLinks.append({'type': 'other' })

                            reference_item['ids'] = '##'.join(referenceIds)
                            reference_item['referenceAPAs'] = '; '.join(x for x in referenceAPAs)
                            reference_item['documentLinks'] = documentLinks
                            references.append(reference_item)

        else:               
            pass

    """ Sort referenceAPAs arrary: Notice: discuss later"""
    """ Produce ReferenceLst.dita """

    # write prog of ReferenceList.dita Notice: discuss latre
    reference_str = '' + base_str
    reference_str = reference_str + '<prog>\n\t<author>{}</author>\n</prog>\n\t'.format(cAuthor)
    # write body of ReferenceList.dita
    reference_str = reference_str + '<body>' 
    for reference_item in references:
        documentLinks = reference_item['documentLinks']
        if len(documentLinks) > 0:
            tmp = None
            if documentLinks[0]['type'] == "Attachment":
                """ Notice: 
                    1. <cite>{3}</cite> output is correct? 
                    2. art_{2}: {2} can be multiple ids? """
                tmp = '<p>\n\t \
                    <xref href="{0}" format=pdf" scope="external">\
                    <cite otherprops="{1}" keyref="references/art_{2}">{3}</cite></xref>\n\t\
                    </p>\n'.format(documentLinks[0]['url'],
                                reference_item['cito'],
                                reference_item['ids'],
                                reference_item['referenceAPAs'])
            elif documentLinks[0]['type'] == "DOI" or documentLinks[0]['type'] == "URL":
                """ Notice: desc is '??' now.
                    where can I reference the value of it? """
                tmp = '<p>\n\t \
                    <xref href="{0}" format="html" scope="external">\
                    <cite otherprops="{1}" keyref="references/art_{2}">\n\t\
                    <desc>??</desc>{3}\n\t</cite>\n\t</xref> \
                    </p>\n'.format(documentLinks[0]['url'].
                                    reference_item['cito'],
                                    reference_item['ids'],
                                    reference_item['referenceAPAs'])
            else:
                tmp = '<p>\n\t \
                    <cite otherprops="{0}" keyref="references/art_{1}">\
                    <desc>??</desc>{2}</cite>\
                    </p>\n'.format(reference_item['cito'],
                                reference_item['ids'],
                                reference_item['referenceAPAs'])

            reference_str = reference_str + tmp
    reference_str = reference_str + '</body></topic>'

    with open(output_path+'ReferenceList.dita', 'w') as f:
        f.write(reference_str)


    """ Write *List.dita files """








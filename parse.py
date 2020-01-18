import os
import sys
import re
import logging
import time
from datetime import datetime
import pandas as pd
import xmltodict
import xml.etree.ElementTree as et
from lxml import etree, html
import pdb

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
    fig_abbr = "Fig"
    table_abbr = "Tab"
    form_abbr = "Form"
    abbr_fields = {}
    references = []
    referenceIds = []

    """ Notice: topic.dtd should be changed for each files? i.e 
        ReferencesList.dtd, FigureList.dtd, etc """
    base_str = '<?xml version="1.0" encoding="UTF-8"?>\n\
                <!DOCTYPE topic PUBLIC "-//OASIS//DTD DITA Topic//EN" "topic.dtd">'
    
    # Get values from PathAndValues sheet
    # need to be updated later for dynamic
    try:
        cPublicTitle = validate_cell_value(pv["Value"][0])
        cAuthor = validate_cell_value(pv["Value"][1])
        dita_map_path = validate_cell_value(pv["Value"][6])
        input_path = validate_cell_value(pv["Value"][10])
        output_path = validate_cell_value(pv["Value"][11])
        fig_abbr = validate_cell_value(pv["Value"][12])
        table_abbr = validate_cell_value(pv["Value"][13])
        form_abbr = validate_cell_value(pv["Value"][14])

        # initialize abbreviation coutns for Tab, Figure, and Form
        abbr_fields[fig_abbr] = {'count': 0, 'data': [], "name": "Figure"}
        abbr_fields[table_abbr] = {'count': 0, 'data': [], "name": "Table"}
        abbr_fields[form_abbr] = {'count': 0, 'data': [], "name": "Form"}
    except Exception as e:
        print("Error when reading values from PathAndValues")
        log(str(e))
        sys.exit(1)

    print("#############: ", abbr_fields)

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
            base_str = txt.split('<topic')[0]
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

        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        start_points = [m.start() for m in re.finditer('###', txt)]
        for s_point in start_points:
            if "###" in txt:
                input_code = txt[s_point:].split("###")[1].split('@@')[0]
                codes = input_code.split("##")
                if len(codes) > 3: 
                    print("~~~~~~~~~~~ ### here ~~~~~~~~~~~~")
                    reference_item = dict()
                    referenceIds = []
                    references = []
                    documentLinks = []

                    reference_item['start_point'] = s_point
                    reference_item['x'] = 'this'
                    reference_item['cito'] = codes[1].replace('c=', '')
                    # Notice: let's assume x is "this" for now
                    for code in codes[2:]:
                        if 't=' in code:
                            t_code = code.replace('t=', '')
                            if int(t_code) == 1:
                                reference_item['a'] = 2
                            elif int(t_code) == 3:
                                reference_item['a'] = 4
                            else:
                                reference_item['a'] = int(t_code)
                        elif 'p=' in code:
                            reference_item['p'] = code.replace('p=', '')
                        else:
                            code_ids = code.split("#")
                            for code_id in code_ids:
                                ma_attrs = ma[ma.index==int(code_id)]
                                if len(ma_attrs) > 0:
                                    referenceIds.append(code_id)

                    if len(referenceIds) == 0:
                        reference_item['ids'] = None
                    else:
                        if len(referenceIds) > 1:
                            reference_item['a'] = 5

                        xref_str = ''
                        for ref_id in referenceIds:
                            base_idx = 16
                            column_name = ma.columns[base_idx+reference_item['a']]
                            if ref_id in referenceIds:
                                full_reference = ''
                                if ref_id not in references:
                                    full_reference = '' + ma['Reference Entry (APA)'][int(ref_id)]
                                    references.append(ref_id)

                                apa = ma[column_name][int(ref_id)]
                                if str(apa) != "nan":
                                    if reference_item['a'] == 5:
                                        apa = apa.replace('(', '').replace(')', '')

                                d_link = dict()
                                # create output xref
                                if str(ma['Attachment'][int(ref_id)]) != "nan":
                                    xref_href = ma['Attachment'][int(ref_id)]
                                    if 'p' in reference_item:
                                        # Notice: Is it necessary? "view=fitH,100"
                                        xref_href = '{}?page={}&view=fitH,100'.format(
                                            xref_href, reference_item['p'])
                                    d_link = {'url': xref_href,
                                            'type': 'Attachment' }
                                elif str(ma['DOI'][int(ref_id)]) != "nan":
                                    xref_href = ma['DOI'][int(ref_id)]
                                    d_link = {'url': xref_href, 'type': 'DOI' }
                                elif str(ma['URL'][int(ref_id)]) != "nan":
                                    xref_href = ma['URL'][int(ref_id)]
                                    d_link = {'url': xref_href, 'type': 'URL' }
                                else:
                                    d_link = {'type': 'other' }

                                if d_link['type'] == "Attachment":
                                    """ Notice: 
                                        1. <cite>{3}</cite> output is correct? 
                                        2. art_{2}: {2} can be multiple ids? """
                                    xref_str = xref_str + '<xref href="{0}" format=pdf" scope="external">\
                                        <cite otherprops="{1}" keyref="references/art_{2}">{3}</cite>\
                                        </xref>'.format(d_link['url'],
                                                    reference_item['cito'],
                                                    ref_id,
                                                    apa) + '; '
                                elif d_link['type'] == "DOI" or d_link['type'] == "URL":
                                    """ Notice: desc is '??' now.
                                        where can I reference the value of it? """
                                    xref_str = xref_str + '<xref href="{0}" format="html" scope="external">\
                                        <cite otherprops="{1}" keyref="references/art_{2}">\
                                        <desc>{3} in {4} authored by {5}</desc>{6}</cite>\
                                        </xref>'.format(d_link['url'],
                                                        reference_item['cito'],
                                                        ref_id,
                                                        apa,
                                                        cPublicTitle,
                                                        cAuthor,
                                                        full_reference) + '; '
                                else:
                                    xref_str = xref_str + '<cite otherprops="{0}" keyref="references/art_{1}">\
                                        {2} in {3} authored by {4}</desc>{5}\
                                        </cite>'.format(reference_item['cito'],
                                                    ref_id,
                                                    apa,
                                                    cPublicTitle,
                                                    cAuthor,
                                                    full_reference) + '; '

                        replace_str = txt[s_point:].split("@@")[0]+'@@'
                        xml_str = xml_str.replace(replace_str, xref_str[:-2])

                else:
                    abbr_code = codes[0].split("#")[0]
                    if abbr_code in [fig_abbr, table_abbr, form_abbr]:
                        abbr_element_text = codes[0].replace('#', '').replace('@@', '')
                        abbr_fields[abbr_code]['count'] = abbr_fields[abbr_code]['count'] + 1
                        abbr_fields[abbr_code]['data'].append({
                            'num': abbr_fields[abbr_code]['count'],
                            'text': abbr_element_text })

                        abbr_str = '<p id="{0}{3}" otherprops="doco:{1}"> \
                                    <ph otherprops="doco:Label"><b>{2} {3}</b></ph\
                                    <ph><b>{4}</b></ph>\
                                    </p>'.format(abbr_code.lower(),
                                                abbr_fields[abbr_code]["name"],
                                                abbr_code,
                                                abbr_fields[abbr_code]['count'],
                                                abbr_element_text)

                        replace_str = txt[s_point:].split("@@")[0]+'@@'
                        xml_str = xml_str.replace(replace_str, abbr_str)
        else:               
            pass

    with open(output_path+'Method.dita', 'w') as f:
        f.write(xml_str)

    """ Sort references arrary: Notice: discuss later"""
    """ Produce ReferenceLst.dita """









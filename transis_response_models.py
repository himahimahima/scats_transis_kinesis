r"""
kinesis_response_models.py creates a TransisResponse out of the bytesting that transis returns
"""

import xml.etree.ElementTree as ET
import utils
import pprint
from xml.dom import minidom
import gzip
import io
import logging

log = logging.getLogger(__name__)

class DetectorCountMessage:
    """SCATS detector count message that has one site's counts for a 5 minutes period.

    Attributes:
        detector_count_message_element (xml.etree.ElementTree): root of the xml element.
        transis_stream_timestamp       (str)                  : timestamp provided by transis to represent the period over which the counts represent.

    """    
    def __init__(self,detector_count_message_element):
        self.detector_count_message_element = detector_count_message_element
        self.collectionendtimestamp_plus_3_mins = self.detector_count_message_element.attrib['date']

    def to_dict(self):
        """
        Returns a Dict representation of the DetectorCountMessage
        """
        kinesis_record = {}
        region = self.detector_count_message_element.attrib['reg']
        site_id     = self.detector_count_message_element.attrib['Sid']
        date        = self.detector_count_message_element.attrib['date']
        kinesis_record["collectionIntervalSecs"] = 300
        kinesis_record['region'] = str(region)
        kinesis_record['siteId'] = str(site_id)
        kinesis_record['collectionendtimestamp_plus_3_mins'] = utils.get_epoc_from_timestamp_string(str(date))
        detector_counts = {}
        for detector in self.detector_count_message_element.find('Detectors'):
            if 'Did' in detector.attrib and 'count' in detector.attrib:
                detector_counts[str(detector.attrib['Did'])]=detector.attrib['count']
        kinesis_record['detectorCounts'] = detector_counts
        return kinesis_record
    
    def is_empty(self):
        if len(self.detector_count_message_element.find('Detectors')) == 0:
            return True
        else:
            return False



class DetectorCountMessages:
    """A collection of DetectorCountMessages for the entire network for a 5 minutes period

    Attributes:
        detector_count_messages_element    (xml.etree.ElementTree): root of the xml element.
        num_sites                          (int)                  : total number of sites in the record
        detector_count_message_list        (DetectorCountMessage) : list of all the individual DetectorCountMessage objects
        collectionendtimestamp_plus_3_mins (str)                  : timestamp provided by transis to represent the period over which the counts represent.

    """    
    def __init__(self,detector_count_messages_element):
        self.detector_count_messages_element = detector_count_messages_element
        self.num_sites = self.get_num_sites()
        self.detector_count_message_list = [DetectorCountMessage(e) for e in detector_count_messages_element]
        self.collectionendtimestamp_plus_3_mins = self.get_collectionendtimestamp_plus_3_mins()

    def get_num_sites(self):
        if(self.detector_count_messages_element):
            return len(self.detector_count_messages_element)
        else:
            return 0
    
    def get_collectionendtimestamp_plus_3_mins(self):
        return self.detector_count_messages_element[0].attrib['date']




# class SiteLayouts:

#     def __init__(self,detector_layouts_element):
#         self.detector_layouts_element = detector_layouts_element
#         self.num_sites = self.get_num_sites()
#         self.site_layout_list = [SiteLayout(e) for e in detector_layouts_element]
    
#     def get_num_sites(self):
#         if(self.detector_layouts_element):
#             return len(self.detector_layouts_element)
#         else:
#             return 0

#     def to_csv_sites(self,file_name):
#         with open(file_name,"w") as fh:
#             fh.write(self.site_layout_list[0].get_attributes_as_csv_header()+"\n")
#             for site in self.site_layout_list:
#                 fh.write(site.to_string() + "\n")
    
#     def to_csv_roads(self,file_name):
#         with open(file_name,"wb") as fh:
#             for site in self.site_layout_list:
#                 for arm in site.arms:
#                     fh.write(site.to_string(arm) + "\n")

                
class TransisXMLElement():
    def __init__(self,root):
        self.root = root
        self.attributes = self.get_attributes()

    def to_string(self):
        row = ''
        for attr in self.attributes:
            row = row + '"' + self.root.get(attr) + '"' + ','
        return row[:-1]

    def get_attributes(self):
        return list(self.root.attrib.keys())
    
    def get_attributes_as_csv_header(self):
        header = ""
        for attr in self.attributes:
            header = header + attr + ","
        return header[:-1]


class SiteLayout(TransisXMLElement):
    
    def __init__(self,site_layout_root):
        super().__init__(site_layout_root)
        self.arms = self.__get_element("Arms")
        self.detectors = self.__get_element("Detectors")
        self.streets = self.__get_element("Streets") 
        self.sgs = self.__get_element("SGs") 
        self.phases = self.__get_element("Phases")
    
    def __get_element(self,element_name):
        element = self.root.find(element_name)
        if(element and element_name == "Arms"):
            return Arms(element)
        elif (element and element_name == "Detectors"):
            return Detectors(element)
        elif (element and element_name == "Streets"):
            return Streets(element)
        elif (element and element_name == "SGs"):
            return SGs(element)
        elif (element and element_name == "Phases"):
            return Phases(element)                         
        else:
            return None
    
    # def get_subcomponent(self,subcomponent):
    #     subcomponents = {
    #         "arms": self.arms,
    #         "detectors": self.detectors,
    #         "streets": self.streets,
    #         ""
    #     }
    #     return subcomponents[subcomponent]

class SiteLayouts(TransisXMLElement):
    def __init__(self, site_layouts_root):
        super().__init__(site_layouts_root)
        self.site_layout_list = [SiteLayout(e) for e in site_layouts_root]
        self.num_sites = self.get_num_sites()
    
    def get_num_sites(self):
        if(self.site_layout_list):
            return len(self.site_layout_list)
        else:
            return 0
    
    def get_csv_string(self,subcomponent=None):
        csv = io.StringIO()
        header = self.get_csv_header(subcomponent)
        csv.write(header)
        for site in self.site_layout_list:
            if subcomponent == "sites":
                csv.write(site.to_string() + "\n")
            if subcomponent == "arms" and site.arms:
                for arm in site.arms.arms_list:
                    csv.write('"' + site.root.get("sId")+ '",'+arm.to_string() + "\n")
            elif subcomponent == "detectors" and site.detectors:
                for detector in site.detectors.detectors_list:
                    csv.write('"' + site.root.get("sId")+ '",'+ detector.to_string() + "\n")
            elif subcomponent == "streets" and site.streets:
                for street in site.streets.streets_list:
                    csv.write('"' + site.root.get("sId")+ '",'+ street.to_string() + "\n")
            elif subcomponent == "sgs" and site.sgs:
                for sg in site.sgs.sgs_list:
                    csv.write('"' + site.root.get("sId")+ '",'+ sg.to_string() + "\n")                    
            elif subcomponent == "phases" and site.sgs:
                for phase in site.phases.phases_list:
                    for sngo in phase.root.find("SGNos"):
                        csv.write(f'"{site.root.get("sId")}","{phase.root.get("name")}","{sngo.text}"\n')                          
        return csv.getvalue()
    
    def get_csv_header(self, subcomponent):
        csv_headers = {
            "sites": self.site_layout_list[0].get_attributes_as_csv_header()+"\n",
            "arms": 'sId,' + self.site_layout_list[0].arms.arms_list[0].get_attributes_as_csv_header()+"\n",
            "detectors": 'sId,' + self.site_layout_list[0].detectors.detectors_list[0].get_attributes_as_csv_header()+"\n",
            "streets": 'sId,' + self.site_layout_list[0].streets.streets_list[0].get_attributes_as_csv_header()+"\n",
            "sgs": 'sId,' + self.site_layout_list[0].sgs.sgs_list[0].get_attributes_as_csv_header()+"\n",
            "phases": 'sId,name,sgno\n'
        }
        return csv_headers[subcomponent]


    
class Arms(TransisXMLElement):
    def __init__(self, arms_root):
        super().__init__(arms_root)
        self.arms_list = [Arm(e) for e in arms_root]
    
    def to_string(self):
        csv = io.StringIO()
        for arm in self.arms_list:
            csv.write(arm.to_string() + "\n") 
        return csv.getvalue()
    
    def get_attributes_as_csv_header(self):
        header = ""
        for attr in self.attributes:
            header = header + attr + ","
        return header[:-1]

class Arm(TransisXMLElement):
    def __init__(self, arms_root):
        super().__init__(arms_root)

class Detectors(TransisXMLElement):
    def __init__(self, detectors_root):
        super().__init__(detectors_root)
        self.detectors_list = [Detector(e) for e in detectors_root]

class Detector(TransisXMLElement):
    def __init__(self, detector_root):
        super().__init__(detector_root) 

class Streets(TransisXMLElement):
    def __init__(self, streets_root):
        super().__init__(streets_root)
        self.streets_list = [Street(e) for e in streets_root]

class Street(TransisXMLElement):
    def __init__(self, street_root):
        super().__init__(street_root)

class SGs(TransisXMLElement):
    def __init__(self, sgs_root):
        super().__init__(sgs_root)
        self.sgs_list = [SG(e) for e in sgs_root]

class SG(TransisXMLElement):
    def __init__(self, sg_root):
        super().__init__(sg_root)

class Phases(TransisXMLElement):
    def __init__(self, phases_root):
        super().__init__(phases_root)
        self.phases_list = [Phase(e) for e in phases_root]

class Phase(TransisXMLElement):
    def __init__(self, phase_root):
        super().__init__(phase_root)


class TransisResponse:
    """A Response object from SCATS Transis API

    Attributes:
        byte_string                 (bytes)                 : bytesstring recieved from transis api
        root                        (xml.etree.ElementTree) : xml object of response
        detector_count_messages     (DetectorCountMessages) : The DetectorCountMessages object in the response if it exists
        response_received_timestamp (str)                   : The time that the entire response was recieved from the requestor service.

    """ 
    def __init__(self,byte_string):
        self.byte_string = byte_string
        self.root = self._xml_from_bytes()
        self.detector_count_messages = self.get_detector_count_messages()
        self.site_layouts = self.get_site_layouts()
        self.response_received_timestamp = utils.get_formatted_current_timestamp()
    
    def _xml_from_bytes(self):
        """Transform XML bytesting into a xml.etree.ElementTree object"""
        xml_string = self.byte_string.decode("utf-8")
        root = ET.fromstring(xml_string) 
        return root
    
    def get_detector_count_messages(self):
        """Get the DetectorCountMessages in the xml response and return the DetectorCountMessages object"""
        detector_count_message_element = self.root.find('DetectorCountMessages')
        if(detector_count_message_element):
            detector_count_messages = DetectorCountMessages(detector_count_message_element)
            return detector_count_messages
        else:
            return None

    def get_site_layouts(self):
        """Returns the SiteLayouts objecst if the response has a SiteLayouts element in the response."""
        site_layouts_element = self.root.find('SiteLayouts')
        if(site_layouts_element):
            site_layouts = SiteLayouts(site_layouts_element)
            return site_layouts
        else:
            return None
    
    def is_error(self):
        error = self.root.get("error")
        if error in ["true", "True"]:
            return self.root.find("Errors")[0].get("msg")
        else:
            return False
    
    def to_file(self,file_name):
        xmlstr = minidom.parseString(ET.tostring(self.root)).toprettyxml(indent="   ")
        with open(file_name, "w") as f:
            f.write(xmlstr)
    
    def to_string(self):
        return self.byte_string.decode("utf-8")
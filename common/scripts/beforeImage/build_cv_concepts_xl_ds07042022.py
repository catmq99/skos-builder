import cx_Oracle
import time
import io
import pandas as pd
import requests
import json
from SPARQLWrapper import SPARQLWrapper, RDF, N3, CSV, JSON, RDFXML
from rdflib import Graph, Literal, RDF, URIRef,  Namespace #basic RDF handling
from rdflib.namespace import FOAF , XSD, DC , OWL, RDFS, DCTERMS, SKOS #most common namespaces
import urllib.parse #for parsing strings to URI's
import os
import sys
import datetime
import re

cmdargs = sys.argv

connection = cx_Oracle.connect(
    user="sch_rd",
    password="rd",
    dsn="10.250.45.51/xepdb1")

print("Successfully connected to Oracle Database")

def_cursor = connection.cursor()
alt_cursor = connection.cursor()
broader_cursor = connection.cursor()
mapping_cursor = connection.cursor()
deprecated_cursor = connection.cursor()

def camel_case(s):
  s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
  return ''.join([s[0].lower(), s[1:]])

def check_concept_list(concept_string):
    # this will create an arrary of broader concepts (by their pref label)
    # return the array on preflabels
    # expecting input to be a comma seperated list of broader preflabels
    # 1. turn the list into an array
    # 2. check each element to see that it is a valid preflabel
    # 3. retun the array of valid broaders

    #broader_string = "bone densitometry,ultrasound"
    concepts_list = concept_string.split(",")
    print ("broaders string is: ",concept_string)
    #print ("broaders list is: ",broader_list)
    return concepts_list

def check_mapping_type(mapping, verbose_on):
    if verbose_on: print("!!!  mapping is ->",mapping)
    if verbose_on: print("!!!  str lower mapping is ->", str.lower(mapping))
    time.sleep(2)
    if str.lower(mapping) == "related":
        return "relatedMatch"
    if str.lower(mapping) == "close":
        return "closeMatch"
    if str.lower(mapping) == "exact":
        return "exactMatch"
    if str.lower(mapping) == "broad":
        return "broadMatch"
    if str.lower(mapping) == "narrow":
        return "narrowMatch"


def main(target_vocab, eClass, ns, verbose_on ):
    # -- inputs we need for this script
    # enumeratedClass - e.g. "BiospecimenCollectionMethod", "ImagingModality"
    enumeratedClass = eClass

    # namespace - e.g. biospecimen, imaging, indication
    namespace = ns

    # -- the variables we'll build using the inputs
    # ns_data - the data namespace where Concepts and ConceptSchemes will be created
    if (target_vocab[0:2] == "AZ") or (target_vocab[0:1] == "R"):
        nds_base = Namespace('https://purl.astrazeneca.net/rd/data/')
    if target_vocab[0:2] == "AG": #https://www.agrimetrics.co.uk/data/crop/Variety/VarietyScheme
        nds_base = Namespace('https://www.agrimetrics.co.uk/data/')

    if (target_vocab[0:2] == "AZ") or (target_vocab[0:1] == "R"):
        nsv_base = Namespace('https://purl.astrazeneca.net/rd/vocab/')
    if target_vocab[0:2] == "AG":
        nsv_base = Namespace('https://www.agrimetrics.co.uk/def/')

    ns_data = Namespace(nds_base + namespace + '/' + enumeratedClass + '/')
    if verbose_on: print("!!!  ns_data ->", ns_data)
    if verbose_on: print("!!!  nds_base ->", nds_base)

    # ns_vocab - the namespace where all entity, class and property definitions will be created
    ns_vocab = Namespace(nsv_base + namespace + '/' + enumeratedClass + '/')
    schema = Namespace('http://schema.org/')
    meta = Namespace('https://purl.astrazeneca.net/rd/vocab/meta/')
    ident = Namespace('https://purl.astrazeneca.net/rd/vocab/identifier/')
    cvns = ns_data # the default namespace
    #dct = Namespace('http://purl.org/dc/terms/')
    #dc = Namespace('http://purl.org/dc/elements/1.1/')
    SKOSXL = Namespace('http://www.w3.org/2008/05/skos-xl#')


    # setup the graph
    g_concepts = Graph()
    g_concepts.bind("skos", SKOS)
    g_concepts.bind("dc", DC)
    g_concepts.bind("skosxl", SKOSXL)
    g_concepts.bind(enumeratedClass.lower(), ns_data)
    g_concepts.bind("meta", meta)
    g_concepts.bind("dct", DCTERMS)
    g_concepts.bind("ident", ident)
    g_concepts.bind("owl", OWL)

    cv_concept_scheme = URIRef(cvns + enumeratedClass + "Scheme")
    cv_top_concept = URIRef(cvns + enumeratedClass)

    # iterate over the tables to create ttl stanzas
    # read the concept leaf nodes from the Oracle master (defs) and details (alts) tables
    def_query="select preflabel, definition, nvl(broader,'True') as is_top_concept , nvl(concept_namespace,'default') as concept_namespace from cv_defs_prefs where must_have='Y' and VOCAB='"+target_vocab+"'"
    if verbose_on: print(def_cursor.execute(def_query))
    print("starting concept defintion builds")
    for def_row in def_cursor.execute(def_query):
        # assign values from the SQL cursor row
        concept_pref_label = def_row[0]
        if verbose_on: print("pref label", def_row[0])
        is_top_concept = (def_row[2]=="True")
        if verbose_on: print("is_top_concept", def_row[2])
        concept_definition = def_row[1]
        if verbose_on: print("concept_definition", def_row[1])

        #get uuid for subject
        get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
        shortuuid_data = get_shortuuid_response.text
        shortuuid_object = json.loads(shortuuid_data)
        cv_subject_id = shortuuid_object["shortuuid"]
        # print(cv_subject_id)
        if def_row[3] == 'default': #using the defaul namespace
            cv_subject = URIRef((cvns + cv_subject_id))
        else: #using a non-default namespace, and taking the namespace details from the csv file
            cv_subject = URIRef((ns_data + def_row[3] + cv_subject_id))
        #print('creating terms for ', cv_subject)

        #get uuid for skos xl label
        get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
        shortuuid_data = get_shortuuid_response.text
        shortuuid_object = json.loads(shortuuid_data)
        skosxl_label_id = shortuuid_object["shortuuid"]
        skosxl_label_uri = URIRef((cvns + skosxl_label_id))

        # define skos concept
        g_concepts.add((cv_subject, RDF.type, SKOS.Concept))
        g_concepts.add((cv_subject, SKOS.inScheme, cv_concept_scheme))
        g_concepts.add((cv_subject, DCTERMS.modified, Literal(datetime.datetime.now())))
        #g_concepts.add((cv_concept_scheme, SKOS.hasTopConcept, cv_top_concept))
        if is_top_concept:
            g_concepts.add((cv_subject, SKOS.topConceptOf, cv_concept_scheme))
        #g_concepts.add((cv_subject, SKOS.broader, cv_top_concept))
        #g_concepts.add((cv_top_concept, SKOS.narrower, cv_subject))
        g_concepts.add((cv_subject, SKOS.definition, Literal(def_row[1], lang="en")))

        # pref labels
        # this line adds a skos core pref label
        g_concepts.add((cv_subject, SKOS.prefLabel, Literal(def_row[0].lower(), lang="en")))
        g_concepts.add((cv_subject, RDFS.label, Literal(def_row[0].lower(), lang="en")))

        #  these lines build the skos-xl labels
        g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
        g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(def_row[0].lower(), lang="en")))
        g_concepts.add((cv_subject, SKOSXL.prefLabel, skosxl_label_uri))

        # get the altlabels for this concept and create the skos labels for them
        #
        if verbose_on: print("building alt labels ...")
        # query to retrieve the literal values and language
        alt_query = "select altlabel , lang , nvl(system, 'non-system') from cv_alts a where preflabel = '"+concept_pref_label+"' and VOCAB='"+target_vocab+"'"
        print("alt query is: ", alt_query)
        time.sleep(3)

        #for each altLabel create the SKOS altlabel
        for alt_row in alt_cursor.execute(alt_query):
            altLabel_literal = alt_row[0]
            if verbose_on: print("literal is:",altLabel_literal)
            lang = alt_row[1]
            if verbose_on: print("lang tag is:",lang)
            system = alt_row[2]
            if verbose_on: print("system is:",system)

            #print(alt_row)
            if (system == "non-system"):
                # build a core skos altLabel
                g_concepts.add((cv_subject, SKOS.altLabel, Literal(altLabel_literal, lang=lang)))

                # Build a skosxl label and assign as an altLabel
                #
                # get uuid for skos xl label
                get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
                shortuuid_data = get_shortuuid_response.text
                shortuuid_object = json.loads(shortuuid_data)
                skosxl_label_id = shortuuid_object["shortuuid"]
                skosxl_label_uri = URIRef((cvns + skosxl_label_id))
                #  these lines build the skos-xl labels
                g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
                g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(altLabel_literal, lang=lang)))
                g_concepts.add((cv_subject, SKOSXL.altLabel, skosxl_label_uri))
            else:
                # we are dealing with system specific labels. e.g. this is the label used by "sysx". What we
                # need to do is:
                # 1. create a hidden label for this value.
                # 2. create an RDF property for this system's label. e.g. if teh system is called "sysx" then we
                #    build a property for the system. in this case it'll be in the ident namespace, and called
                #    ident:sysxIdentifier
                # 3. Assign a literal value to the concept using the ident:sysxIdentifier property


                # 1.
                # build a core skos hiddenLabel
                #g_concepts.add((cv_subject, SKOS.hiddenLabel, Literal(altLabel_literal, lang=lang)))

                #2. #build a AZ System Identifier (label)
                identifier_property = camel_case(system)+"Identifier"
                g_concepts.add((URIRef(ident + identifier_property), RDFS.subPropertyOf, SKOS.altLabel))

                #3
                g_concepts.add((cv_subject, URIRef(ident+identifier_property), Literal(altLabel_literal, lang=lang)))

                # then we are going to build these as system specific labels, which are not for general use
                get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
                shortuuid_data = get_shortuuid_response.text
                shortuuid_object = json.loads(shortuuid_data)
                label_id = shortuuid_object["shortuuid"]
                label_uri = URIRef((cvns + label_id))

                g_concepts.add((label_uri, RDF.type, ident.Identifier))
                g_concepts.add((label_uri, RDF.type, SKOSXL.label))
                g_concepts.add((label_uri, SKOSXL.literalForm, Literal(altLabel_literal, lang=lang)))
                g_concepts.add((label_uri, ident.identifierType, Literal(system)))
                g_concepts.add((cv_subject, SKOSXL.hiddenLabel, label_uri))


        # Now build the mapped relations: - look at each cv mapping and grab the URI and the orgin.
        #
        if verbose_on: print("building mapped relations for ...",concept_pref_label)
        # 1. create the query to return all the mapped concepts
        #
        mapping_sql_query = "select preflabel, uri, origin, mapping_related_close_exact from cv_mappings where preflabel ='"+concept_pref_label+"' and VOCAB='" + target_vocab + "'"
        for mapping in mapping_cursor.execute(mapping_sql_query):
            if verbose_on: print("(prefLabel)->", mapping[0], "(URI)->", mapping[1], "(ORIGIN)->", mapping[2], "(related)->",mapping[3])
            mapped_uri=mapping[1]
            mapped_origin=mapping[2]
            mapping_relation=check_mapping_type(mapping[3], verbose_on)
            if mapping_relation == "exactMatch":
                g_concepts.add((cv_subject, SKOS.exactMatch, URIRef(mapped_uri)))
            if mapping_relation ==  "closeMatch":
                g_concepts.add((cv_subject, SKOS.closeMatch, URIRef(mapped_uri)))
            if mapping_relation ==  "relatedMatch":
                g_concepts.add((cv_subject, SKOS.relatedMatch, URIRef(mapped_uri)))
            if mapping_relation ==  "broaderMatch":
                g_concepts.add((cv_subject, SKOS.broaderMatch, URIRef(mapped_uri)))
    print("Completed defintion builds")


    # Now build the depracted relations: - look at each def and see if it has a "replaced by" in the depracted column. The depracted values
    # are comma delimited, and it is the list of Concepts that replace this one.
    # <concept> dcterms:replacedBy <the concpet in the deprecated column>
    if verbose_on: print("building deprecated relations")

    # 1. create the query to return all the deprecated concepts that have a "replaced by" value(s)
    # NB - the "replaced by" ("deprecated") column is a comma seperated list of preflabels of replacement concepts  values
    print("starting depreaction builds")
    deprecated_sql_query = "select preflabel, deprecated from cv_defs_prefs where deprecated is not null and must_have='Y' and VOCAB='" + target_vocab + "'"

    for deprecated_row in deprecated_cursor.execute(deprecated_sql_query):
        # 2. For each concept grab the depracted (replaced by) list string, and iterate over its elements
        # e.g. "x-ray,ultrasound" ...
        if verbose_on: print("deprecated_row[0], (deprecated prefLabel)->", deprecated_row[0]," deprecated_row[1], (replaced by prefLabel)->",deprecated_row[1])
        deprecated_pref_label = deprecated_row[0] # this is the preflabel for the deprecated concept we are dealing with


        # 3. smash the list into terms, and for each term go and get the URI of the broader concept
        replacement_list = check_concept_list(deprecated_row[1])  # returns an array of the replaced by concepts

        # 4. iterate over the array of replaced by  preflabels.
        #    - find the replacement concept URI using the deprecatd_replaced_by_pref_label
        #    - find the deprecated concept URI depreacted_pref_label
        if verbose_on: print("deprecated replacements list is: ", replacement_list)
        for replacement_pref_label in replacement_list:
            # build the sparql to find the concpets
            replaced_by_sparql_query = """
                    SELECT ?replacement_concept ?deprecated_concept
                    WHERE {
                    ?replacement_concept a skos:Concept .
                    ?replacement_concept skos:prefLabel """ + '"' + replacement_pref_label + '"@en .' + """
                    ?deprecated_concept a skos:Concept .
                    ?deprecated_concept skos:prefLabel """ + '"' + deprecated_pref_label + '"@en .' + """
                    #?deprecated_concept skos:prefLabel ?narrower_label .
                    }
                    """
            # print("sparql to find the broader URI: ",broader_sparql_query)
            replaced_by_res = g_concepts.query(replaced_by_sparql_query)

            # 5. assert the replaced by  triples
            # print("here's the sparql results")
            for row in replaced_by_res:
            #    res_row = """
            #                row.replacement_concept (replacement_term): """ + triple_row.replacement_concept + """
            #                row.deprecated_concept (deprecated_term): """ + str(row.deprecated_concept) + """
            #                triples to build ... """ + """
            #                <""" + str(row.deprecated_concept) + """>""" + str(DCTERMS.replacedBy) + """<""" + str(row.replacement_concept) + """> .""" + """
            #                <""" + str(row.deprecated_concept) + """>""" +  owl:deprecated true + """ .
            #                <""" + str(row.deprecated_concept) + """>""" + dc:modified + """<""" + str(datetime.datetime.now()) + """> ."""
            #    print(res_row)
                g_concepts.add((row.deprecated_concept, OWL.deprecated, Literal("true", datatype=XSD.boolean)))
                g_concepts.add((row.deprecated_concept, DCTERMS.isReplacedBy, row.replacement_concept))
                g_concepts.add((row.deprecated_concept, DCTERMS.modified, Literal(datetime.datetime.now())))
    print("completed depreaction builds",time.time())

    # ===============
    # Now build the broader relations: - look at each def and see if it has Broader values set. The broader values are comma delimited
    # test strings in the "broader" column.
    print("building broader relations",time.time())
    # 1. create the query to return all the narrower concepts that have a "broader" value(s)
    # NB - the "broader" column is a comma seperated list of preflabels for broader values
    broader_sql_query = "select preflabel, broader from cv_defs_prefs where broader is not null and must_have='Y' and VOCAB='"+target_vocab+"'"

    for broader_row in broader_cursor.execute(broader_sql_query):
        # 2. For each concept grab the broader list string, and iterate over its elements
        # e.g. "x-ray,ultrasound" ...
        if verbose_on: print("broader_row[0], (narrower prefLabel)->", broader_row[0]," broader_row[1], (broader prefLabel)->",broader_row[1])
        narrower_pref_label = broader_row[0] # this is the preflabel for the narrower concept we are dealing

        # 3. smash the list into terms, and for each term go and get the URI of the broader concept
        broader_list = check_concept_list(broader_row[1]) # retruns an array of the broaders

        # 4. iterate over the array of broader preflabels.
        #    - find the broader concept URI using the broader_pref_label
        #    - find the narrower concept URI narrower_pref_label
        if verbose_on: print("broaders list is: ", broader_list)
        for broader_pref_label in broader_list:
            # build the sparql to find the concpets
            broader_sparql_query = """
            SELECT ?broader_concept ?narrower_concept
            WHERE {
            ?broader_concept a skos:Concept .
            ?broader_concept skos:prefLabel """ + '"' + broader_pref_label +'"@en .'+  """
            ?narrower_concept a skos:Concept .
            ?narrower_concept skos:prefLabel """ + '"' + broader_row[0]+'"@en .'+  """
            #?narrower_concept skos:prefLabel ?narrower_label .
            }
            """
            #print("sparql to find the broader URI: ",broader_sparql_query)
            broader_res = g_concepts.query(broader_sparql_query)

            #5. assert the broader triples
            #print("here's the sparql results")
            for row in broader_res:
                res_row=""" 
                row.broader_concept (broader_term): """ + str(row.broader_concept) + """  
                row.narrower_concept (narrower_term): """ + str(row.narrower_concept) + """
                triples to build ... """ + """
                <""" + str(row.narrower_concept) + """> skos:broader <"""+str(row.broader_concept)+"""> .""" + """
                <""" + str(row.narrower_concept) + """> skos:prefLabel """+'"'+narrower_pref_label+'"@en .' + """
                <""" + str(row.broader_concept) + """> skos:narrower <"""+str(row.narrower_concept)+"""> .""" + """
                <""" + str(row.broader_concept) + """> skos:prefLabel """+'"'+broader_pref_label+'"@en .'
                if verbose_on: print("broaders res row is",res_row)
                g_concepts.add((row.narrower_concept, SKOS.broader, row.broader_concept))
                g_concepts.add((row.broader_concept, SKOS.narrower, row.narrower_concept))
    print("completed broader builds",time.time())


    #print(g_concepts.serialize(format='turtle'))
    g_concepts.serialize(destination='../data/az_' + enumeratedClass + '_concepts.ttl', format='turtle')
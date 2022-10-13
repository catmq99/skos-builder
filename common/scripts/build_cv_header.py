# imports
import pandas as pd
from rdflib import Graph, Literal, URIRef, RDF, Namespace, SKOS, RDFS
from SPARQLWrapper import SPARQLWrapper, RDF, N3, CSV, JSON, RDFXML
from rdflib import Graph, Literal, RDF, URIRef, RDFS, Namespace  # basic RDF handling
from rdflib.namespace import FOAF, XSD, DC, OWL ,NamespaceManager # most common namespaces
import urllib.parse  # for parsing strings to URI's
import os
import sys
import time
import datetime
import re
import cx_Oracle

# print(os.environ["PWD"])
print('Argument List:', str(sys.argv))
cmdargs = sys.argv
# print('target vocab is ', str(cmdargs[1]))

connection = cx_Oracle.connect(
    user="sch_rd",
    password="rd",
    dsn="10.250.45.51/xepdb1")

system_cursor = connection.cursor()

#global df_vocab_headers
#df_vocab_headers = pd.DataFrame()

def camel_case(s):
  s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
  return ''.join([s[0].lower(), s[1:]])

def read_cv_meta_data(target_vocab, eClass,ns, verbose_on):
    print("build_cv_header:read_cv_meta_data()")
    print("... starting read_cv_meta_data",target_vocab,eClass,ns)
    print("... target_vocab:", target_vocab)
    print("... eClass:", eClass)
    print("... ns:",ns)

    # -- inputs we need for this script
    # enumeratedClass - e.g. "BiospecimenCollectionMethod", "ImagingModality"
    enumeratedClass = eClass

    # namespace - e.g. biospecimen, imaging, indication
    namespace = ns

    # -- the variables we'll build using the inputs
    # ns_data - the data namespace where Concepts and ConceptSchemes will be created
    if target_vocab[0:1] == "AZ":
        ns_data = Namespace('https://purl.astrazeneca.net/rd/data/' + namespace + '/' + enumeratedClass + '/')
    if target_vocab[0:1] == "AG": #https://www.agrimetrics.co.uk/data/crop/Variety/VarietyScheme
        ns_data = Namespace('https://www.agrimetrics.co.uk/data/' + namespace + '/' + enumeratedClass + '/')
    #ns_data = Namespace('https://purl.astrazeneca.net/rd/data/' + namespace + '/' + enumeratedClass + '/')

    if target_vocab[0:2] == "AZ":
        ns_vocab = Namespace('https://purl.astrazeneca.net/rd/vocab/' + namespace + '/' + enumeratedClass + '/')
    if target_vocab[0:2] == "AG":
        ns_vocab = Namespace('https://www.agrimetrics.co.uk/def/' + namespace + '/' + enumeratedClass + '/')

    # ns_vocab - the namespace where all entity, class and property definitions will be created


    # set file locations
    if verbose_on: print("pwd: ", os.getcwd())
    time.sleep(2)

    local_input_file = "../data/vocab_header_data.csv"
    git_input_file = "./common/header_build/vocab_header_data.csv"
    input_file = local_input_file

    # read the CV header's nodes from the az_vocab_header_data.csv file
    global df_vocab_headers
    df_vocab_headers = pd.read_csv(input_file)
    if verbose_on: print('here is the header  ')
    if verbose_on: print(df_vocab_headers.all())
    if verbose_on: print(df_vocab_headers.head())
    print('header done  ')

    # read the vocabulary meta-data from the headers dataframe
    # #
    # # Vocabulary definition
    # #
    # 1. <https://purl.astrazeneca.net/rd/vocab/clinicalstudy/StudyPhase_CV> a owl:Ontology;
    # 2.  dct:created "23-July-2021";
    # 3.  dct:identifier  <https://purl.astrazeneca.net/rd/vocab/clinicalstudy/StudyPhase_CV>;
    # 4.  owl:versionInfo "StudyPhase_CV-1.0.0"@en;
    # 5.  dct:title "AZ Study Phase CV"@en;
    # 6.  dct:abstract "Phases of a Clinical Study."@en;
    # 7.  meta:repo <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV>;
    # 8.  meta:docs <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV/README.md>;
    # 9.  dct:accrualPolicy <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV/README.md#AccrualPolicy>;
    # 10.  dct:dateAccepted "2022-01-21";
    # 11.  meta:owner <https://purl.astrazeneca.net/en/data/agent/Person/abcd123>
    # 12.  dct:creator <https://purl.astrazeneca.net/en/data/agent/Person/efgh456>
    # 13.  dct:created "2012-11-20";
    # 14.  dct:modified "2022-02-30";
    #  15. meta:lastEditedBy <https://purl.astrazeneca.net/en/data/agent/Person/ijkl789>
    #

def main(target_vocab,eClass,ns, verbose_on):
    if verbose_on: print("build_cv_header:main()")
    if verbose_on: print("... starting main",target_vocab,eClass,ns)
    if verbose_on: print("... target_vocab:", target_vocab)
    if verbose_on: print("... eClass:", eClass)
    if verbose_on: print("... ns:",ns)

    # -- inputs we need for this script
    # enumeratedClass - e.g. "BiospecimenCollectionMethod", "ImagingModality"
    enumeratedClass = eClass

    # namespace - e.g. biospecimen, imaging, indication
    namespace = ns


    # -- the variables we'll build using the inputs
    # ns_data - the data namespace where Concepts and ConceptSchemes will be created
    #ns_data = Namespace('https://purl.astrazeneca.net/rd/data/' + namespace + '/' + enumeratedClass + '/')

    # ns_vocab - the namespace where all entity, class and property definitions will be created
    #ns_vocab = Namespace('https://purl.astrazeneca.net/rd/vocab/' + namespace + '/' + enumeratedClass + '/')

    print ("target_vocab[0:2] is ", target_vocab[0:1])
    if (target_vocab[0:2] == "AZ") or (target_vocab[0:1] == "R"):
        ns_data = Namespace('https://purl.astrazeneca.net/rd/data/' + namespace + '/' + enumeratedClass + '/')
    if target_vocab[0:2] == "AG": #https://www.agrimetrics.co.uk/data/crop/Variety/VarietyScheme
        ns_data = Namespace('https://www.agrimetrics.co.uk/data/' + namespace + '/' + enumeratedClass + '/')
    #ns_data = Namespace('https://purl.astrazeneca.net/rd/data/' + namespace + '/' + enumeratedClass + '/')

    if (target_vocab[0:2] == "AZ") or (target_vocab[0:1] == "R"):
        ns_vocab = Namespace('https://purl.astrazeneca.net/rd/vocab/' + namespace + '/' + enumeratedClass + '/')
    if target_vocab[0:2] == "AG":
        ns_vocab = Namespace('https://www.agrimetrics.co.uk/def/' + namespace + '/' + enumeratedClass + '/')


    # local_output_file = '../../AZ_IMAGING_MODALITY_BUILD/data/' + VOCAB + '_header_data.ttl'
    local_output_file = '../data/az_' + enumeratedClass + '_header_data.ttl'
    if verbose_on: print("local_output_file :",local_output_file)
    git_output_file = './common/header_build/' + enumeratedClass + '_header_data.ttl'

    # CONSTANT declarations
    #
    # !!!! change this to the row in the header csv you want to use.

    global SINGLE_VOCAB
    SINGLE_VOCAB = target_vocab
    # VOCAB = str(cmdargs[1])

    VOCAB = SINGLE_VOCAB
    DATE = datetime.datetime.now()


    read_cv_meta_data(target_vocab, eClass,ns, verbose_on)
    print("meta data done")
    print(df_vocab_headers.values)
    for index, row in df_vocab_headers.iterrows():
        if verbose_on: print ("SINGLE_VOCAB->", SINGLE_VOCAB, "   row['CV_ID'] ->", row['CV_ID'])
        if (row['CV_ID'] == SINGLE_VOCAB):
            #vocab id
            VOCAB = row['CV_ID'].upper()
            print("VOCAB is ", VOCAB)
            # Ontology metadata
            data_prefix = row['CV_ID'].lower()+'d'
            #print("data_prefix",data_prefix)
            data_uri = row['dataURI']
            #print ("data_uri",data_uri)

            vocab_uri = row['vocabURI']
            vocab_prefix = row['CV_ID'].lower()

            # 1. <https://purl.astrazeneca.net/rd/vocab/clinicalstudy/StudyPhase_CV> a owl:Ontology;
            owl_ontology = row['ontology']
            if verbose_on: print("owl_ontology is",owl_ontology)
            ## 2.  dct:created "23-July-2021";
            dct_created = DATE
            #3.  dct:identifier  <https://purl.astrazeneca.net/rd/vocab/clinicalstudy/StudyPhase_CV>;
            dct_identifier = row['ontology']
            #4.  owl:versionInfo "StudyPhase_CV-1.0.0"@en;
            owl_version_info = row['versionInfo']
            #5.  dct:title "AZ Study Phase CV"@en;
            #dct_title = "AZ "+ row['schemeLabel'].rstrip("Scheme") + " CV"
            dct_title = row['vocabTitle']
            #6.  dct:abstract "Phases of a Clinical Study."@en;
            dct_abstract = row['description']
            #7.  meta:repo <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV>;
            meta_repo = "https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/"+row['ontology_name']
            #8.  meta:docs <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV/README.md>;
            meta_docs = "https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/"+row['ontology_name']+"/README.md"
            #9.  dct:accrualPolicy <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV/README.md#AccrualPolicy>;
            dct_accrual_policy = "https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/"+row['ontology_name']+"/README.md#AccrualPolicy"
            #10.  dct:dateAccepted "2022-01-21";
            dct_date_accepted = row['date_accepted']
            #11.  meta:owner <https://purl.astrazeneca.net/en/data/agent/Person/abcd123>
            meta_owner = row['owner']
            #12.  dct:creator <https://purl.astrazeneca.net/en/data/agent/Person/efgh456>
            dct_creator = row['creator']
            # 13.  dct:created "2012-11-20";
            #dct_created = str(DATE.year) + str(DATE.month)+str(DATE.day)
            #14.  dct:modified "2022-02-30";
            #dct_modified = DATE
            # 15. meta:lastEditedBy <https://purl.astrazeneca.net/en/data/agent/Person/ijkl789>
            meta_last_edited_by = "https://purl.astrazeneca.net/en/data/agent/Person/klck825"
            # 16. dct:subject StudyPhase
            subject = row['subject']
            version = row['version']
            contributor = row['contributor']


            # Concept Scheme and top concept meta-data
            concept_scheme = row['scheme']
            top_concept = row['topConcept']
            scheme_label = row['schemeLabel']
            top_concept_label = row['schemeLabel'].replace("Scheme","").lower()
            skos_concept_scheme = data_uri + concept_scheme

            # Build vocab header graph
            #
            g_vocab_header = Graph()


            schema = Namespace('http://schema.org/')
            dct = Namespace('http://purl.org/dc/terms/')
            meta = Namespace('https://purl.astrazeneca.net/rd/vocab/meta/')
            ident = Namespace('https://purl.astrazeneca.net/rd/vocab/identifier/')
            SKOSXL = Namespace('http://www.w3.org/2008/05/skos-xl#')

            # bind namespaces
            g_vocab_header.bind('dc', DC)
            g_vocab_header.bind('owl', OWL)
            g_vocab_header.bind('dct', dct)
            g_vocab_header.bind('skos', SKOS)
            g_vocab_header.bind(enumeratedClass.lower(), ns_data)
            g_vocab_header.bind(enumeratedClass.lower()+'_v', ns_vocab)
            g_vocab_header.bind('meta', meta)
            g_vocab_header.bind("ident", ident)
            g_vocab_header.bind("skosxl", SKOSXL)

            # Define Concept Scheme and top concept
            #
            g_vocab_header.add((URIRef(skos_concept_scheme), RDF.type, SKOS.ConceptScheme))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(skos_concept_scheme), SKOS.prefLabel, Literal(dct_title + " Scheme", lang='en')))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.contributor, Literal(contributor)))
            g_vocab_header.add((URIRef(data_uri+top_concept), RDF.type, SKOS.Concept))
            g_vocab_header.add((URIRef(data_uri + top_concept), SKOS.prefLabel, Literal(top_concept_label)))
            g_vocab_header.add((URIRef(skos_concept_scheme), SKOS.hasTopConcept, URIRef(data_uri + top_concept)))
            g_vocab_header.add((URIRef(data_uri + top_concept), SKOS.inScheme, URIRef(skos_concept_scheme)))

            # define some meta data about the meta ontology
            #g_vocab_header.add((meta.owner, RDFS.label, Literal("owner")))
            g_vocab_header.add((dct.owner, RDFS.label, Literal("owner")))
            g_vocab_header.add((meta.contributor, RDFS.label, Literal("contributor")))



            # # Vocabulary definition
            # #
            # 1. <https://purl.astrazeneca.net/rd/vocab/clinicalstudy/StudyPhase_CV> a owl:Ontology;
            g_vocab_header.add((URIRef(owl_ontology), RDF.type, OWL.Ontology))
            # 2.  dct:created "23-July-2021";
            g_vocab_header.add((URIRef(owl_ontology), dct.created, Literal(dct_created)))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.created, Literal(dct_created)))
            # 3.  dct:identifier  <https://purl.astrazeneca.net/rd/vocab/clinicalstudy/StudyPhase_CV>;
            g_vocab_header.add((URIRef(owl_ontology), RDFS.isDefinedBy, URIRef(dct_identifier)))
            # 4.  owl:versionInfo "StudyPhase_CV-1.0.0"@en;
            g_vocab_header.add((URIRef(owl_ontology), OWL.versionInfo, Literal(owl_version_info, lang='en')))
            # 5.  dct:title "AZ Study Phase CV"@en;
            g_vocab_header.add((URIRef(owl_ontology), DC.title, Literal(dct_title, lang='en')))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.title, Literal(dct_title, lang='en')))
            # 6.  dct:abstract "Phases of a Clinical Study."@en;
            g_vocab_header.add((URIRef(owl_ontology), DC.description, Literal(dct_abstract, lang='en')))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.description, Literal(dct_abstract, lang='en')))
            # 7.  meta:repo <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV>;
            g_vocab_header.add((URIRef(owl_ontology), meta.repo, URIRef(meta_repo)))
            g_vocab_header.add((URIRef(skos_concept_scheme), meta.repo, URIRef(meta_repo)))
            # 8.  meta:docs <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV/README.md>;
            g_vocab_header.add((URIRef(owl_ontology), meta.doc, URIRef(meta_docs)))
            # 9.  dct:accrualPolicy <https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/StudyPhase_CV/README.md#AccrualPolicy>;
            g_vocab_header.add((URIRef(owl_ontology), dct.accrualPolicy, URIRef(dct_accrual_policy)))
            # 10.  dct:dateAccepted "2022-01-21";
            g_vocab_header.add((URIRef(owl_ontology), dct.dateAccepted, Literal(dct_date_accepted)))
            # 11.  meta:owner <https://purl.astrazeneca.net/en/data/agent/Person/abcd123>
            g_vocab_header.add((URIRef(owl_ontology), meta.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(skos_concept_scheme), meta.owner, Literal(meta_owner)))
            # 12.  dct:creator <https://purl.astrazeneca.net/en/data/agent/Person/efgh456>
            g_vocab_header.add((URIRef(owl_ontology), dct.creator, URIRef(dct_creator)))
            # 13.  dct:created "2012-11-20";
            g_vocab_header.add((URIRef(owl_ontology), dct.created, Literal(dct_created)))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.created, Literal(dct_created)))
            # 14.  dct:modified "2022-02-30";
            g_vocab_header.add((URIRef(owl_ontology), dct.modified, Literal(datetime.datetime.now())))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.modified, Literal(datetime.datetime.now())))
            #  15. meta:lastEditedBy <https://purl.astrazeneca.net/en/data/agent/Person/ijkl789>
            g_vocab_header.add((URIRef(owl_ontology), meta.lastEditedBy, URIRef(meta_last_edited_by)))
            g_vocab_header.add((URIRef(owl_ontology), RDFS.isDefinedBy, URIRef(subject)))
            g_vocab_header.add((URIRef(owl_ontology), DC.subject, URIRef(subject)))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.subject, URIRef(subject)))
            g_vocab_header.add((URIRef(owl_ontology), dct.version, Literal(version)))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.version, Literal(version)))
            g_vocab_header.add((URIRef(owl_ontology), meta.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(owl_ontology), meta.contributor, Literal(contributor)))
            g_vocab_header.add((URIRef(owl_ontology), meta.dateCreated, Literal(str(DATE.year) + str(DATE.month)+str(DATE.day))))

            system_query = "select distinct nvl(system, 'non-system') from cv_alts a where VOCAB='" + target_vocab + "'"
            #
            for system_row in system_cursor.execute(system_query):
                system_literal = system_row[0]
                identifier_property = camel_case(system_literal)+"Identifier"
                if verbose_on: print("system is:", system_literal)
                # Adding here some statements for the skos file that relate az identifier resources to skos
                if system_literal != "non-system":
                    #define the AZ Identfier class
                    g_vocab_header.add((ident.Identifier, OWL.equivalentClass, SKOSXL.Label))
                    # for this system identifier (label) -
                    # 1. define the label property for this system (system literal + "Identifier"
                    g_vocab_header.add((URIRef(ident + identifier_property), RDF.type, OWL.ObjectProperty))
                    g_vocab_header.add((URIRef(ident+identifier_property), RDFS.label, Literal( system_literal+" identifier", lang="en")))


            #g_concepts.add((label_uri, RDF.type, ident.Identifier))
            #g_concepts.add((label_uri, RDF.type, SKOSXL.label))
            #g_concepts.add((label_uri, SKOSXL.literalForm, Literal(altLabel_literal, lang=lang)))
            #g_concepts.add((label_uri, ident.identifierType, Literal(system)))
            #g_concepts.add((cv_subject, SKOSXL.hiddenLabel, label_uri))



            if verbose_on: print(g_vocab_header.serialize(format='turtle'))
            # set ttl serialization destination
            #

            output_file = local_output_file
            if verbose_on: print("serializing to ", output_file)
            #time.sleep(1)
            g_vocab_header.serialize(destination=output_file, format='turtle')

#print("build_cv_header: __name__ is: ",repr(__name__))
time.sleep(2)
if __name__ == "__main__":
    main()

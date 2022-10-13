import datetime
import json
import os
import re
import pandas as pd
import requests
from rdflib import Graph, Literal, RDF, URIRef, Namespace, DC, OWL, RDFS, SKOS, DCTERMS, XSD
import numpy as np

# from rdflib.namespace import XSD, DC, OWL, RDFS, DCTERMS, SKOS


path = f'{os.path.normpath(os.path.join(os.getcwd(), ".."))}/data'

enumeratedClass = "CollectionMethod"
namespace = "biospecimen-collection-method"
target_vocab = "RDMBC"
excel_workbook = 'rdm-demo-biospecimen_coll_method_curation_and_build_template.xlsx'
case_sensitive = False


def df_generator(excel_workbook=excel_workbook):
    for sheet in ['defs', 'alts', 'mappings']:
        df = pd.read_excel(f'{path}/{excel_workbook}', sheet_name=f'cv_{sheet}_template')
        header = df.iloc[0]
        df = df.iloc[1:]
        df.columns = header
        df[df.columns] = df.apply(lambda x: x.str.strip())

        # trim_strings = lambda x: x.strip() if isinstance(x, str) else x
        yield df


vocab_header = pd.read_csv(f'{path}/vocab_header_data_pids.csv')
df1 = df_generator()
defs = next(df1)
alts = next(df1)
mappings = next(df1)


# def read_cv_meta(eClass=enumeratedClass, ns=namespace):
#     ns_data = Namespace('https://pid.astrazeneca.net/rd/data/' + ns + '/' + eClass + '/')
#     ns_vocab = Namespace('https://pid.astrazeneca.net/rd/vocab/' + ns + '/' + eClass + '/')


def camel_case(s):
    s = re.sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])


def build_header(vocab=target_vocab, eClass=enumeratedClass, ns=namespace):
    ns_data = Namespace('https://pid.astrazeneca.net/rd/data/' + ns + '/' + eClass + '/')
    ns_vocab = Namespace('https://pid.astrazeneca.net/rd/vocab/' + ns + '/' + eClass + '/')
    local_output_file = '../data/az_' + enumeratedClass + '_header_data.ttl'
    DATE = datetime.datetime.now()
    for index, row in vocab_header.iterrows():
        if row['CV_ID'] == vocab:
            vocab = row['CV_ID'].upper()
            # data_prefix = row['CV_ID'].lower() + 'd'
            data_uri = row['dataURI']
            # vocab_uri = row['vocabURI']
            # vocab_prefix = row['CV_ID'].lower()
            owl_ontology = row['ontology']
            dct_created = DATE
            dct_identifier = row['ontology']
            owl_version_info = row['versionInfo']
            dct_title = row['vocabTitle']
            dct_abstract = row['description']
            meta_repo = "https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/" + row[
                'ontology_name']
            meta_docs = "https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/" + row[
                'ontology_name'] + "/README.md"
            dct_accrual_policy = "https://bitbucket.astrazeneca.net/projects/ONRD/repos/vocabs/browse/enterprise/" + \
                                 row['ontology_name'] + "/README.md#AccrualPolicy"
            dct_date_accepted = row['date_accepted']
            meta_owner = row['owner']
            dct_creator = row['creator']
            # meta_last_edited_by = "https://pid.astrazeneca.net/en/data/agent/Person/klck825"
            meta_last_edited_by = row['meta:lastEditedBy']

            subject = row['subject']
            version = row['version']
            contributor = row['contributor']

            # Concept Scheme and top concept meta-data

            concept_scheme = row['scheme']
            top_concept = row['topConcept']
            # scheme_label = row['schemeLabel']
            top_concept_label = row['schemeLabel'].replace("Scheme", "").lower()
            skos_concept_scheme = data_uri + concept_scheme

            # Build vocab header graph
            #
            g_vocab_header = Graph()

            # schema = Namespace('http://schema.org/')
            dct = Namespace('http://pid.org/dc/terms/')
            meta = Namespace('https://pid.astrazeneca.net/rd/vocab/meta/')
            ident = Namespace('https://pid.astrazeneca.net/rd/vocab/identifier/')
            SKOSXL = Namespace('http://www.w3.org/2008/05/skos-xl#')

            # bind namespaces
            g_vocab_header.bind('dc', DC)
            g_vocab_header.bind('owl', OWL)
            g_vocab_header.bind('dct', dct)
            g_vocab_header.bind('skos', SKOS)
            g_vocab_header.bind(enumeratedClass.lower(), ns_data)
            g_vocab_header.bind(enumeratedClass.lower() + '_v', ns_vocab)
            g_vocab_header.bind('meta', meta)
            g_vocab_header.bind("ident", ident)
            g_vocab_header.bind("skosxl", SKOSXL)

            # Define Concept Scheme and top concept
            #
            g_vocab_header.add((URIRef(skos_concept_scheme), RDF.type, SKOS.ConceptScheme))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(skos_concept_scheme), SKOS.prefLabel, Literal(dct_title + " Scheme", lang='en')))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.contributor, Literal(contributor)))
            g_vocab_header.add((URIRef(data_uri + top_concept), RDF.type, SKOS.Concept))
            g_vocab_header.add((URIRef(data_uri + top_concept), SKOS.prefLabel, Literal(top_concept_label)))
            # Dscuffell: 01/08/2022: commented this out because the top concpets are derived fronm the input data sheet. and the boolean "is_top_concept:"
            # g_vocab_header.add((URIRef(skos_concept_scheme), SKOS.hasTopConcept, URIRef(data_uri + top_concept)))
            g_vocab_header.add((URIRef(data_uri + top_concept), SKOS.inScheme, URIRef(skos_concept_scheme)))

            # define some metadata about the meta ontology
            # g_vocab_header.add((meta.owner, RDFS.label, Literal("owner")))
            g_vocab_header.add((meta.owner, RDFS.label, Literal("owner")))
            g_vocab_header.add((dct.contributor, RDFS.label, Literal("contributor")))

            # # Vocabulary definition
            # #
            g_vocab_header.add((URIRef(owl_ontology), RDF.type, OWL.Ontology))
            g_vocab_header.add((URIRef(owl_ontology), dct.created, Literal(dct_created)))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.created, Literal(dct_created)))
            g_vocab_header.add((URIRef(owl_ontology), RDFS.isDefinedBy, URIRef(dct_identifier)))
            g_vocab_header.add((URIRef(owl_ontology), OWL.versionInfo, Literal(owl_version_info, lang='en')))
            g_vocab_header.add((URIRef(owl_ontology), DC.title, Literal(dct_title, lang='en')))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.title, Literal(dct_title, lang='en')))
            g_vocab_header.add((URIRef(owl_ontology), DC.description, Literal(dct_abstract, lang='en')))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.description, Literal(dct_abstract, lang='en')))
            g_vocab_header.add((URIRef(owl_ontology), meta.repo, URIRef(meta_repo)))
            g_vocab_header.add((URIRef(skos_concept_scheme), meta.repo, URIRef(meta_repo)))
            g_vocab_header.add((URIRef(owl_ontology), meta.doc, URIRef(meta_docs)))
            g_vocab_header.add((URIRef(owl_ontology), dct.accrualPolicy, URIRef(dct_accrual_policy)))
            g_vocab_header.add((URIRef(owl_ontology), dct.dateAccepted, Literal(dct_date_accepted)))
            g_vocab_header.add((URIRef(owl_ontology), meta.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(skos_concept_scheme), meta.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(owl_ontology), dct.creator, URIRef(dct_creator)))
            g_vocab_header.add((URIRef(owl_ontology), dct.created, Literal(dct_created)))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.created, Literal(dct_created)))
            g_vocab_header.add((URIRef(owl_ontology), dct.modified, Literal(datetime.datetime.now())))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.modified, Literal(datetime.datetime.now())))
            g_vocab_header.add((URIRef(owl_ontology), meta.lastEditedBy, URIRef(meta_last_edited_by)))
            g_vocab_header.add((URIRef(owl_ontology), RDFS.isDefinedBy, URIRef(subject)))
            g_vocab_header.add((URIRef(owl_ontology), DC.subject, URIRef(subject)))
            g_vocab_header.add((URIRef(skos_concept_scheme), DC.subject, URIRef(subject)))
            g_vocab_header.add((URIRef(owl_ontology), dct.version, Literal(version)))
            g_vocab_header.add((URIRef(skos_concept_scheme), dct.version, Literal(version)))
            g_vocab_header.add((URIRef(owl_ontology), meta.owner, Literal(meta_owner)))
            g_vocab_header.add((URIRef(owl_ontology), dct.contributor, Literal(contributor)))
            g_vocab_header.add(
                (URIRef(owl_ontology), meta.dateCreated, Literal(str(DATE.year) + str(DATE.month) + str(DATE.day))))

            alts['system'] = alts['system'].fillna('non-system')
            alts_system = alts.drop_duplicates()
            alts_system = alts_system['system']
            for r in alts_system:
                system_literal = r
                identifier_property = camel_case(system_literal) + "Identifier"
                if system_literal != "non-system":
                    g_vocab_header.add((ident.Identifier, OWL.equivalentClass, SKOSXL.Label))
                    g_vocab_header.add((URIRef(ident + identifier_property), RDF.type, OWL.ObjectProperty))
                    g_vocab_header.add((URIRef(ident + identifier_property), RDFS.label,
                                        Literal(system_literal + " identifier", lang="en")))
                    g_vocab_header.add((URIRef(ident + identifier_property), RDFS.subPropertyOf,
                                        SKOSXL.altlabel))

            g_vocab_header.serialize(destination=local_output_file, format='turtle')


def check_mapping_type(mapping):
    if str.lower(mapping) == "related":
        return "relatedMatch"
    if str.lower(mapping) == "close":
        return "closeMatch"
    if str.lower(mapping) == "broader":
        return "broadMatch"
    if str.lower(mapping) == "narrower":
        return "narrowMatch"
    if str.lower(mapping) == "exact":
        return "exactMatch"


def check_concept_list(concept_string):
    concepts_list = concept_string.split(",")
    return concepts_list


def build_concepts(vocab=target_vocab, eClass=enumeratedClass, ns=namespace):
    # doesn't yet include other bases, this can be passed in as a variable or something, or like before an if statement
    # with the CV_ID
    nds_base = Namespace('https://pid.astrazeneca.net/rd/data/')
    # nsv_base = Namespace('https://pid.astrazeneca.net/rd/vocab/')
    ns_data = Namespace(nds_base + ns + '/' + eClass + '/')
    # ns_vocab = Namespace(nsv_base + ns + '/' + eClass + '/')
    # schema = Namespace('http://schema.org/')

    meta = Namespace('https://pid.astrazeneca.net/rd/vocab/meta/')
    ident = Namespace('https://pid.astrazeneca.net/rd/vocab/identifier/')
    cvns = ns_data  # the default namespace
    SKOSXL = Namespace('http://www.w3.org/2008/05/skos-xl#')

    # build graph
    g_concepts = Graph()
    g_concepts.bind("skos", SKOS)
    g_concepts.bind("dc", DC)
    g_concepts.bind("skosxl", SKOSXL)
    g_concepts.bind(eClass.lower(), ns_data)
    g_concepts.bind("meta", meta)
    g_concepts.bind("dct", DCTERMS)
    g_concepts.bind("ident", ident)
    g_concepts.bind("owl", OWL)

    cv_concept_scheme = URIRef(cvns + eClass + "Scheme")
    # cv_top_concept = URIRef(cvns + eClass)

    # SQL query equivalent
    df_defs = defs[['Preflabel', 'Definition', 'Broader', 'concept_namespace', 'Deprecated']][defs['Must Have'] == 'Y']
    df_defs['Broader'] = df_defs['Broader'].fillna('True')
    df_defs['concept_namespace'] = df_defs['concept_namespace'].fillna('default')
    mask = df_defs['Deprecated'].str.strip().str.lower() == 'yes'
    df_defs.loc[mask, 'Deprecated'] = 'True'
    df_defs = df_defs.rename(columns={'Broader': 'is_top_concept', 'Deprecated': 'is_deprecated'})
    df_defs['is_deprecated'] = df_defs['is_deprecated'].fillna(np.nan).replace([np.nan], [None])
    # deprecated df
    df_deprecated = df_defs[['Preflabel', 'is_deprecated']]
    df_deprecated = df_deprecated[~df_deprecated['is_deprecated'].isnull()]

    # broader df
    df_broader = df_defs[['Preflabel', 'is_top_concept']]
    df_broader = df_broader[~df_broader['is_top_concept'].isnull()]

    # clean alts dataframe
    # temp_alts = alts[['Preflabel', 'Altlabel', 'Lang', 'system']]
    df_alts = alts[['Altlabel', 'Lang', 'system']]

    df_alts['system'].fillna('non-system')

    # clean mappings
    df_mappings = mappings[['Preflabel', 'URI', 'Origin', 'Mapping']]

    for def_index, def_row in df_defs.iterrows():
        concept_pref_label = def_row[0].strip()
        is_top_concept = (def_row[2] == 'True')
        # concept_definition = def_row[1]
        is_deprecated = (def_row[4] is not None)

        # this needs changing
        get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
        shortuuid_data = get_shortuuid_response.text
        shortuuid_object = json.loads(shortuuid_data)
        cv_subject_id = shortuuid_object["shortuuid"]

        if def_row[3] == 'default':
            cv_subject = URIRef((cvns + cv_subject_id))
        else:
            cv_subject = URIRef((nds_base + def_row[3] + cv_subject_id))

        # and again
        get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
        shortuuid_data = get_shortuuid_response.text
        shortuuid_object = json.loads(shortuuid_data)
        skosxl_label_id = shortuuid_object["shortuuid"]
        skosxl_label_uri = URIRef((cvns + skosxl_label_id))

        # define skos concept
        g_concepts.add((cv_subject, RDF.type, SKOS.Concept))

        g_concepts.add((cv_subject, DCTERMS.modified, Literal(datetime.datetime.now())))

        if is_top_concept:
            g_concepts.add((cv_subject, SKOS.topConceptOf, cv_concept_scheme))
            g_concepts.add((cv_concept_scheme, SKOS.hasTopConcept, cv_subject))
        else:
            g_concepts.add((cv_subject, SKOS.inScheme, cv_concept_scheme))

        if is_deprecated:
            g_concepts.add((cv_subject, OWL.deprecated, Literal("true", datatype=XSD.boolean)))
        if case_sensitive:
            # # Stage where lower case pref label is forced
            # g_concepts.add((cv_subject, SKOS.definition, Literal(def_row[1], lang="en")))
            # g_concepts.add((cv_subject, SKOS.prefLabel, Literal(def_row[0].strip(), lang="en")))
            # g_concepts.add((cv_subject, RDFS.label, Literal(def_row[0].strip(), lang="en")))

            g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
            g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(def_row[0].strip(), lang="en")))
            g_concepts.add((cv_subject, SKOSXL.prefLabel, skosxl_label_uri))
        else:
            # # Stage where lower case pref label is forced
            # g_concepts.add((cv_subject, SKOS.definition, Literal(def_row[1], lang="en")))
            # g_concepts.add((cv_subject, SKOS.prefLabel, Literal(def_row[0].lower().strip(), lang="en")))
            # g_concepts.add((cv_subject, RDFS.label, Literal(def_row[0].lower().strip(), lang="en")))

            g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
            g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(def_row[0].lower().strip(), lang="en")))
            g_concepts.add((cv_subject, SKOSXL.prefLabel, skosxl_label_uri))

        temp_alts = alts[['Altlabel', 'Lang', 'system']][alts['Preflabel'] == concept_pref_label]
        temp_alts['system'] = temp_alts['system'].fillna('non-system')
        for alt_index, alt_row in temp_alts.iterrows():
            altLabel_literal = str(alt_row[0]).strip()
            lang = alt_row[1]
            system = alt_row[2]
            if system == "non-system":
                # build a core skos altLabel
                # g_concepts.add((cv_subject, SKOS.altLabel, Literal(altLabel_literal, lang=lang)))

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

                identifier_property = camel_case(system) + "Identifier"

                g_concepts.add((cv_subject, URIRef(ident + identifier_property), Literal(altLabel_literal, lang=lang)))

                get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
                shortuuid_data = get_shortuuid_response.text
                shortuuid_object = json.loads(shortuuid_data)
                label_id = shortuuid_object["shortuuid"]
                label_uri = URIRef((cvns + label_id))

                g_concepts.add((label_uri, RDF.type, ident.Identifier))
                g_concepts.add((ident.Identifier, RDFS.label, Literal("system identifier")))
                g_concepts.add((label_uri, RDF.type, SKOSXL.label))
                g_concepts.add((label_uri, SKOSXL.literalForm, Literal(altLabel_literal, lang=lang)))
                g_concepts.add((label_uri, ident.identifierType, Literal(system)))
                g_concepts.add((cv_subject, SKOSXL.altLabel, label_uri))

                # g_concepts.add((cv_subject, SKOS.altLabel, Literal(altLabel_literal, lang=lang)))
        mappings['Preflabel'] = mappings['Preflabel'].str.strip()
        temp_mappings = mappings[['Preflabel', 'URI', 'Origin', 'Mapping']][mappings['Preflabel'] == concept_pref_label]
        for mapping_index, mapping in temp_mappings.iterrows():
            mapped_uri = mapping[1]
            # mapped_origin = mapping[2]
            mapping_relation = check_mapping_type(mapping[3])
            if mapping_relation == "exactMatch":
                g_concepts.add((cv_subject, SKOS.exactMatch, URIRef(mapped_uri)))
            if mapping_relation == "closeMatch":
                g_concepts.add((cv_subject, SKOS.closeMatch, URIRef(mapped_uri)))
            if mapping_relation == "relatedMatch":
                g_concepts.add((cv_subject, SKOS.relatedMatch, URIRef(mapped_uri)))
            if mapping_relation == "broadMatch":
                g_concepts.add((cv_subject, SKOS.broadMatch, URIRef(mapped_uri)))

    # Build Deprecated relations
    for deprecated_index, deprecated_row in df_deprecated.iterrows():
        deprecated_pref_label = deprecated_row[0].strip()
        replacement_list = check_concept_list(deprecated_row[1])

        for replacement_pref_label in replacement_list:
            replaced_by_sparql_query = """
                    SELECT ?replacement_concept ?deprecated_concept
                    WHERE {
                    ?replacement_concept a skos:Concept .
                    ?deprecated_concept a skos:Concept .
                    ?deprecated_concept_label skosxl:literalForm """ + '"' + deprecated_pref_label + '"@en .' + """
                    ?deprecated_concept skosxl:prefLabel ?deprecated_concept_label .
                    ?replacement_concept_label skosxl:literalForm """ + '"' + replacement_pref_label + '"@en .' + """
                    ?replacement_concept skosxl:prefLabel ?replacement_concept_label .                    
                    }
                    """
            replaced_by_res = g_concepts.query(replaced_by_sparql_query)
            for row in replaced_by_res:
                g_concepts.add((row.deprecated_concept, DCTERMS.isReplacedBy, row.replacement_concept))
                g_concepts.add((row.deprecated_concept, DCTERMS.modified, Literal(datetime.datetime.now())))
    for broader_index, broader_row in df_broader.iterrows():
        narrower_pref_label = broader_row[0].strip()
        broader_list = check_concept_list(broader_row[1])
        for broader_pref_label in broader_list:
            # build the sparql to find the concepts
            broader_sparql_query = """
            SELECT  ?broader_concept ?narrower_concept
            WHERE {
    		?broader_concept a skos:Concept .
    		?narrower_concept a skos:Concept .
    		?broaderskosxllabel skosxl:literalForm """ + '"' + broader_pref_label + '"@en .' + """
    		?broader_concept skosxl:prefLabel ?broaderskosxllabel .
            ?narrowerskosxllabel skosxl:literalForm """ + '"' + broader_row[0].strip() + '"@en .' + """
    		?narrower_concept skosxl:prefLabel ?narrowerskosxllabel .
            }
            """

            broader_res = g_concepts.query(broader_sparql_query)
            for row in broader_res:
                res_row = """ 
                row.broader_concept (broader_term): """ + str(row.broader_concept) + """  
                row.narrower_concept (narrower_term): """ + str(row.narrower_concept) + """
                triples to build ... """ + """
                <""" + str(row.narrower_concept) + """> skos:broader <""" + str(row.broader_concept) + """> .""" + """
                <""" + str(row.narrower_concept) + """> skos:prefLabel """ + '"' + narrower_pref_label + '"@en .' + """
                <""" + str(row.broader_concept) + """> skos:narrower <""" + str(row.narrower_concept) + """> .""" + """
                <""" + str(row.broader_concept) + """> skos:prefLabel """ + '"' + broader_pref_label + '"@en .'
                g_concepts.add((row.narrower_concept, SKOS.broader, row.broader_concept))
                g_concepts.add((row.broader_concept, SKOS.narrower, row.narrower_concept))

    g_concepts.serialize(destination='../data/az_' + enumeratedClass + '_concepts.ttl', format='turtle')


def main():
    print('building header')
    build_header()
    print('header built')
    header_file = 'az_' + enumeratedClass + '_header_data.ttl'

    print('building concepts')
    build_concepts()
    concepts_file = 'az_' + enumeratedClass + '_concepts.ttl'
    print('concepts built')

    output_file = enumeratedClass + '_cv_skosxl.ttl'

    with open('../data/' + header_file) as fp:
        header_data = fp.read()

    with open('../data/' + concepts_file) as fp:
        concepts_data = fp.read()
    output_data = header_data
    output_data += "\n"
    output_data += concepts_data

    with open('../output_cvs/' + output_file, 'w') as fp:
        fp.write(output_data)
    print('done')


if __name__ == "__main__":
    main()

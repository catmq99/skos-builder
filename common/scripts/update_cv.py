import datetime
import json
import os
import numpy as np
import pandas as pd
import requests
from rdflib import Graph, Literal, RDF, URIRef, Namespace, DC, OWL, RDFS, SKOS, DCTERMS, XSD

from build_vocab_xl_new import df_generator, build_header, camel_case, check_mapping_type, check_concept_list

from Sparql_queries_for_update import get_skosxl_preflabels, get_skosxl_altlabels, get_pref_labels, get_system_identifier

path = f'{os.path.normpath(os.path.join(os.getcwd(), ".."))}\\data'

enumeratedClass = "Adduct"
namespace = "substance/SRS"
target_vocab = "RMA"
excel_workbook = '5a. Adduct 31-08-2022 pre-SKOS_1.0.3.xlsx'
ttl_file = 'Adduct_cv_1.0.2.ttl'
case_sensitive = True
version = "1.0.3"

vocab_header = pd.read_csv(f'{path}\\vocab_header_data.csv')
df1 = df_generator(excel_workbook)
defs = next(df1)
alts = next(df1)
mappings = next(df1)

# build_header()  # may be unnecessary but doesn't hurt

graph1 = Graph()
graph1.parse('../output_cvs/' + ttl_file, format='ttl')  # read export


# df_export = to_dataframe(graph)
# # df2 = df.explode()
def update_concepts(vocab=target_vocab, eClass=enumeratedClass, ns=namespace):
    nds_base = Namespace('https://purl.astrazeneca.net/rd/data/')
    ns_data = Namespace(nds_base + ns + '/' + eClass + '/')
    meta = Namespace('https://purl.astrazeneca.net/rd/vocab/meta/')
    ident = Namespace('https://purl.astrazeneca.net/rd/vocab/identifier/')
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

    # query graph
    uri_preflabel = get_pref_labels(graph1)
    skosxl_preflabel = get_skosxl_preflabels(graph1)
    skosxl_altlabel = get_skosxl_altlabels(graph1)

    for def_index, def_row in df_defs.iterrows():
        concept_pref_label = def_row[0]

        is_top_concept = (def_row[2] == 'True')
        # concept_definition = def_row[1]
        is_deprecated = (def_row[4] is not None)
        if def_row[0].strip() in uri_preflabel['preflabel'].values:
            cv_subject = URIRef(uri_preflabel['uri'][uri_preflabel['preflabel'] == def_row[0].strip()].values[0])
        else:
            print('adding new concept')
            get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
            shortuuid_data = get_shortuuid_response.text
            shortuuid_object = json.loads(shortuuid_data)
            cv_subject_id = shortuuid_object["shortuuid"]

            if def_row[3] == 'default':
                cv_subject = URIRef((cvns + cv_subject_id))
            else:
                cv_subject = URIRef((nds_base + def_row[3] + cv_subject_id))

        # and again this time get the uri of the skosxl label
        if def_row[0].strip() in skosxl_preflabel['preflabel'].values:
            skosxl_label_uri = URIRef(
                skosxl_preflabel['uri'][skosxl_preflabel['preflabel'] == def_row[0].strip()].values[0])
        else:
            print('adding new label to concept')
            get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
            shortuuid_data = get_shortuuid_response.text
            shortuuid_object = json.loads(shortuuid_data)
            skosxl_label_id = shortuuid_object["shortuuid"]
            skosxl_label_uri = URIRef((cvns + skosxl_label_id))

        # define skos concept
        g_concepts.add((cv_subject, RDF.type, SKOS.Concept))
        g_concepts.add((cv_subject, SKOS.inScheme, cv_concept_scheme))
        g_concepts.add((cv_subject, DCTERMS.modified, Literal(datetime.datetime.now())))

        if is_top_concept:
            g_concepts.add((cv_subject, SKOS.topConceptOf, cv_concept_scheme))

        if is_deprecated:
            g_concepts.add((cv_subject, OWL.deprecated, Literal("true", datatype=XSD.boolean)))

        if case_sensitive:
            # Stage where lower case pref label is forced
            g_concepts.add((cv_subject, SKOS.definition, Literal(def_row[1], lang="en")))
            g_concepts.add((cv_subject, SKOS.prefLabel, Literal(def_row[0].strip(), lang="en")))
            g_concepts.add((cv_subject, RDFS.label, Literal(def_row[0].strip(), lang="en")))

            g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
            g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(def_row[0].strip(), lang="en")))
            g_concepts.add((cv_subject, SKOSXL.prefLabel, skosxl_label_uri))
        else:
            # Stage where lower case pref label is forced
            g_concepts.add((cv_subject, SKOS.definition, Literal(def_row[1], lang="en")))
            g_concepts.add((cv_subject, SKOS.prefLabel, Literal(def_row[0].lower().strip(), lang="en")))
            g_concepts.add((cv_subject, RDFS.label, Literal(def_row[0].lower().strip(), lang="en")))

            g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
            g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(def_row[0].lower().strip(), lang="en")))
            g_concepts.add((cv_subject, SKOSXL.prefLabel, skosxl_label_uri))

        temp_alts = alts[['Altlabel', 'Lang', 'system']][alts['Preflabel'] == concept_pref_label]
        temp_alts['system'] = temp_alts['system'].fillna('non-system')
        for alt_index, alt_row in temp_alts.iterrows():
            # if isinstance(alt_row[0], str):
            #     print(str(alt_row[0]))
            #     # pass
            # else:
            #     print(type(alt_row[0]))
            #     print(str(alt_row[0]))

            altLabel_literal = alt_row[0]
            lang = alt_row[1]
            system = alt_row[2]

            if system == "non-system":
                # build a core skos altLabel
                g_concepts.add((cv_subject, SKOS.altLabel, Literal(str(altLabel_literal), lang=lang)))

                # Build a skosxl label and assign as an altLabel
                #
                # get uuid for skos xl label
                if str(altLabel_literal).strip() in skosxl_altlabel['preflabel'].values:
                    skosxl_label_uri = \
                        URIRef(
                            skosxl_altlabel['uri'][skosxl_altlabel['preflabel'] == altLabel_literal.strip()].values[0])
                else:
                    print('adding new alt label')
                    get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
                    shortuuid_data = get_shortuuid_response.text
                    shortuuid_object = json.loads(shortuuid_data)
                    skosxl_label_id = shortuuid_object["shortuuid"]
                    skosxl_label_uri = URIRef((cvns + skosxl_label_id))
                #  these lines build the skos-xl labels
                g_concepts.add((skosxl_label_uri, RDF.type, SKOSXL.Label))
                g_concepts.add((skosxl_label_uri, SKOSXL.literalForm, Literal(str(altLabel_literal), lang=lang)))
                g_concepts.add((cv_subject, SKOSXL.altLabel, skosxl_label_uri))
            else:
                identifier_property = camel_case(system) + "Identifier"
                system_label = get_system_identifier(graph1, system, identifier_property,concept_pref_label)

                g_concepts.add(
                    (cv_subject, URIRef(ident + identifier_property), Literal(str(altLabel_literal), lang=lang)))

                if str(altLabel_literal).strip() in system_label['systemlabel'].values:
                    label_uri = URIRef(
                        system_label['systemlabeluri'][system_label['systemlabel'] == str(altLabel_literal).strip()].values[0])
                else:
                    print('adding new system label')
                    get_shortuuid_response = requests.get("http://10.250.45.51:8282/shortuuid")
                    shortuuid_data = get_shortuuid_response.text
                    shortuuid_object = json.loads(shortuuid_data)
                    label_id = shortuuid_object["shortuuid"]
                    label_uri = URIRef((cvns + label_id))

                g_concepts.add((label_uri, RDF.type, ident.Identifier))
                g_concepts.add((ident.Identifier, RDFS.label, Literal("system identifier")))
                g_concepts.add((label_uri, RDF.type, SKOSXL.label))
                g_concepts.add((label_uri, SKOSXL.literalForm, Literal(str(altLabel_literal), lang=lang)))
                g_concepts.add((label_uri, ident.identifierType, Literal(system)))
                g_concepts.add((cv_subject, SKOSXL.altLabel, label_uri))

                g_concepts.add((cv_subject, SKOS.altLabel, Literal(str(altLabel_literal), lang=lang)))

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
        deprecated_pref_label = deprecated_row[0]
        replacement_list = check_concept_list(deprecated_row[1])

        for replacement_pref_label in replacement_list:
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
            replaced_by_res = g_concepts.query(replaced_by_sparql_query)
            for row in replaced_by_res:
                g_concepts.add((row.deprecated_concept, DCTERMS.isReplacedBy, row.replacement_concept))
                g_concepts.add((row.deprecated_concept, DCTERMS.modified, Literal(datetime.datetime.now())))

    for broader_index, broader_row in df_broader.iterrows():
        narrower_pref_label = broader_row[0]
        broader_list = check_concept_list(broader_row[1])
        for broader_pref_label in broader_list:
            # build the sparql to find the concepts
            broader_sparql_query = """
            SELECT ?broader_concept ?narrower_concept
            WHERE {
            ?broader_concept a skos:Concept .
            ?broader_concept skos:prefLabel """ + '"' + broader_pref_label + '"@en .' + """
            ?narrower_concept a skos:Concept .
            ?narrower_concept skos:prefLabel """ + '"' + broader_row[0] + '"@en .' + """
            #?narrower_concept skos:prefLabel ?narrower_label .
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

    g_concepts.serialize(destination='../data/az_' + enumeratedClass + '_concepts_' + version + '.ttl', format='turtle')


def main():
    print('building header')
    build_header()
    print('header built')
    header_file = 'az_' + enumeratedClass + '_header_data.ttl'

    print('building concepts')
    update_concepts()
    concepts_file = 'az_' + enumeratedClass + '_concepts_' + version + '.ttl'
    print('concepts built')

    output_file = enumeratedClass + '_cv_' + version + '.ttl'

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

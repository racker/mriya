__author__ = 'Volodymyr Varchuk'

from collections import namedtuple

SELECT_TMPLATE = "SELECT {columns} FROM {table} {where_statement} "  # WHERE Id='001W000000Npv47IAB' LIMIT 1"
WHERE_STATEMENT_TEMPLATE = 'WHERE {conditions}'

MappingElement = namedtuple('MappingElement',
                            ['global_table', 'table_src', 'column_src',
                             'table_dst', 'column_dst', "operation", "type"])


class MappingParser:
    def __init__(self, mapping):
        self.mappings = []
        for mapping_element in mapping:
            self.mappings.append(MappingElement._make(
                [mapping_element[mapping_key] for mapping_key in
                 MappingElement._fields]))
        self.src_extract_soql_generator()
        self.dst_update_relations()
        self.src_update_relations()

    def src_extract_soql_generator(self):
        if len(self.mappings) > 0:
            from_table = self.mappings[0].global_table
        else:
            print('There are no elements in mapping. Nothing to process')
            return
        columns = []
        for map_element in self.mappings:
            if from_table != map_element.table_src:
                columns.append(
                    map_element.table_src + '.' + map_element.column_src)
            else:
                columns.append(map_element.column_src)
        self.soql = SELECT_TMPLATE.format(columns=', '.join(columns),
                                          table=from_table, where_statement='')
        return self.soql

    def dst_get_soql_created_records(self, records):
        if len(self.mappings) > 0:
            from_table = self.get_dst_table()
        else:
            print('There are no elements in mapping. Nothing to process')
            return
        columns = []
        for map_element in self.mappings:
            if map_element.type != 'regular':
                if from_table != map_element.table_dst:
                    columns.append(
                        map_element.table_dst + '.' + map_element.column_dst)
                else:
                    columns.append(map_element.column_dst)
        # where statement
        if len(records) > 0:
            where_clause = WHERE_STATEMENT_TEMPLATE.format(
                conditions=' or '.join(
                    ["id='{0}'".format(record['id']) for record in records]))
        else:
            where_clause = ''
        self.soql_created = SELECT_TMPLATE.format(columns=', '.join(columns),
                                                  table=from_table,
                                                  where_statement=where_clause)
        return self.soql_created

    def dst_update_relations(self):
        self.dst_update_mapping = {}
        for map_elenemt in self.mappings:
            self.dst_update_mapping[
                map_elenemt.column_dst] = map_elenemt.column_src

        self.dst_update_mapping['type'] = self.get_dst_table()
        return self.dst_update_mapping

    def src_update_relations(self):
        self.src_update_mapping = {}
        for map_elenemt in self.mappings:
            if map_elenemt.operation == 'upd_src':
                self.src_update_mapping[
                    map_elenemt.column_src] = map_elenemt.column_dst
            if map_elenemt.column_src not in self.src_update_mapping.keys() and map_elenemt.type == 'SRC_ID':
                self.src_update_mapping[
                    map_elenemt.column_src] = map_elenemt.column_dst
        self.src_update_mapping['type'] = self.get_src_table()
        return self.src_update_mapping

    def get_dst_table(self):
        if len(self.mappings) > 0:
            for mapping_item in self.mappings:
                return mapping_item.table_dst
        else:
            return None

    def get_src_table(self):
        if len(self.mappings) > 0:
            for mapping_item in self.mappings:
                return mapping_item.table_src
        else:
            return None

    def get_src_old_id_column(self):
        for mapping_item in self.mappings:
            if mapping_item.operation == 'upd_src':
                return mapping_item.column_src

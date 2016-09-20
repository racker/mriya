__author__ = 'Volodymyr Varchuk'

from collections import namedtuple

SELECT_TMPLATE = "SELECT {columns} FROM {table} {where_statement} LIMIT 10000"  # WHERE Id='001W000000Npv47IAB' LIMIT 1"
WHERE_STATEMENT_TEMPLATE = 'WHERE {conditions}'

MappingElement = namedtuple('MappingElement',
                            ['table_src', 'column_src',
                             'table_dst', 'column_dst', "operation", "column_type"])


class MappingParser:
    def __init__(self, mapping):
        if len(mapping['mapping']) == 0:
            print('There are no elements in mapping. Nothing to process')
            return
        self.mappings = []
        self.source_object = mapping['source_object']
        self.destination_object = mapping ['destination_object']
        self.where_condition = mapping['where_condition']
        for mapping_element in mapping['mapping']:
            self.mappings.append(MappingElement._make(
                [mapping_element[mapping_key] for mapping_key in
                 MappingElement._fields]))

        self.src_update_mapping = self.src_update_relations()
        self.dst_update_mapping = self.dst_update_relations()


    def get_src_soql(self):
        columns = []
        for map_element in self.mappings:
            if self.source_object != map_element.table_src:
                columns.append(
                    map_element.table_src + '.' + map_element.column_src)
            else:
                columns.append(map_element.column_src)
        self.src_soql = SELECT_TMPLATE.format(columns=', '.join(columns),
                                          table=self.source_object, where_statement=self.where_condition)
        return self.src_soql

    # def get_dst_soql(self):
    #     columns = []
    #     for map_element in self.mappings:
    #         if self.source_object != map_element.table_src:
    #             columns.append(
    #                 map_element.table_src + '.' + map_element.column_src)
    #         else:
    #             columns.append(map_element.column_src)
    #     self.dst_soql = SELECT_TMPLATE.format(columns=', '.join(columns),
    #                                       table=self.source_object, where_statement='')
    #     return self.dst_soql


    def get_dst_soql_cond(self, records_id, condition_field=None):
        columns = []
        for map_element in self.mappings:
            if map_element.column_type != 'regular':
                if self.destination_object != map_element.table_dst:
                    columns.append(
                        map_element.table_dst + '.' + map_element.column_dst)
                else:
                    columns.append(map_element.column_dst)
        for map_element in self.mappings:
            if map_element.column_type != 'dst_id':
                dst_id_column = map_element.column_dst
        cond_field = dst_id_column if condition_field is None else condition_field
        if len(records_id) > 0:
            print(records_id)
            where_clause = WHERE_STATEMENT_TEMPLATE.format(
                conditions=' or '.join(
                    ["{0}='{1}'".format(cond_field, record) for record in records_id]))
        else:
            where_clause = ''
        self.dst_soql_cond = SELECT_TMPLATE.format(columns=', '.join(columns),
                                          table=self.source_object, where_statement=where_clause)
        if len(self.dst_soql_cond) > 20000:
            self.dst_soql_cond = None
            print('destination soql with condition is too long')

        return self.dst_soql_cond


    def dst_get_soql_created_records(self, records):

        if len(self.mappings) == 0:
            print('There are no elements in mapping. Nothing to process')
            return
        columns = []
        for map_element in self.mappings:
            if map_element.column_type != 'regular':
                if self.destination_object != map_element.table_dst:
                    columns.append(
                        map_element.table_dst + '.' + map_element.column_dst)
                else:
                    columns.append(map_element.column_dst)
            if map_element.column_type == 'src_id':
                id_column = map_element.column_dst
        # where statement

        if len(records) > 0:
            where_clause = WHERE_STATEMENT_TEMPLATE.format(
                conditions=' or '.join(
                    ["{0}='{1}'".format(id_column, record) for record in records]))
        else:
            where_clause = ''
        soql_created = SELECT_TMPLATE.format(columns=', '.join(columns),
                                                  table=self.destination_object,
                                                  where_statement=where_clause)
        return soql_created


    def dst_update_relations(self):
        dst_update_mapping = {}
        for map_elenemt in self.mappings:
            dst_update_mapping[
                map_elenemt.column_dst] = map_elenemt.column_src
        # do not needed for bulk
        # self.dst_update_mapping['type'] = self.destination_object
        return dst_update_mapping


    def src_update_relations(self):
        src_update_mapping = {}
        for map_elenemt in self.mappings:
            if map_elenemt.operation == 'upd_src':
                src_update_mapping[
                    map_elenemt.column_src] = map_elenemt.column_dst
            if map_elenemt.column_src not in src_update_mapping.keys() and map_elenemt.column_type == 'src_id':
                src_update_mapping[
                    map_elenemt.column_src] = map_elenemt.column_dst
        # do not needed for bulk
        # self.src_update_mapping['type'] = self.source_object
        print(src_update_mapping)
        return src_update_mapping


    def get_src_old_id_column(self):
        for mapping_item in self.mappings:
            if mapping_item.operation == 'upd_src':
                return mapping_item.column_src

    def get_dst_old_id_column(self):
        for mapping_item in self.mappings:
            if mapping_item.operation == 'upd_src':
                return mapping_item.column_src


    def get_src_columns(self):
        src_column = []
        for mapping_item in self.mappings:
            src_column.append(mapping_item.column_src)
        return src_column
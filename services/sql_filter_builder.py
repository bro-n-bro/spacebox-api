class SqlFilterBuilderService:

    def build_filter(self, query_params):
        filter = ''
        for query_param in query_params.keys():
            if query_param in ['offset', 'limit']:
                continue
            param_key = query_param.split('__')[1]
            field_name = query_param.split('__')[0]
            builder = self.get_builder(param_key)
            filter_for_field = builder(field_name, query_params.get(query_param))
            if not filter:
                filter_prefix = 'WHERE'
            else:
                filter_prefix = 'AND'
            filter += f'{filter_prefix} {filter_for_field}'
        return filter

    def get_builder(self, param_key: str):
        builder_matcher = {
            'in': self.build_in_list_filter,
            '': self.build_filter
        }
        return builder_matcher.get(param_key, self.build_filter)

    def build_in_list_filter(self, field_name: str, value: str) -> str:
        sql_list_of_values = '('
        for item in value.split(','):
            sql_list_of_values += f"'{item}',"
        sql_list_of_values = f'{sql_list_of_values[:-1]})'
        return f"{field_name} IN {sql_list_of_values}"

    def build_equal_filter(self, field_name: str, value: str) -> str:
        return f"{field_name} = '{value}'"

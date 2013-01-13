def field_included(data, field):
    return (data and 'include' in data and field in data['include'])

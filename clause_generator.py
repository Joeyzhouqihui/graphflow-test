class Clause_generator():
    def __init__(self):
        self.create_clause = "create"
        self.vertex_clause = "({0}:{1} {2})"
        self.vertex_clause_without_prop = "({0}:{1})"
        self.edge_clause = "-[:{0} {1}]->"
        self.edge_clause_without_prop = "-[:{0}]->"
        self.end_clause = ";"

    #传入一个字典，然后构建properties
    def construct_prop(self, dic):
        props = []
        for key in dic.keys() :
            if isinstance(dic[key], str):
                value = '\"'+dic[key]+'\"'
            else:
                value = str(dic[key])
            props.append(key + ':' + value)
        return '{' + ','.join(props) + '}'

    def construct_vertex(self, id, type, prop=None):
        if prop is None:
            vertex = self.vertex_clause_without_prop.format(id, type)
        else:
            props = self.construct_prop(prop)
            vertex = self.vertex_clause.format(id, type, props)
        return vertex

    def construct_edge(self,
                       from_id, from_type,
                       edge_type,
                       to_id, to_type,
                       from_prop = None, edge_prop = None, to_prop = None):
        from_vertex = self.construct_vertex(from_id, from_type, from_prop)
        to_vertex = self.construct_vertex(to_id, to_type, to_prop)
        if edge_prop is None:
            edge = self.edge_clause_without_prop.format(edge_type)
        else:
            props = self.construct_prop(edge_prop)
            edge = self.edge_clause.format(edge_type, props)
        return from_vertex + edge + to_vertex

    def create_vertex(self, id, type, prop=None):
        vertex = self.construct_vertex(id, type, prop)
        return self.create_clause + ' ' + vertex + self.end_clause

    def create_vertices(self, ids, types, props=None):
        len = len(ids)
        vertices = []
        for i in range(len):
            vertex = self.construct_vertex(ids[i], types[i], props[i])
            vertices.append(vertex)
        return self.create_clause + ' ' + ','.join(vertices) + self.end_clause

    def create_edge(self,
                    from_id, from_type,
                    edge_type,
                    to_id, to_type,
                    from_prop = None, edge_prop = None, to_prop = None):
        edge = self.construct_edge(from_id, from_type, edge_type, to_id, to_type, from_prop, edge_prop, to_prop)
        return self.create_clause + ' ' + edge + self.end_clause

    def create_edges(self,
                     from_ids, from_types,
                     edge_types,
                     to_ids, to_types,
                     from_props = None, edge_props = None, to_props = None):
        len = len(from_ids)
        edges = []
        for i in range(len):
            edge = self.construct_edge(from_ids[i], from_types[i],
                                       edge_types[i],
                                       to_ids[i], to_types[i],
                                       from_props[i], edge_props[i], to_props[i])
            edges.append(edge)
        return self.create_clause + ' ' + ','.join(edges) + self.end_clause

    def create_edges(self, edges):
        pass


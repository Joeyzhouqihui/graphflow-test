class Clause_generator():
    def __init__(self):
        self.create_clause = "create"
        self.vertex_clause = "({0}:{1} {2})"
        self.vertex_clause_without_prop = "({0}:{1})"
        self.edge_clause = "-[:{0} {1}]->"
        self.edge_clause_without_prop = "-[:{0}]->"
        self.end_clause = ";"
        self.continuous_match_clause = "Continuously match"
        self.sep_clause = ","
        self.tmp = None
        self.file = "file \"{0}\""

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

    def add_vertex(self, id, type, prop=None):
        vertex = self.construct_vertex(id, type, prop)
        if self.tmp is None:
            self.tmp = vertex
        else:
            self.tmp = self.tmp + self.sep_clause + vertex

    def create_vertex(self):
        if self.tmp is None:
            return ';'
        return_clause = self.create_clause + ' ' + self.tmp + self.end_clause
        self.tmp = None
        return return_clause

    def add_edge(self,
                    from_id, from_type,
                    edge_type,
                    to_id, to_type,
                    from_prop = None, edge_prop = None, to_prop = None):
        edge = self.construct_edge(from_id, from_type, edge_type, to_id, to_type, from_prop, edge_prop, to_prop)
        if self.tmp is None:
            self.tmp = edge
        else:
            self.tmp = self.tmp + self.sep_clause + edge

    def create_edge(self):
        if self.tmp is None:
            return ';'
        return_clause = self.create_clause + ' ' + self.tmp + self.end_clause
        self.tmp = None
        return return_clause

    def add_match_edge(self,
                        from_var, from_type,
                        edge_type,
                        to_var, to_type):
        edge = self.construct_edge(from_var, from_type, edge_type, to_var, to_type)
        if self.tmp is None:
            self.tmp = edge
        else:
            self.tmp = self.tmp + self.sep_clause + edge

    def create_continuous_edge(self, filename):
        if self.tmp is None:
            return ';'
        return_clause = self.continuous_match_clause + ' ' + self.tmp + ' ' + self.file.format(filename) + ' ' + self.end_clause
        self.tmp = None
        return return_clause

class Variable_generator():
    def __init__(self):
        self.count = 0

    def get_variable(self):
        re = 'n' + str(self.count)
        self.count += 1
        return re

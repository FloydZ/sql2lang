import sys
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

test = """CREATE TABLE public.historical_data_template (
                    date date NOT NULL,
                    open double precision NOT NULL,
                    high double precision NOT NULL,
                    low double precision NOT NULL,
                    close double precision NOT NULL,
                    volume bigint,
                    CONSTRAINT historical_data_template_pk PRIMARY KEY (date)
                
                );"""



class Internal_Representation():
    """
    internal representation of an input sql string:
        input = -- object: public.historical_data_template | type: TABLE --
                -- DROP TABLE IF EXISTS public.historical_data_template CASCADE;
                CREATE TABLE public.historical_data_template (
                    date date NOT NULL,
                    open double precision NOT NULL,
                    high double precision NOT NULL,
                    low double precision NOT NULL,
                    close double precision NOT NULL,
                    volume bigint,
                    CONSTRAINT historical_data_template_pk PRIMARY KEY (date)
                
                );
                
        output [{"key": "date", "type": "datetime"},
                ...
                ]
                
    """
    def __init__(self, str):
        """
        dont parse anything on startup. do this in the children classes.
        :param str:
        """
        pass

########################################################################################################################
#   Table name Stuff
    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

    def extract_from_part(self, parsed):
        from_seen = False
        for item in parsed.tokens:
            if from_seen:
                if self.is_subselect(item):
                    for x in self.extract_from_part(item):
                        yield x
                elif item.ttype is Keyword:
                    return
                else:
                    yield item
            elif item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True

    def extract_table_identifiers(self, token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_name()
            elif isinstance(item, Identifier):
                yield item.get_name()
            # It's a bug to check for Keyword here, but in the example
            # above some tables names are identified as keywords...
            elif item.ttype is Keyword:
                yield item.value

    def removePossiblePublicFromTableName(self, table_name):
        if "public." in table_name:
            return table_name[7:]
        return table_name

    def extract_tables(self, sql):
        # stream = self.extract_from_part(sqlparse.parse(sql)[0])
        # return list(self.extract_table_identifiers(stream))
        # LOL TODO
        return self.removePossiblePublicFromTableName(sql.split(" ")[2])

########################################################################################################################
#   Definition, Element Stuff
    def extract_definitions(self,token_list):
        """
            needed by 'parse_def'

        :param token_list:
        :return:
        """
        # assumes that token_list is a parenthesis
        definitions = []
        tmp = []
        par_level = 0
        for token in token_list.flatten():
            if token.is_whitespace:
                continue
            elif token.match(sqlparse.tokens.Punctuation, '('):
                par_level += 1
                continue
            if token.match(sqlparse.tokens.Punctuation, ')'):
                if par_level == 0:
                    break
                else:
                    par_level += 1
            elif token.match(sqlparse.tokens.Punctuation, ','):
                if tmp:
                    definitions.append(tmp)
                tmp = []
            else:
                tmp.append(token)
        if tmp:
            definitions.append(tmp)
        return definitions

    def parse_def(self, sql):
        """
        main function to parse the internal definitions of a SQL CREATE Statment

        :param sql: string
        :return:
        """
        parsed = sqlparse.parse(sql)[0]

        # extract the parenthesis which holds column definitions
        _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
        columns = self.extract_definitions(par)

        r = []
        for column in columns:
            s = {}
            s['key'] = column[0]
            s['type'] = column[1:]
            r.append(s)
            #print('NAME: {name!s:12} DEFINITION: {definition}'.format(
            #    name=column[0], definition=' '.join(str(t) for t in column[1:])))
        return r


class SQL2RUST(Internal_Representation):
    def __init__(self, str):
        """
        str is the sql string to parse

        :param str:
        """
        super().__init__(str)

        # TODO error mng
        self.internal_code = self.parse_def(str)
        self.table_name = self.extract_tables(str)

        print(self.internal2RustStruct())
        print()
        print(self.internal2RustDieselSchema(["TODO", "TODO2"]))
        print()

    def internal2RustStructAppendNotNull(self, s, a):
        """
        :param s: the string to return
        :param a: rest of the array, where a NOT NULL can be found
        :return:
        """

        ss = str(a[:-1]) + " " + str(a[:-2])
        if ss == "NOT NULL":
            return s + " " + ss

        return "Optional<" + s + ">"

    def internal2RustStructUnsigned(self, s, a):
        """

        :param s: "i32", "i64", "u32"
        :param a: rest vom array der geparst wrd
        :return:
        """
        try:
            if str(a[1]) == "unsigned":
                # TODO stimmt das?
                return "u" + s[-1:]
        except Exception as _:
            pass

        return s

    def internal2RustStructLine(self, a):
        """
        convert a given array of sql types to a rust struct

        :param a: parse_def[i]['type
        :return:
        """

        # im using this approach, because if i used a dict like:
        #   out = resolve_rust[date]
        # i could not use dynamically special function like 'internal2RustStructUnsigned'
        arg = str(a[0]).lower()
        if arg == "date":
            return self.internal2RustStructAppendNotNull("DateTime<Utc>", a)
        elif arg == "bigint":
            return self.internal2RustStructAppendNotNull(self.internal2RustStructUnsigned("i64", a), a)
        elif arg == "int":
            return self.internal2RustStructAppendNotNull(self.internal2RustStructUnsigned("i32", a), a)
        elif arg == "tinyint":
            return self.internal2RustStructAppendNotNull(self.internal2RustStructUnsigned("i8", a), a)
        elif arg == "double precision":
            return self.internal2RustStructAppendNotNull(self.internal2RustStructUnsigned("f64", a), a)

    def internal2RustStruct(self):
        """
        converts a the array 'a', which is the output

        :param a: output from parse_def()
        :return:
        """
        ret = "pub struct " + self.table_name + " {\n"

        for r in self.internal_code:
            if r['key'] is None:
                break

            if r['key'] == "CONSTRAINT":
                break

            typ = self.internal2RustStructLine(r['type'])
            if typ is None:
                break

            ret += "\t" + str(r['key']) + ":" + " " + typ + ",\n"

        return ret + "}"

    def internal2RustDieselSchema(self, primary_elements):
        """

        :return:
        """

        if len(primary_elements) == 0:
            print("sorry, please give at least one primary element")

        pp = ",".join([str(o) for o in primary_elements])
        p = " (" + pp + ")"

        s = "table! {\n"
        s += "\t" + self.table_name + p + "{\n"

        for r in self.internal_code:
            if r['key'] is None:
                break

            if r['key'] == "CONSTRAINT":
                break

            s += "\t\t" + str(r['key']) + " -> " + str(r['type'][0]) + ",\n"

        s += "\t}\n"# end of table name
        s += "}"    # end of table!
        return s

def help():
    print("sql2lang.py Usage:")
    print("\t\t[test|rust] [sql_string|file_with_sql_code]")


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        help()
        exit(1)

    cmd = sys.argv[1]
    if cmd == "test":
        # some testing/debugging stuff
        s = SQL2RUST(test)

    elif cmd == "rust":
        r = SQL2RUST(sys.argv[2])

    exit(1)
    # ok the given argument is not test. so it still can be a string or a file to read
    try:
        f = open(cmd)
        print("sorry file reading is currenlty not implemented")
        # Do something with the file
        exit(1)
    except IOError:
        pass
    finally:
        f.close()
